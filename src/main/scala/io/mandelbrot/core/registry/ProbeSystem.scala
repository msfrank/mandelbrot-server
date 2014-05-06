/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.registry

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
import java.net.URI

import io.mandelbrot.core.{ServerConfig, ResourceNotFound, ApiException}
import io.mandelbrot.core.notification.{EmitPolicy, NotificationPolicy, Notification}
import io.mandelbrot.core.message.MandelbrotMessage
import io.mandelbrot.core.state.StateService

/**
 *
 */
class ProbeSystem(uri: URI, initialSpec: Option[ProbeSpec]) extends Actor with ActorLogging {
  import ProbeSystem._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system)
  val timeout = Timeout(5.seconds)

  // state
  var probes: Map[ProbeRef,ProbeActor] = Map.empty
  var currentSpec: Option[ProbeSpec] = None
  var notifier: NotificationPolicy = new EmitPolicy(context.system)

  val stateService = StateService(context.system)

  def receive = {

    /* initialize the probe system with the specified spec */
    case InitializeProbeSystem(spec) =>
      applyProbeSpec(spec)
      log.debug("initialized probe system {}", uri)

    /* update the probe system with the specified spec */
    case command @ UpdateProbeSystem(_, registration) =>
      val spec = ProbeConversions.registration2spec(registration)
      applyProbeSpec(spec)
      log.debug("updated probe system {}", uri)

    /* get the ProbeSystem spec */
    case query: DescribeProbeSystem =>
      currentSpec match {
        case Some(spec) =>
          sender() ! DescribeProbeSystemResult(query, spec)
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* get the state of all probes in the system */
    case query: GetProbeSystemStatus =>
      currentSpec match {
        case Some(spec) =>
          val futures = probes.toVector.map { case (ref: ProbeRef, actor: ProbeActor) =>
            actor.actor.ask(GetProbeStatus(ref))(timeout).mapTo[GetProbeStatusResult]
          }
          // FIXME: handle error reply
          Future.sequence(futures).map { case results: Vector[GetProbeStatusResult] =>
            GetProbeSystemStatusResult(query, results.map(_.state))
          }.pipeTo(sender())
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* get the metadata of all probes in the system */
    case query: GetProbeSystemMetadata =>
      currentSpec match {
        case Some(spec) =>
          val metadata = probes.keys.map { ref => ref -> findProbeSpec(spec, ref.path).metadata }.toMap
          sender() ! GetProbeSystemMetadataResult(query, metadata)
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* send message to specified probe */
    case message: MandelbrotMessage =>
      probes.get(message.source) match {
        case Some(probeActor: ProbeActor) =>
          probeActor.actor ! message
        case None =>
          log.warning("ignoring message {}: probe is not known", message)
      }

    /* forward probe operations to the specified probe */
    case op: ProbeOperation =>
      probes.get(op.probeRef) match {
        case Some(probeActor: ProbeActor) =>
          probeActor.actor.forward(op)
        case None =>
          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
      }

    /* handle notifications which have been passed up from Probe */
    case notification: Notification =>
      notifier.notify(notification)

    case Terminated(ref) =>
      log.debug("actor {} has been terminated", ref.path)

  }

  /**
   * flatten ProbeSpec into a Set of ProbeRefs
   */
  def probeSpec2Set(path: Vector[String], spec: ProbeSpec): Set[ProbeRef] = {
    val iterChildren = spec.children.toSet
    val childRefs = iterChildren.map { case (name: String, childSpec: ProbeSpec) =>
      probeSpec2Set(path :+ name, childSpec)
    }.flatten
    childRefs + ProbeRef(uri, path)
  }
  def probeSpec2Set(spec: ProbeSpec): Set[ProbeRef] = probeSpec2Set(Vector.empty, spec)

  /**
   * find the ProbeSpec referenced by path
   */
  def findProbeSpec(spec: ProbeSpec, path: Vector[String]): ProbeSpec = {
    if (path.isEmpty) spec else findProbeSpec(spec.children(path.head), path.tail)
  }

  /**
   * apply the spec to the probe system, adding and removing probes as necessary
   */
  def applyProbeSpec(spec: ProbeSpec): Unit = {
    val specSet = probeSpec2Set(spec)
    val probeSet = probes.keySet
    // add new probes
    val probesAdded = specSet -- probeSet
    probesAdded.toVector.sorted.foreach { case ref: ProbeRef =>
      val actor = ref.parentOption match {
        case Some(parent) =>
          context.actorOf(Probe.props(ref, probes(parent).actor, stateService))
        case None =>
          context.actorOf(Probe.props(ref, self, stateService))
      }
      log.debug("probe {} joins", ref)
      actor ! InitProbe
      probes = probes + (ref -> ProbeActor(findProbeSpec(spec, ref.path), actor))
      stateService ! ProbeMetadata(ref, spec.metadata)
    }
    // remove stale probes
    val probesRemoved = probeSet -- specSet
    probesRemoved.toVector.sorted.reverse.foreach { case ref: ProbeRef =>
      log.debug("probe {} retires", ref)
      probes(ref).actor ! RetireProbe
      probes(ref).actor ! PoisonPill
      probes = probes - ref
    }
    currentSpec = Some(spec)
  }
}

object ProbeSystem {
  def props(uri: URI, initialSpec: Option[ProbeSpec] = None) = Props(classOf[ProbeSystem], uri, initialSpec)

  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)
  case class InitializeProbeSystem(spec: ProbeSpec)
  case object InitProbe
  case object RetireProbe
}

/**
 *
 */
sealed trait ProbeSystemOperation {
  val uri: URI
}
sealed trait ProbeSystemCommand extends ProbeSystemOperation
sealed trait ProbeSystemQuery extends ProbeSystemOperation
case class ProbeSystemOperationFailed(op: ProbeSystemOperation, failure: Throwable)

case class DescribeProbeSystem(uri: URI) extends ProbeSystemQuery
case class DescribeProbeSystemResult(op: DescribeProbeSystem, spec: ProbeSpec)

case class UpdateProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeSystemCommand
case class UpdateProbeSystemResult(op: UpdateProbeSystem, ref: ActorRef)

case class GetProbeSystemStatus(uri: URI) extends ProbeSystemQuery
case class GetProbeSystemStatusResult(op: GetProbeSystemStatus, state: Vector[ProbeStatus])

case class GetProbeSystemMetadata(uri: URI) extends ProbeSystemQuery
case class GetProbeSystemMetadataResult(op: GetProbeSystemMetadata, metadata: Map[ProbeRef,Map[String,String]])
