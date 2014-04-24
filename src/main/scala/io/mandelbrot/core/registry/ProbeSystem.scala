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
import akka.persistence.{PersistenceFailure, Persistent, Processor}
import scala.concurrent.Future
import scala.concurrent.duration._
import java.net.URI

import io.mandelbrot.core.notification.{NotificationService, Notification}
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.messagestream.MandelbrotMessage
import io.mandelbrot.core.state.StateService

/**
 *
 */
class ProbeSystem(uri: URI) extends Processor with ActorLogging {
  import ProbeSystem._
  import context.dispatcher

  // config
  override def processorId = uri.toString
  val timeout = Timeout(5.seconds)

  // state
  var probes: Map[ProbeRef,ProbeActor] = Map.empty
  var currentSpec: Option[ProbeSpec] = None

  val stateService = StateService(context.system)

  def receive = {

    /* configure the probe system using the spec */
    case Persistent(spec: ProbeSpec, sequenceNr) =>
      val specSet = probeSpec2Set(spec)
      val probeSet = probes.keySet
      // add new probes
      val probesAdded = specSet -- probeSet
      probesAdded.toVector.sorted.foreach { case ref: ProbeRef =>
        val actor = ref.parentOption match {
          case Some(parent) =>
            context.actorOf(Probe.props(ref, probes(parent).actor))
          case None =>
            context.actorOf(Probe.props(ref, self))
        }
        log.debug("added probe {}", ref)
        probes = probes + (ref -> ProbeActor(findProbeSpec(spec, ref.path), actor))
        stateService ! ProbeMetadata(ref, spec.metadata)
      }
      // remove stale probes
      val probesRemoved = probeSet -- specSet
      probesRemoved.toVector.sorted.reverse.foreach { case ref: ProbeRef =>
        probes(ref).actor ! PoisonPill
        log.debug("removed probe {}", ref)
        probes = probes - ref
      }
      currentSpec = Some(spec)

    case PersistenceFailure(message, sequenceNr, cause) =>
      log.error("failed to persist message {}: {}", message, cause.getMessage)

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
          log.warning("ignoring message {}: probe no longer exists", message)
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
      NotificationService(context.system) ! notification

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
}

object ProbeSystem {
  def props(uri: URI) = Props(classOf[ProbeSystem], uri)

  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)
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

case class UpdateProbeSystem(uri: URI, spec: ProbeSpec) extends ProbeSystemCommand
case class UpdateProbeSystemResult(op: UpdateProbeSystem, ref: ActorRef)

case class GetProbeSystemStatus(uri: URI) extends ProbeSystemQuery
case class GetProbeSystemStatusResult(op: GetProbeSystemStatus, state: Vector[ProbeStatus])

case class GetProbeSystemMetadata(uri: URI) extends ProbeSystemQuery
case class GetProbeSystemMetadataResult(op: GetProbeSystemMetadata, metadata: Map[ProbeRef,Map[String,String]])
