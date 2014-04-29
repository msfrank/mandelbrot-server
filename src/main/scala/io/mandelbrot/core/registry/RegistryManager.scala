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
import akka.persistence.{SnapshotOffer, EventsourcedProcessor, Persistent}
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core.notification.{NotificationPolicyType, NotificationService, Notification}
import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException}
import io.mandelbrot.core.message.{StatusMessage, MessageStream}
import scala.concurrent.duration.Duration

/**
 *
 */
class RegistryManager extends EventsourcedProcessor with ActorLogging {
  import RegistryManager._

  // config
  override def processorId = "probe-registry"

  // state
  val objectSystems = new java.util.HashMap[URI,ActorRef](1024)

  /* */
  MessageStream(context.system).subscribe(self, classOf[StatusMessage])

  def receiveCommand = {

    /* create the ProbeSystem */
    case command @ RegisterProbeSystem(uri, spec) =>
      if (!objectSystems.containsKey(uri)) {
        persist(Event(command))(updateState)
      } else {
        sender() ! ProbeRegistryOperationFailed(command, new ApiException(Conflict))
      }

    /* update the ProbeSystem */
    case UpdateProbeSystem(uri, spec) =>
      objectSystems.get(uri) match {
        case null =>
          persist(Event(RegisterProbeSystem(uri, spec)))(updateState)
        case ref: ActorRef =>
          ref.forward(Persistent(spec))
      }

    /* terminate the ProbeSystem */
    case command @ UnregisterProbeSystem(uri) =>
      objectSystems.get(uri) match {
        case null =>
          sender() ! ProbeRegistryOperationFailed(command, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          persist(Event(command))(updateState)
      }

    /* return the list of registered ProbeSystems */
    case query: ListProbeSystems =>
      sender() ! ListProbeSystemsResult(query, objectSystems.keySet().toVector)

    /* forward ProbeSystem operations or return failure if system doesn't exist */
    case op: ProbeSystemOperation =>
      objectSystems.get(op.uri) match {
        case null =>
          sender() ! ProbeSystemOperationFailed(op, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          ref.forward(op)
      }

    /* forward Probe operations or return failure if system doesn't exist */
    case op: ProbeOperation =>
      objectSystems.get(op.probeRef.uri) match {
        case null =>
          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          ref.forward(op)
      }

    /* forward state messages to the appropriate ProbeSystem */
    case message: StatusMessage =>
      objectSystems.get(message.source.uri) match {
        case null =>
          // do nothing
        case ref: ActorRef =>
          ref ! message
      }

    /* handle notifications which have been passed up from ProbeSystems */
    case notification: Notification =>
      NotificationService(context.system) ! notification

    case Terminated(ref) =>
      log.debug("actor {} has been terminated", ref.path)
  }

  def receiveRecover = {

    case event: Event =>
      updateState(event)

    case SnapshotOffer(metadata, snapshot) =>
      log.debug("received snapshot offer: metadata={}, snapshot={}", metadata, snapshot)
  }

  def updateState(event: Event) = event.event match {
    /* create the ProbeSystem */
    case command @ RegisterProbeSystem(uri, spec) =>
      val ref = context.actorOf(ProbeSystem.props(uri))
      context.watch(ref)
      objectSystems.put(uri, ref)
      ref ! Persistent(spec)
      sender() ! RegisterProbeSystemResult(command, ref)
      log.debug("created probe system {} at {}", uri, ref.path)

    /* terminate the ProbeSystem */
    case command @ UnregisterProbeSystem(uri) =>
      val ref = objectSystems.get(uri)
      log.debug("deleted probe system {}", uri)
      objectSystems.remove(uri)
      ref ! PoisonPill
  }
}

object RegistryManager {
  def props() = Props(classOf[RegistryManager])

  case class Event(event: Any)
}

/* contains tunable parameters for the probe */
case class ProbePolicy(joiningTimeout: Duration,
                       probeTimeout: Duration,
                       leavingTimeout: Duration,
                       flapWindow: Duration,
                       flapDeviations: Int,
                       notificationPolicy: NotificationPolicyType,
                       inherits: Boolean)

/* the probe specification */
case class ProbeSpec(objectType: String, metadata: Map[String,String], children: Map[String,ProbeSpec])
//case class ProbeSpec(objectType: String, policy: ProbePolicy, metadata: Map[String,String], children: Map[String,ProbeSpec], static: Boolean)

/* a dynamic probe system registration */
case class ProbeRegistration(objectType: String, policy: ProbePolicy, metadata: Map[String,String], children: Map[String,ProbeRegistration])

/* object registry operations */
sealed trait ProbeRegistryOperation
sealed trait ProbeRegistryQuery extends ProbeRegistryOperation
sealed trait ProbeRegistryCommand extends ProbeRegistryOperation
case class ProbeRegistryOperationFailed(op: ProbeRegistryOperation, failure: Throwable)

case class RegisterProbeSystem(uri: URI, spec: ProbeSpec) extends ProbeRegistryCommand
case class RegisterProbeSystemResult(op: RegisterProbeSystem, ref: ActorRef)

case class ListProbeSystems() extends ProbeRegistryQuery
case class ListProbeSystemsResult(op: ListProbeSystems, uris: Vector[URI])

case class UnregisterProbeSystem(uri: URI) extends ProbeRegistryCommand
case class UnregisterProbeSystemResult(op: UnregisterProbeSystem)
