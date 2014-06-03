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

import com.typesafe.config.Config
import akka.actor._
import akka.persistence.{Recover, SnapshotOffer, EventsourcedProcessor}
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core._
import io.mandelbrot.core.notification.{NotificationPolicy, NotificationBehavior, NotificationService, Notification}
import io.mandelbrot.core.message.{StatusMessage, MessageStream}
import org.joda.time.{DateTimeZone, DateTime}

/**
 *
 */
class RegistryManager extends EventsourcedProcessor with ActorLogging {
  import RegistryManager._
  import ProbeSystem.InitializeProbeSystem

  // config
  override def processorId = "registry-manager"
  val settings = ServerConfig(context.system).settings.registry

  // state
  val probeSystems = new java.util.HashMap[URI,ProbeSystemActor](1024)
  val unregisteredRefs = new java.util.HashMap[ActorRef,URI](64)

  /* subscribe to status messages */
  MessageStream(context.system).subscribe(self, classOf[StatusMessage])

  override def preStart(): Unit = {
    self ! Recover()
  }

  override def postStop(): Unit = {
//    log.debug("snapshotting {}", processorId)
//    val systems = probeSystems.map {case (uri,system) => uri -> (system.registration,system.meta)}.toMap
//    saveSnapshot(RegistryManagerSnapshot(systems))
  }

  def receiveCommand = {

    /* register the ProbeSystem */
    case command: RegisterProbeSystem =>
      if (!probeSystems.containsKey(command.uri)) {
        if (registrationValid(command.registration))
          persist(Event(command, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))
        else
          sender() ! ProbeRegistryOperationFailed(command, new ApiException(BadRequest))
      } else {
        sender() ! ProbeRegistryOperationFailed(command, new ApiException(Conflict))
      }

    /* update the ProbeSystem */
    case command: UpdateProbeSystem =>
      probeSystems.get(command.uri) match {
        case null =>
          sender() ! ProbeSystemOperationFailed(command, new ApiException(ResourceNotFound))
        case system: ProbeSystemActor if !registrationValid(command.registration) =>
          sender() ! ProbeSystemOperationFailed(command, new ApiException(BadRequest))
        case system: ProbeSystemActor =>
          persist(Event(command, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))
      }

    /* unregister the ProbeSystem */
    case command: UnregisterProbeSystem =>
      probeSystems.get(command.uri) match {
        case null =>
          sender() ! ProbeRegistryOperationFailed(command, new ApiException(ResourceNotFound))
        case system: ProbeSystemActor if unregisteredRefs.contains(system.actor) =>
          sender() ! ProbeRegistryOperationFailed(command, new ApiException(Conflict))
        case system: ProbeSystemActor =>
          persist(Event(command, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))
      }

    /* return the list of registered ProbeSystems */
    case query: ListProbeSystems =>
      val systems = probeSystems.map { case (uri,system) => uri -> system.meta }.toMap
      sender() ! ListProbeSystemsResult(query, systems)

    /* forward ProbeSystem operations or return failure if system doesn't exist */
    case op: ProbeSystemOperation =>
      probeSystems.get(op.uri) match {
        case null =>
          sender() ! ProbeSystemOperationFailed(op, new ApiException(ResourceNotFound))
        case system: ProbeSystemActor =>
          system.actor.forward(op)
      }

    /* forward Probe operations or return failure if system doesn't exist */
    case op: ProbeOperation =>
      probeSystems.get(op.probeRef.uri) match {
        case null =>
          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
        case system: ProbeSystemActor =>
          system.actor.forward(op)
      }

    /* forward state messages to the appropriate ProbeSystem */
    case message: StatusMessage =>
      probeSystems.get(message.source.uri) match {
        case null =>
          // do nothing
        case system: ProbeSystemActor =>
          system.actor ! message
      }

    /* handle notifications which have been passed up from ProbeSystems */
    case notification: Notification =>
      NotificationService(context.system) ! notification

    /* probe system actor has terminated */
    case Terminated(ref) =>
      val uri = unregisteredRefs.get(ref)
      log.debug("probe system {} has been terminated", uri)
      probeSystems.remove(uri)
      unregisteredRefs.remove(ref)

  }

  def receiveRecover = {

    case event: Event =>
      updateState(event, recovering = true)

    /* recreate probe systems from snapshot */
    case SnapshotOffer(metadata, snapshot: RegistryManagerSnapshot) =>
      log.debug("loading snapshot of {} using offer {}", processorId, metadata)
      snapshot.probeSystems.foreach { case (uri,(registration,meta)) =>
        val actor = context.actorOf(ProbeSystem.props(uri))
        actor ! InitializeProbeSystem(registration)
        probeSystems.put(uri, ProbeSystemActor(registration, actor, meta))
        log.debug("loading probe system {}", uri)
      }
  }

  def updateState(event: Event, recovering: Boolean) = event.event match {

    /* register the ProbeSystem */
    case command @ RegisterProbeSystem(uri, registration) =>
      val actor = context.actorOf(ProbeSystem.props(uri))
      log.debug("registering probe system {} at {}", uri, actor.path)
      actor ! InitializeProbeSystem(registration)
      context.watch(actor)
      val meta = ProbeSystemMetadata(event.timestamp, event.timestamp, None)
      probeSystems.put(uri, ProbeSystemActor(registration, actor, meta))
      if (!recovering)
        sender() ! RegisterProbeSystemResult(command, actor)

    /* update the ProbeSystem */
    case command @ UpdateProbeSystem(uri, registration) =>
      val system = probeSystems.get(uri)
      log.debug("updating probe system {} at {}", uri, system.actor.path)
      system.actor ! command
      probeSystems.put(uri, system.copy(meta = system.meta.copy(lastUpdate = event.timestamp)))
      if (!recovering)
        sender() ! UpdateProbeSystemResult(command, system.actor)

    /* terminate the ProbeSystem */
    case command @ UnregisterProbeSystem(uri) =>
      val system = probeSystems.get(uri)
      log.debug("unregistering probe system {}", uri)
      unregisteredRefs.put(system.actor, uri)
      system.actor ! command
      if (!recovering)
        sender() ! UnregisterProbeSystemResult(command)
  }

  /**
   * Returns true if the specified registration parameters adhere to server
   * policy, otherwise returns false.
   */
  def registrationValid(registration: ProbeRegistration): Boolean = {
    // FIXME: implement validation logic
    true
  }
}

object RegistryManager {
  def props() = Props(classOf[RegistryManager])

  def settings(config: Config): Option[Any] = None

  case class ProbeSystemActor(registration: ProbeRegistration, actor: ActorRef, meta: ProbeSystemMetadata)
  case class Event(event: Any, timestamp: DateTime)
  case class RegistryManagerSnapshot(probeSystems: Map[URI,(ProbeRegistration,ProbeSystemMetadata)]) extends Serializable
}

/* contains tunable parameters for the probe */
case class ProbePolicy(joiningTimeout: FiniteDuration,
                       probeTimeout: FiniteDuration,
                       alertTimeout: FiniteDuration,
                       leavingTimeout: FiniteDuration,
                       flapWindow: FiniteDuration,
                       flapDeviations: Int,
                       notificationPolicy: NotificationPolicy) extends Serializable

/* the probe specification */
case class ProbeSpec(probeType: String,
                     metadata: Map[String,String],
                     policy: ProbePolicy,
                     children: Map[String,ProbeSpec]) extends Serializable

/* a dynamic probe system registration */
case class ProbeRegistration(systemType: String,
                             metadata: Map[String,String],
                             probes: Map[String,ProbeSpec]) extends Serializable

/* */
case class ProbeSystemMetadata(joinedOn: DateTime, lastUpdate: DateTime, retiredOn: Option[DateTime])

/* object registry operations */
sealed trait ProbeRegistryOperation
sealed trait ProbeRegistryQuery extends ProbeRegistryOperation
sealed trait ProbeRegistryCommand extends ProbeRegistryOperation
case class ProbeRegistryOperationFailed(op: ProbeRegistryOperation, failure: Throwable)

case class RegisterProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeRegistryCommand
case class RegisterProbeSystemResult(op: RegisterProbeSystem, ref: ActorRef)

case class ListProbeSystems() extends ProbeRegistryQuery
case class ListProbeSystemsResult(op: ListProbeSystems, systems: Map[URI,ProbeSystemMetadata])

case class UnregisterProbeSystem(uri: URI) extends ProbeRegistryCommand
case class UnregisterProbeSystemResult(op: UnregisterProbeSystem)
