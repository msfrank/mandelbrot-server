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
import akka.persistence.{Recover, SnapshotOffer, EventsourcedProcessor, Persistent}
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core._
import io.mandelbrot.core.notification.{NotificationPolicyType, NotificationService, Notification}
import io.mandelbrot.core.message.{StatusMessage, MessageStream}

/**
 *
 */
class RegistryManager extends EventsourcedProcessor with ActorLogging {
  import RegistryManager._

  // config
  override def processorId = "probe-registry"
  val settings = ServerConfig(context.system).settings.registry

  // state
  val probeSystems = new java.util.HashMap[URI,ActorRef](1024)


  /* subscribe to status messages */
  MessageStream(context.system).subscribe(self, classOf[StatusMessage])

  override def preStart(): Unit = {
    /* if a static registry is defined, then load static systems */
    for (registryFile <- settings.staticRegistry) {
      log.debug("loading static registry from {}", registryFile.getAbsolutePath)
      try {
        val staticRegistry = StaticRegistry(registryFile, settings)
        staticRegistry.systems.foreach { case ((uri, spec)) =>
          val ref = context.actorOf(ProbeSystem.props(uri, Some(spec)))
          context.watch(ref)
          probeSystems.put(uri, ref)
          log.debug("created static probe system {} at {}", uri, ref.path)
        }
      } catch {
        case ex: Throwable => log.error("failed to load static registry: {}", ex.getMessage)
      }
    }
    /* */
    self ! Recover()
  }

  override def postStop(): Unit = {
    //log.debug("snapshotting {}", processorId)
    //saveSnapshot(RegistryManagerSnapshot(probeSystems.keySet().toVector))
  }

  def receiveCommand = {

    /* register the ProbeSystem */
    case command @ RegisterProbeSystem(uri, registration) =>
      if (!probeSystems.containsKey(uri)) {
        persist(Event(command))(updateState(_, recovering = false))
      } else {
        sender() ! ProbeRegistryOperationFailed(command, new ApiException(Conflict))
      }

    /* create the ProbeSystem */
    case command @ CreateProbeSystem(uri, _) =>
      if (!probeSystems.containsKey(uri)) {
        persist(Event(command))(updateState(_, recovering = false))
      } else {
        sender() ! ProbeRegistryOperationFailed(command, new ApiException(Conflict))
      }

    /* update the ProbeSystem */
    case command @ UpdateProbeSystem(uri, spec) =>
      probeSystems.get(uri) match {
        case null =>
          sender() ! ProbeSystemOperationFailed(command, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          ref.forward(command)
      }

    /* terminate the ProbeSystem */
    case command @ UnregisterProbeSystem(uri) =>
      probeSystems.get(uri) match {
        case null =>
          sender() ! ProbeRegistryOperationFailed(command, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          persist(Event(command))(updateState(_, recovering = false))
      }

    /* return the list of registered ProbeSystems */
    case query: ListProbeSystems =>
      sender() ! ListProbeSystemsResult(query, probeSystems.keySet().toVector)

    /* forward ProbeSystem operations or return failure if system doesn't exist */
    case op: ProbeSystemOperation =>
      probeSystems.get(op.uri) match {
        case null =>
          sender() ! ProbeSystemOperationFailed(op, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          ref.forward(op)
      }

    /* forward Probe operations or return failure if system doesn't exist */
    case op: ProbeOperation =>
      probeSystems.get(op.probeRef.uri) match {
        case null =>
          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
        case ref: ActorRef =>
          ref.forward(op)
      }

    /* forward state messages to the appropriate ProbeSystem */
    case message: StatusMessage =>
      probeSystems.get(message.source.uri) match {
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
      updateState(event, recovering = true)

    /* recreate probe systems from snapshot */
    case SnapshotOffer(metadata, snapshot: RegistryManagerSnapshot) =>
      log.debug("loading snapshot of {} using offer {}", processorId, metadata)
      snapshot.probeSystems.foreach { uri =>
        // some probe systems may have been created statically in preStart(), so check
        // whether the actor exists before recreating
        if (!probeSystems.contains(uri)) {
          val ref = context.actorOf(ProbeSystem.props(uri))
          probeSystems.put(uri, ref)
          log.debug("loading probe system: {} -> {}", uri, ref.path)
        }
      }
  }

  def updateState(event: Event, recovering: Boolean) = event.event match {

    /* register the ProbeSystem */
    case command @ RegisterProbeSystem(uri, registration) =>
      val spec = ProbeConversions.registration2spec(registration)
      val ref = context.actorOf(ProbeSystem.props(uri, Some(spec)))
      context.watch(ref)
      probeSystems.put(uri, ref)
      log.debug("registered probe system {} at {}", uri, ref.path)
      if (!recovering)
        sender() ! RegisterProbeSystemResult(command, ref)

    /* create the ProbeSystem */
    case command @ CreateProbeSystem(uri, spec) =>
      val ref = context.actorOf(ProbeSystem.props(uri, Some(spec)))
      context.watch(ref)
      probeSystems.put(uri, ref)
      log.debug("created probe system {} at {}", uri, ref.path)
      if (!recovering)
        sender() ! CreateProbeSystemResult(command, ref)

    /* terminate the ProbeSystem */
    case command @ UnregisterProbeSystem(uri) =>
      val ref = probeSystems.get(uri)
      probeSystems.remove(uri)
      ref ! PoisonPill
      log.debug("deleted probe system {}", uri)
      if (!recovering)
        sender() ! UnregisterProbeSystemResult(command)
  }
}

object RegistryManager {
  def props() = Props(classOf[RegistryManager])

  case class Event(event: Any)
  case class RegistryManagerSnapshot(probeSystems: Vector[URI]) extends Serializable
}

/* contains tunable parameters for the probe */
case class ProbePolicy(joiningTimeout: Duration,
                       probeTimeout: Duration,
                       leavingTimeout: Duration,
                       flapWindow: Duration,
                       flapDeviations: Int,
                       notificationPolicy: NotificationPolicyType,
                       inherits: Boolean) extends Serializable

/* the probe specification */
case class ProbeSpec(objectType: String,
                     policy: Option[ProbePolicy],
                     metadata: Map[String,String],
                     children: Map[String,ProbeSpec],
                     static: Boolean) extends Serializable

/* a dynamic probe system registration */
case class ProbeRegistration(objectType: String,
                             policy: Option[ProbePolicy],
                             metadata: Map[String,String],
                             children: Map[String,ProbeRegistration]) extends Serializable

/* object registry operations */
sealed trait ProbeRegistryOperation
sealed trait ProbeRegistryQuery extends ProbeRegistryOperation
sealed trait ProbeRegistryCommand extends ProbeRegistryOperation
case class ProbeRegistryOperationFailed(op: ProbeRegistryOperation, failure: Throwable)

case class RegisterProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeRegistryCommand
case class RegisterProbeSystemResult(op: RegisterProbeSystem, ref: ActorRef)

case class CreateProbeSystem(uri: URI, spec: ProbeSpec) extends ProbeRegistryCommand
case class CreateProbeSystemResult(op: CreateProbeSystem, ref: ActorRef)

case class ListProbeSystems() extends ProbeRegistryQuery
case class ListProbeSystemsResult(op: ListProbeSystems, uris: Vector[URI])

case class UnregisterProbeSystem(uri: URI) extends ProbeRegistryCommand
case class UnregisterProbeSystemResult(op: UnregisterProbeSystem)
