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
import io.mandelbrot.core.system.StatusMessage
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core._
import io.mandelbrot.core.system.MessageStream

/**
 * the registry manager holds a map of all probe systems in memory, and is the parent actor
 * of all probe systems (which in turn are the parents of each probe in a system).  The registry
 * manager is responsible for accepting registration, update, and unregistration requests and
 * applying them to the appropriate probe system.
 */
class RegistryManager extends Actor with ActorLogging {
  import RegistryManager._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system).settings.registry
  val timeout = 5.seconds
  val maxInFlight: Int = 1024 // FIXME: make this a config parameter

  // state
  var services: ServiceMap = null
  val inFlight = new java.util.HashMap[ProbeRegistryOperation,OperationContext](maxInFlight)
  var currentLsn: Long = Long.MinValue
  val probeSystems = new java.util.HashMap[URI,ActorRef](1024)
  val unregisteredRefs = new java.util.HashMap[ActorRef,URI](64)
  var snapshotCancellable: Option[Cancellable] = None

  val registrar: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.registrar.plugin, settings.registrar.settings)
    log.info("loading registrar plugin {}", settings.registrar.plugin)
    context.actorOf(props, "registrar")
  }

  def receive = {

    /* we must receive this message before we can recover */
    case _services: ServiceMap =>
      services = _services
      registrar ! RecoverProbeSystems

    case op: ProbeRegistryOperation if inFlight.size() == maxInFlight =>
      sender() ! ProbeRegistryOperationFailed(op, new ApiException(RetryLater))

    /* register the ProbeSystem */
    case command: RegisterProbeSystem =>
      log.debug("op {}", command)
      registrar ! command
      val opContext = OperationContext(command, sender(), context.system.scheduler.scheduleOnce(timeout, self, OperationTimeout(command)))
      inFlight.put(command, opContext)

    /* update the ProbeSystem */
    case command: UpdateProbeSystem =>
      registrar ! command
      val opContext = OperationContext(command, sender(), context.system.scheduler.scheduleOnce(timeout, self, OperationTimeout(command)))
      inFlight.put(command, opContext)

    /* unregister the ProbeSystem */
    case command: UnregisterProbeSystem =>
      registrar ! command
      val opContext = OperationContext(command, sender(), context.system.scheduler.scheduleOnce(timeout, self, OperationTimeout(command)))
      inFlight.put(command, opContext)

//    /* return the list of registered ProbeSystems */
//    case query: ListProbeSystems =>
//      val systems = probeSystems.map { case (uri,system) => uri -> system.meta }.toMap
//      sender() ! ListProbeSystemsResult(query, systems)

    /* forward ProbeSystem operations or return failure if system doesn't exist */
    case op: ProbeSystemOperation =>
      probeSystems.get(op.uri) match {
        case null =>
          sender() ! ProbeSystemOperationFailed(op, new ApiException(ResourceNotFound))
        case actor: ActorRef =>
          actor.forward(op)
      }

    /* forward Probe operations or return failure if system doesn't exist */
    case op: ProbeOperation =>
      probeSystems.get(op.probeRef.uri) match {
        case null =>
          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
        case actor: ActorRef =>
          actor.forward(op)
      }

    /* forward state messages to the appropriate ProbeSystem */
    case message: StatusMessage =>
      probeSystems.get(message.source.uri) match {
        case null =>
          // do nothing
        case actor: ActorRef =>
          actor ! message
      }

    /* probe system actor has terminated */
    case Terminated(ref) =>
      val uri = unregisteredRefs.get(ref)
      log.debug("probe system {} has been terminated", uri)
      unregisteredRefs.remove(ref)
      val actor = probeSystems(uri)
      if (actor == ref)
        probeSystems.remove(uri)

    /* */
    case ProbeSystemRecovers(uri, registration, lsn) =>
      val actor = context.actorOf(ProbeSystem.props(uri, registration, lsn, services))
      log.debug("recovering probe system {} at {}", uri, actor.path)
      context.watch(actor)
      probeSystems.put(uri, actor)

    /* register the ProbeSystem */
    case ProbeSystemRegisters(command, timestamp, lsn) =>
      val actor = context.actorOf(ProbeSystem.props(command.uri, command.registration, lsn, services))
      log.debug("registering probe system {} at {}", command.uri, actor.path)
      context.watch(actor)
      probeSystems.put(command.uri, actor)
      inFlight.remove(command) match {
        case null =>
        case OperationContext(_, caller, cancellable) =>
          caller ! RegisterProbeSystemResult(command, actor)
          cancellable.cancel()
      }

    /* update the ProbeSystem */
    case ProbeSystemUpdates(command, timestamp, lsn) =>
      val actor = probeSystems.get(command.uri)
      log.debug("updating probe system {} at {}", command.uri, actor.path)
      actor ! ConfigureProbeSystem(command.registration, lsn)
      probeSystems.put(command.uri, actor)
      inFlight.remove(command) match {
        case null =>
        case OperationContext(_, caller, cancellable) =>
          caller ! UpdateProbeSystemResult(command, actor)
          cancellable.cancel()
      }

    /* terminate the ProbeSystem */
    case ProbeSystemUnregisters(command, timestamp, lsn) =>
      val actor = probeSystems.get(command.uri)
      log.debug("unregistering probe system {}", command.uri)
      unregisteredRefs.put(actor, command.uri)
      actor ! RetireProbeSystem(lsn)
      inFlight.remove(command) match {
        case null =>
        case OperationContext(_, caller, cancellable) =>
          caller ! UnregisterProbeSystemResult(command)
          cancellable.cancel()
      }

    /* async operation failed */
    case failure: ProbeRegistryOperationFailed =>
      inFlight.remove(failure.op) match {
        case null =>
        case OperationContext(_, caller, cancellable) =>
          log.debug("op fails: {} (cause: {})", failure, failure.failure.getCause)
          caller ! failure
          cancellable.cancel()
      }

    /* async operation timed out, notify caller */
    case OperationTimeout(op) =>
      inFlight.remove(op) match {
        case null =>
        case OperationContext(_, caller, cancellable) =>
          caller ! ProbeRegistryOperationFailed(op, new ApiException(RetryLater))
      }
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

  case object RecoverProbeSystems
  case class ProbeSystemRecovers(uri: URI, registration: ProbeRegistration, lsn: Long)
  case class OperationContext(op: ProbeRegistryOperation, caller: ActorRef, cancellable: Cancellable)
  case class OperationTimeout(op: ProbeRegistryOperation)
  case class ProbeSystemRegisters(command: RegisterProbeSystem, timestamp: DateTime, lsn: Long)
  case class ProbeSystemUpdates(command: UpdateProbeSystem, timestamp: DateTime, lsn: Long)
  case class ProbeSystemUnregisters(command: UnregisterProbeSystem, timestamp: DateTime, lsn: Long)
}

/* contains tunable parameters for the probe */
case class ProbePolicy(joiningTimeout: FiniteDuration,
                       probeTimeout: FiniteDuration,
                       alertTimeout: FiniteDuration,
                       leavingTimeout: FiniteDuration,
                       behavior: BehaviorPolicy,
                       notifications: Option[Set[String]]) extends Serializable

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
case class ProbeSystemMetadata(joinedOn: DateTime, lastUpdate: DateTime)

/* object registry operations */
sealed trait ProbeRegistryOperation
sealed trait ProbeRegistryQuery extends ProbeRegistryOperation
sealed trait ProbeRegistryCommand extends ProbeRegistryOperation
case class ProbeRegistryOperationFailed(op: ProbeRegistryOperation, failure: Throwable)

case class RegisterProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeRegistryCommand
case class RegisterProbeSystemResult(op: RegisterProbeSystem, ref: ActorRef)

case class UpdateProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeRegistryCommand
case class UpdateProbeSystemResult(op: UpdateProbeSystem, ref: ActorRef)

case class ListProbeSystems() extends ProbeRegistryQuery
case class ListProbeSystemsResult(op: ListProbeSystems, systems: Map[URI,ProbeSystemMetadata])

case class UnregisterProbeSystem(uri: URI) extends ProbeRegistryCommand
case class UnregisterProbeSystemResult(op: UnregisterProbeSystem)

/* marker trait for Registrar implementations */
trait Registrar
trait ClusteredRegistrar extends Registrar
