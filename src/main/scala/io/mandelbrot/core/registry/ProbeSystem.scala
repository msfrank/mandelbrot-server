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
import scala.collection.mutable
import java.net.URI
import java.util.UUID

import io.mandelbrot.core.{ServerConfig, ResourceNotFound, ApiException}
import io.mandelbrot.core.notification.{ProbeNotification, NotificationService, Notification}
import io.mandelbrot.core.http.TimeseriesParams
import io.mandelbrot.core.message.MandelbrotMessage
import io.mandelbrot.core.state.StateService
import io.mandelbrot.core.history._
import io.mandelbrot.core.tracking.TrackingService

/**
 *
 */
class ProbeSystem(uri: URI) extends Actor with ActorLogging {
  import ProbeSystem._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system).settings
  implicit val timeout = Timeout(5.seconds)   // TODO: pull this from settings

  // state
  var probes: Map[ProbeRef,ProbeActor] = Map.empty
  val retiredProbes = new mutable.HashMap[ActorRef,ProbeRef]()
  val zombieProbes = new mutable.HashSet[ProbeRef]
  var currentRegistration: Option[ProbeRegistration] = None

  val stateService = StateService(context.system)
  val notificationService = NotificationService(context.system)
  val historyService = HistoryService(context.system)
  val trackingService = TrackingService(context.system)

  var notifier: Option[ActorRef] = Some(notificationService)

  def receive = {

    /* initialize the probe system with the specified spec */
    case InitializeProbeSystem(registration) =>
      applyProbeRegistration(registration)
      log.debug("initialized probe system {}", uri)

    /* update the probe system with the specified spec */
    case command @ UpdateProbeSystem(_, registration) =>
      applyProbeRegistration(registration)
      log.debug("updated probe system {}", uri)

    /* get the ProbeSystem spec */
    case query: DescribeProbeSystem =>
      currentRegistration match {
        case Some(registration) =>
          sender() ! DescribeProbeSystemResult(query, registration)
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* acknowledge the specified probes in the probe system */
    case command: AcknowledgeProbeSystem =>
      val futures: Iterable[Future[Option[(ProbeRef,UUID)]]] = command.correlations.filter {
        case (ref: ProbeRef, correlation: UUID) => probes.contains(ref)
      }.map { case (ref: ProbeRef, correlation: UUID) =>
        val future = probes(ref).actor.ask(AcknowledgeProbe(ref, correlation))(timeout)
        future.map {
          case result: AcknowledgeProbeResult => Some(ref -> result.acknowledgementId)
          case result: ProbeOperationFailed => None
        }.mapTo[Option[(ProbeRef,UUID)]]
      }
      Future.sequence(futures).map {
        case results: Iterable[Option[(ProbeRef,UUID)]] =>
          val acknowledgements = results.flatten.toMap
          AcknowledgeProbeSystemResult(command, acknowledgements)
      }.recover {
        case ex: Throwable => ProbeSystemOperationFailed(command, ex)
      }.pipeTo(sender())

    /* unacknowledge the specified probes in the probe system */
    case command: UnacknowledgeProbeSystem =>
      val futures: Iterable[Future[Option[(ProbeRef,UUID)]]] = command.unacknowledgements.filter {
        case (ref: ProbeRef, unacknowledgement: UUID) => probes.contains(ref)
      }.map { case (ref: ProbeRef, unacknowledgement: UUID) =>
        val future = probes(ref).actor.ask(UnacknowledgeProbe(ref, unacknowledgement))(timeout)
        future.map {
          case result: UnacknowledgeProbeResult => Some(ref -> result.acknowledgementId)
          case result: ProbeOperationFailed => None
        }.mapTo[Option[(ProbeRef,UUID)]]
      }
      Future.sequence(futures).map {
        case results: Iterable[Option[(ProbeRef,UUID)]] =>
          val unacknowledgements = results.flatten.toMap
          UnacknowledgeProbeSystemResult(command, unacknowledgements)
      }.recover {
        case ex: Throwable => ProbeSystemOperationFailed(command, ex)
      }.pipeTo(sender())

    /* get the state of probes in the system */
    case query: GetProbeSystemStatus =>
      currentRegistration match {
        case Some(spec) =>
          val futures = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
            actor.actor.ask(GetProbeStatus(ref))(timeout).mapTo[GetProbeStatusResult]
          }
          Future.sequence(futures).map {
            case results: Set[GetProbeStatusResult] =>
              GetProbeSystemStatusResult(query, results.map(result => result.state.probeRef -> result.state).toMap)
          }.recover {
            case ex: Throwable => ProbeSystemOperationFailed(query, ex)
          }.pipeTo(sender())
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* get the metadata of probes in the system */
    case query: GetProbeSystemMetadata =>
      currentRegistration match {
        case Some(spec) =>
          val metadata = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
            ref -> findProbeSpec(spec, ref.path).metadata
          }.toMap
          sender() ! GetProbeSystemMetadataResult(query, metadata)
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* get the policy of probes in the system */
    case query: GetProbeSystemPolicy =>
      currentRegistration match {
        case Some(spec) =>
          val policy = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
            ref -> findProbeSpec(spec, ref.path).policy
          }.toMap
          sender() ! GetProbeSystemPolicyResult(query, policy)
        case None =>
          sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
      }

    /* get the status history for the specified probes */
    case query: GetProbeSystemStatusHistory =>
      val from = query.params.from
      val to = query.params.to
      val limit = query.params.limit
      val q = if (query.paths.isEmpty) GetStatusHistory(Left(ProbeRef(uri)), from, to, limit) else {
        val refs = findMatching(query.paths).map(_._1)
        GetStatusHistory(Right(refs), from, to, limit)
      }
      historyService.ask(q).map {
        case GetStatusHistoryResult(_, history) =>
          GetProbeSystemStatusHistoryResult(query, history)
        case failure: HistoryServiceOperationFailed =>
          ProbeSystemOperationFailed(query, failure.failure)
      }.pipeTo(sender())

    /* get the notification history for the specified probes */
    case query: GetProbeSystemNotificationHistory =>
      val from = query.params.from
      val to = query.params.to
      val limit = query.params.limit
      val q = if (query.paths.isEmpty) GetNotificationHistory(Left(ProbeRef(uri)), from, to, limit) else {
        val refs = findMatching(query.paths).map(_._1)
        GetNotificationHistory(Right(refs), from, to, limit)
      }
      historyService.ask(q).map {
        case GetNotificationHistoryResult(_, history) =>
          GetProbeSystemNotificationHistoryResult(query, history)
        case failure: HistoryServiceOperationFailed =>
          ProbeSystemOperationFailed(query, failure.failure)
      }.pipeTo(sender())

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
      notifier.foreach(_ ! notification)

    /* retire all running probes */
    case command: UnregisterProbeSystem =>
      probes.foreach {
        case (ref,probeactor) if !retiredProbes.contains(probeactor.actor) =>
          probeactor.actor ! RetireProbe
          retiredProbes.put(probeactor.actor, ref)
        case _ => // do nothing
      }

    /* clean up retired probes, reanimate zombie probes */
    case Terminated(actorref) =>
      val proberef = retiredProbes(actorref)
      probes = probes - proberef
      retiredProbes.remove(actorref)
      if (zombieProbes.contains(proberef)) {
        zombieProbes.remove(proberef)
        applyProbeRegistration(currentRegistration.get)
      } else {
        log.debug("probe {} has been terminated", proberef)
      }
      if (probes.isEmpty)
        context.stop(self)

  }

  /**
   * flatten ProbeRegistration into a Set of ProbeRefs
   */
  def spec2RefSet(path: Vector[String], spec: ProbeSpec): Set[ProbeRef] = {
    val iterChildren = spec.children.toSet
    val childRefs = iterChildren.map { case (name: String, childSpec: ProbeSpec) =>
      spec2RefSet(path :+ name, childSpec)
    }.flatten
    childRefs + ProbeRef(uri, path)
  }
  def registration2RefSet(registration: ProbeRegistration): Set[ProbeRef] = {
    registration.probes.flatMap { case (name,spec) =>
      spec2RefSet(Vector(name), spec)
    }.toSet
  }

  /**
   * find the ProbeSpec referenced by path.  NOTE: It is assumed that the specified
   * ProbeRef exists!  if it doesn't, this code will throw an exception.
   */
  def findProbeSpec(spec: ProbeSpec, path: Vector[String]): ProbeSpec = {
    if (path.isEmpty) spec else findProbeSpec(spec.children(path.head), path.tail)
  }
  def findProbeSpec(registration: ProbeRegistration, path: Vector[String]): ProbeSpec = {
    findProbeSpec(registration.probes(path.head), path.tail)
  }

  /**
   * apply the spec to the probe system, adding and removing probes as necessary
   */
  def applyProbeRegistration(registration: ProbeRegistration): Unit = {
    val specSet = registration2RefSet(registration)
    val probeSet = probes.keySet
    // add new probes
    val probesAdded = specSet -- probeSet
    probesAdded.toVector.sorted.foreach { case ref: ProbeRef =>
      val probeSpec = findProbeSpec(registration, ref.path)
      val actor = ref.parentOption match {
        case Some(parent) if !parent.path.isEmpty =>
          context.actorOf(Probe.props(ref, probes(parent).actor, probeSpec.policy, stateService, notificationService, trackingService))
        case _ =>
          context.actorOf(Probe.props(ref, self, probeSpec.policy, stateService, notificationService, trackingService))
      }
      context.watch(actor)
      log.debug("probe {} joins", ref)
      probes = probes + (ref -> ProbeActor(probeSpec, actor))
      stateService ! ProbeMetadata(ref, registration.metadata)
    }
    // remove stale probes
    val probesRemoved = probeSet -- specSet
    probesRemoved.toVector.sorted.reverse.foreach { case ref: ProbeRef =>
      log.debug("probe {} retires", ref)
      val probeactor = probes(ref)
      probeactor.actor ! RetireProbe
      retiredProbes.put(probeactor.actor, ref)
    }
    // update existing probes and mark zombie probes
    val probesUpdated = probeSet.intersect(specSet)
    probesUpdated.foreach {
      case ref: ProbeRef if retiredProbes.contains(probes(ref).actor) =>
        zombieProbes.add(ref)
      case ref: ProbeRef =>
        val probeSpec = findProbeSpec(registration, ref.path)
        probes(ref).actor ! UpdateProbe(probeSpec.policy)
    }
    currentRegistration = Some(registration)
  }

  /**
   *
   */
  def findMatching(paths: Option[Set[String]]): Set[(ProbeRef,ProbeActor)] = paths match {
    case None =>
      probes.toSet
    case Some(_paths) =>
      val parser = new ProbeMatcherParser()
      val matchers = _paths.map(path => parser.parseProbeMatcher(uri.toString + path))
      probes.flatMap { case matching @ (ref,actor) =>
        matchers.collectFirst {
          case matcher if matcher.matches(ref) =>
            matching
        }
      }.toSet
  }
}

object ProbeSystem {
  def props(uri: URI) = Props(classOf[ProbeSystem], uri)

  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)
  case class InitializeProbeSystem(registration: ProbeRegistration)
  case class UpdateProbe(policy: ProbePolicy)
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
case class DescribeProbeSystemResult(op: DescribeProbeSystem, registration: ProbeRegistration)

case class UpdateProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeSystemCommand
case class UpdateProbeSystemResult(op: UpdateProbeSystem, ref: ActorRef)

case class AcknowledgeProbeSystem(uri: URI, correlations: Map[ProbeRef,UUID]) extends ProbeSystemCommand
case class AcknowledgeProbeSystemResult(op: AcknowledgeProbeSystem, acknowledgements: Map[ProbeRef,UUID])

case class UnacknowledgeProbeSystem(uri: URI, unacknowledgements: Map[ProbeRef,UUID]) extends ProbeSystemCommand
case class UnacknowledgeProbeSystemResult(op: UnacknowledgeProbeSystem, unacknowledgements: Map[ProbeRef,UUID])

case class GetProbeSystemStatus(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemStatusResult(op: GetProbeSystemStatus, state: Map[ProbeRef,ProbeStatus])

case class GetProbeSystemMetadata(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemMetadataResult(op: GetProbeSystemMetadata, metadata: Map[ProbeRef,Map[String,String]])

case class GetProbeSystemPolicy(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemPolicyResult(op: GetProbeSystemPolicy, policy: Map[ProbeRef,ProbePolicy])

case class GetProbeSystemStatusHistory(uri: URI, paths: Option[Set[String]], params: TimeseriesParams) extends ProbeSystemQuery
case class GetProbeSystemStatusHistoryResult(op: GetProbeSystemStatusHistory, history: Vector[ProbeStatus])

case class GetProbeSystemNotificationHistory(uri: URI, paths: Option[Set[String]], params: TimeseriesParams) extends ProbeSystemQuery
case class GetProbeSystemNotificationHistoryResult(op: GetProbeSystemNotificationHistory, history: Vector[ProbeNotification])