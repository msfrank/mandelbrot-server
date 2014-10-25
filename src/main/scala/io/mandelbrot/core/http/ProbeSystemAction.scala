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

package io.mandelbrot.core.http

import io.mandelbrot.core.system.{ProbeLink, ProbeStatus, ProbeRef}
import org.joda.time.DateTime
import java.net.URI
import java.util.UUID

import io.mandelbrot.core.registry._
import io.mandelbrot.core.notification.{ProbeNotification, NotificationEvent}
import io.mandelbrot.core.history._

///**
// * the ProbeSystem manages a collection of Probes underneath a URI.  the ProbeSystem
// * is responsible for adding and removing probes when the registration changes, as well
// * as updating probes when policy changes.  lastly, the ProbeSystem acts as an endpoint
// * for commands and queries operating on sets of probes in the system.
// */
//class ProbeSystem(services: ActorRef) extends LoggingFSM[SystemFSMState,SystemFSMData] with Stash {
//  import ProbeSystem._
//  import context.dispatcher
//
//  // config
//  val settings = ServerConfig(context.system).settings
//  implicit val timeout = Timeout(5.seconds)   // TODO: pull this from settings
//
//  // state
//  var uri: URI = null
//  var registration: ProbeRegistration = null
//  var generation: Long = 0
//  var probes: Map[ProbeRef,ProbeActor] = Map.empty
//  val retiredProbes = new mutable.HashMap[ActorRef,(ProbeRef,Long)]
//  val zombieProbes = new mutable.HashSet[ProbeRef]
//  val links = new mutable.HashMap[ProbeRef,ProbeLink]
//  val metricsBus = new MetricsBus()
//
//  override def preStart(): Unit = {
//    startWith(SystemIncubating, SystemWaiting)
//    initialize()
//  }
//
//  when(SystemIncubating) {
//    case Event(op: ProbeSystemOperation, _) =>
//      stash()
//      goto(SystemInitializing) using SystemInitializing(op.uri)
//
//    case Event(op: ProbeOperation, _) =>
//      stash()
//      goto(SystemInitializing) using SystemInitializing(op.probeRef.uri)
//  }
//
//  onTransition {
//    case SystemIncubating -> SystemInitializing => nextStateData match {
//      case state: SystemInitializing => services ! DescribeProbeSystem(state.uri)
//    }
//  }
//
//  when(SystemInitializing) {
//    case Event(result: DescribeProbeSystemResult, state: SystemInitializing) =>
//      goto(SystemRunning) using SystemRunning(state.uri, result.registration, 0)
//    case Event(failure: RegistryServiceOperationFailed, state: SystemInitializing) =>
//      goto(SystemFailed) using SystemError(failure.failure)
//    case Event(op: ProbeSystemOperation, _) =>
//      stash()
//      stay()
//    case Event(op: ProbeOperation, _) =>
//      stash()
//      stay()
//  }
//
//  onTransition {
//    case _ -> SystemRunning => nextStateData match {
//      case state: SystemRunning =>
//        applyProbeRegistration(state.registration, state.lsn)
//        unstashAll()
//    }
//  }
//
//  when (SystemRunning) {
//
//    /* initialize or update the probe system with the specified spec */
//    case ConfigureProbeSystem(newRegistration, lsn) =>
//      applyProbeRegistration(newRegistration, lsn)
//
//    /* get the ProbeSystem spec */
//    case query: DescribeProbeSystem =>
//      sender() ! DescribeProbeSystemResult(query, registration)
//
//    /* acknowledge the specified probes in the probe system */
//    case command: AcknowledgeProbeSystem =>
//      val futures: Iterable[Future[Option[(ProbeRef,UUID)]]] = command.correlations.filter {
//        case (ref: ProbeRef, correlation: UUID) => probes.contains(ref)
//      }.map { case (ref: ProbeRef, correlation: UUID) =>
//        val future = probes(ref).actor.ask(AcknowledgeProbe(ref, correlation))(timeout)
//        future.map {
//          case result: AcknowledgeProbeResult => Some(ref -> result.acknowledgementId)
//          case result: ProbeOperationFailed => None
//        }.mapTo[Option[(ProbeRef,UUID)]]
//      }
//      Future.sequence(futures).map {
//        case results: Iterable[Option[(ProbeRef,UUID)]] =>
//          val acknowledgements = results.flatten.toMap
//          AcknowledgeProbeSystemResult(command, acknowledgements)
//      }.recover {
//        case ex: Throwable => ProbeSystemOperationFailed(command, ex)
//      }.pipeTo(sender())
//
//    /* unacknowledge the specified probes in the probe system */
//    case command: UnacknowledgeProbeSystem =>
//      val futures: Iterable[Future[Option[(ProbeRef,UUID)]]] = command.unacknowledgements.filter {
//        case (ref: ProbeRef, unacknowledgement: UUID) => probes.contains(ref)
//      }.map { case (ref: ProbeRef, unacknowledgement: UUID) =>
//        val future = probes(ref).actor.ask(UnacknowledgeProbe(ref, unacknowledgement))(timeout)
//        future.map {
//          case result: UnacknowledgeProbeResult => Some(ref -> result.acknowledgementId)
//          case result: ProbeOperationFailed => None
//        }.mapTo[Option[(ProbeRef,UUID)]]
//      }
//      Future.sequence(futures).map {
//        case results: Iterable[Option[(ProbeRef,UUID)]] =>
//          val unacknowledgements = results.flatten.toMap
//          UnacknowledgeProbeSystemResult(command, unacknowledgements)
//      }.recover {
//        case ex: Throwable => ProbeSystemOperationFailed(command, ex)
//      }.pipeTo(sender())
//
//    /* get the state of probes in the system */
//    case query: GetProbeSystemStatus =>
//      val futures = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
//        actor.actor.ask(GetProbeStatus(ref))(timeout).mapTo[GetProbeStatusResult]
//      }
//      if (futures.isEmpty)
//        sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
//      else
//        Future.sequence(futures).map {
//          case results: Set[GetProbeStatusResult] =>
//            GetProbeSystemStatusResult(query, results.map(result => result.state.probeRef -> result.state).toMap)
//        }.recover {
//          case ex: Throwable => ProbeSystemOperationFailed(query, ex)
//        }.pipeTo(sender())
//
//    /* get the metadata of probes in the system */
//    case query: GetProbeSystemMetadata =>
//      val metadata = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
//        ref -> findProbeSpec(registration, ref.path).metadata
//      }.toMap
//      if (metadata.isEmpty)
//        sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
//      else
//        sender() ! GetProbeSystemMetadataResult(query, metadata)
//
//    /* get the policy of probes in the system */
//    case query: GetProbeSystemPolicy =>
//      val policy = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
//        ref -> findProbeSpec(registration, ref.path).policy
//      }.toMap
//      if (policy.isEmpty)
//        sender() ! ProbeSystemOperationFailed(query, new ApiException(ResourceNotFound))
//      else
//        sender() ! GetProbeSystemPolicyResult(query, policy)
//
//    /* get the status history for the specified probes */
//    case query: GetProbeSystemStatusHistory =>
//      val q = if (query.paths.isEmpty) GetStatusHistory(Left(ProbeRef(uri)), query.from, query.to, query.limit) else {
//        val refs = findMatching(query.paths).map(_._1)
//        GetStatusHistory(Right(refs), query.from, query.to, query.limit)
//      }
//      services.ask(q).map {
//        case GetStatusHistoryResult(_, history) =>
//          GetProbeSystemStatusHistoryResult(query, history)
//        case failure: HistoryServiceOperationFailed =>
//          ProbeSystemOperationFailed(query, failure.failure)
//      }.pipeTo(sender())
//
//    /* get the notification history for the specified probes */
//    case query: GetProbeSystemNotificationHistory =>
//      val q = if (query.paths.isEmpty) GetNotificationHistory(Left(ProbeRef(uri)), query.from, query.to, query.limit) else {
//        val refs = findMatching(query.paths).map(_._1)
//        GetNotificationHistory(Right(refs), query.from, query.to, query.limit)
//      }
//      services.ask(q).map {
//        case GetNotificationHistoryResult(_, history) =>
//          GetProbeSystemNotificationHistoryResult(query, history)
//        case failure: HistoryServiceOperationFailed =>
//          ProbeSystemOperationFailed(query, failure.failure)
//      }.pipeTo(sender())
//
//    /* send status message to specified probe */
//    case message: StatusMessage =>
//      probes.get(message.probeRef) match {
//        case Some(probeActor: ProbeActor) =>
//          probeActor.actor ! message
//        case None =>
//          log.warning("ignoring message {}: probe source is not known", message)
//      }
//
//    /* send metrics message to all interested probes */
//    case message: MetricsMessage =>
//      if (probes.contains(message.probeRef))
//        metricsBus.publish(message)
//      else
//        log.warning("ignoring message {}: probe source is not known", message)
//
//    /* ignore probe status from top level probes */
//    case status: ProbeStatus =>
//
//    /* forward probe operations to the specified probe */
//    case op: ProbeOperation =>
//      probes.get(op.probeRef) match {
//        case Some(probeActor: ProbeActor) =>
//          probeActor.actor.forward(op)
//        case None =>
//          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
//      }
//
//    /* handle notifications which have been passed up from Probe */
//    case notification: NotificationEvent =>
//      services ! notification
//
//    /* retire all running probes */
//    case command: RetireProbeSystem =>
//      probes.foreach {
//        case (ref,probeactor) if !retiredProbes.contains(probeactor.actor) =>
//          probeactor.actor ! RetireProbe(command.lsn)
//          retiredProbes.put(probeactor.actor, (ref,command.lsn))
//        case _ => // do nothing
//      }
//
//    /* clean up retired probes, reanimate zombie probes */
//    case Terminated(actorref) =>
//      val (proberef,lsn) = retiredProbes(actorref)
//      probes = probes - proberef
//      retiredProbes.remove(actorref)
//      if (zombieProbes.contains(proberef)) {
//        zombieProbes.remove(proberef)
//        applyProbeRegistration(registration, lsn)
//      } else
//        log.debug("probe {} has been terminated", proberef)
//      if (probes.isEmpty)
//        context.stop(self)
//
//  }
//
//  onTransition {
//    case _ -> SystemFailed => unstashAll()
//  }
//
//  when(SystemFailed) {
//    case Event(op: ProbeSystemOperation, state: SystemError) =>
//      stop() replying ProbeSystemOperationFailed(op, state.ex)
//    case Event(op: ProbeOperation, state: SystemError) =>
//      stop() replying ProbeOperationFailed(op, state.ex)
//  }
//
//  /**
//   * flatten ProbeRegistration into a Set of ProbeRefs
//   */
//  def spec2RefSet(path: Vector[String], spec: ProbeSpec): Set[ProbeRef] = {
//    val iterChildren = spec.children.toSet
//    val childRefs = iterChildren.map { case (name: String, childSpec: ProbeSpec) =>
//      spec2RefSet(path :+ name, childSpec)
//    }.flatten
//    childRefs + ProbeRef(uri, path)
//  }
//  def registration2RefSet(registration: ProbeRegistration): Set[ProbeRef] = {
//    registration.probes.flatMap { case (name,spec) =>
//      spec2RefSet(Vector(name), spec)
//    }.toSet
//  }
//
//  /**
//   * find the ProbeSpec referenced by path.  NOTE: It is assumed that the specified
//   * ProbeRef exists!  if it doesn't, this code will throw an exception.
//   */
//  def findProbeSpec(spec: ProbeSpec, path: Vector[String]): ProbeSpec = {
//    if (path.isEmpty) spec else findProbeSpec(spec.children(path.head), path.tail)
//  }
//  def findProbeSpec(registration: ProbeRegistration, path: Vector[String]): ProbeSpec = {
//    findProbeSpec(registration.probes(path.head), path.tail)
//  }
//
//  /**
//   * apply the spec to the probe system, adding and removing probes as necessary
//   */
//  def applyProbeRegistration(newRegistration: ProbeRegistration, lsn: Long): Unit = {
//    log.debug("configuring probe system {}", uri)
//    val specSet = registration2RefSet(newRegistration)
//    val probeSet = probes.keySet
//    // add new probes
//    val probesAdded = specSet -- probeSet
//    probesAdded.toVector.sorted.foreach { case ref: ProbeRef =>
//      val probeSpec = findProbeSpec(newRegistration, ref.path)
//      val directChildren = specSet.filter { _.parentOption match {
//        case Some(parent) => parent == ref
//        case None => false
//      }}
//      val actor = ref.parentOption match {
//        case Some(parent) if parent.path.nonEmpty =>
//          context.actorOf(Probe.props(ref, probes(parent).actor, directChildren, probeSpec.policy, probeSpec.behavior, lsn, services, metricsBus))
//        case _ =>
//          context.actorOf(Probe.props(ref, self, directChildren, probeSpec.policy, probeSpec.behavior, lsn, services, metricsBus))
//      }
//      context.watch(actor)
//      log.debug("probe {} joins", ref)
//      probes = probes + (ref -> ProbeActor(probeSpec, actor))
//      services ! ProbeMetadata(ref, newRegistration.metadata)
//    }
//    // remove stale probes
//    val probesRemoved = probeSet -- specSet
//    probesRemoved.toVector.sorted.reverse.foreach { case ref: ProbeRef =>
//      log.debug("probe {} retires", ref)
//      val probeactor = probes(ref)
//      probeactor.actor ! RetireProbe(lsn)
//      retiredProbes.put(probeactor.actor, (ref,lsn))
//    }
//    // update existing probes and mark zombie probes
//    val probesUpdated = probeSet.intersect(specSet)
//    probesUpdated.foreach {
//      case ref: ProbeRef if retiredProbes.contains(probes(ref).actor) =>
//        zombieProbes.add(ref)
//      case ref: ProbeRef =>
//        val probeSpec = findProbeSpec(newRegistration, ref.path)
//        val directChildren = specSet.filter { _.parentOption match {
//          case Some(parent) => parent == ref
//          case None => false
//        }}
//        val ProbeActor(prevSpec, actor) = probes(ref)
//        probes = probes + (ref -> ProbeActor(probeSpec, actor))
//        if (probeSpec.behavior.getClass == prevSpec.behavior.getClass)
//          actor ! UpdateProbe(directChildren, probeSpec.policy, probeSpec.behavior, lsn)
//        else
//          actor ! ChangeProbe(directChildren, probeSpec.policy, probeSpec.behavior, lsn)
//    }
//    registration = newRegistration
//  }
//
//  /**
//   *
//   */
//  def findMatching(paths: Option[Set[String]]): Set[(ProbeRef,ProbeActor)] = paths match {
//    case None =>
//      probes.toSet
//    case Some(_paths) =>
//      val parser = new ProbeMatcherParser()
//      val matchers = _paths.map(path => parser.parseProbeMatcher(uri.toString + path))
//      probes.flatMap { case matching @ (ref,actor) =>
//        matchers.collectFirst {
//          case matcher if matcher.matches(ref) =>
//            matching
//        }
//      }.toSet
//  }
//}
//
//object ProbeSystem {
//  def props(services: ActorRef) =  Props(classOf[ProbeSystem], services)
//  val idExtractor: ShardRegion.IdExtractor = {
//    case op: ProbeSystemOperation => (op.uri.toString, op)
//    case op: ProbeOperation => (op.probeRef.uri.toString, op)
//  }
//  val shardResolver: ShardRegion.ShardResolver = {
//    case op: ProbeSystemOperation => op.uri.toString
//    case op: ProbeOperation => op.probeRef.uri.toString
//  }
//  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)
//}
//
//sealed trait SystemFSMState
//case object SystemIncubating extends SystemFSMState
//case object SystemInitializing extends SystemFSMState
//case object SystemRunning extends SystemFSMState
//case object SystemFailed extends SystemFSMState
//
//sealed trait SystemFSMData
//case object SystemWaiting extends SystemFSMData
//case class SystemInitializing(uri: URI) extends SystemFSMData
//case class SystemRunning(uri: URI, registration: ProbeRegistration, lsn: Long) extends SystemFSMData
//case class SystemError(ex: Throwable) extends SystemFSMData
//
//case class ConfigureProbeSystem(registration: ProbeRegistration, lsn: Long)
//case class UpdateProbe(children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior, lsn: Long)
//case class ChangeProbe(children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior, lsn: Long)
//case class RetireProbe(lsn: Long)
//case class RetireProbeSystem(lsn: Long)
//
///* describes a link to a probe subtree from a different probe system */
//case class ProbeLink(localRef: ProbeRef, remoteUrl: URI, remoteMatch: String)


/**
*
*/
sealed trait ProbeSystemOperation { val uri: URI }
sealed trait ProbeSystemCommand extends ProbeSystemOperation
sealed trait ProbeSystemQuery extends ProbeSystemOperation
case class ProbeSystemOperationFailed(op: ProbeSystemOperation, failure: Throwable)

case class AcknowledgeProbeSystem(uri: URI, correlations: Map[ProbeRef,UUID]) extends ProbeSystemCommand
case class AcknowledgeProbeSystemResult(op: AcknowledgeProbeSystem, acknowledgements: Map[ProbeRef,UUID])

case class UnacknowledgeProbeSystem(uri: URI, unacknowledgements: Map[ProbeRef,UUID]) extends ProbeSystemCommand
case class UnacknowledgeProbeSystemResult(op: UnacknowledgeProbeSystem, unacknowledgements: Map[ProbeRef,UUID])

case class GetProbeSystemStatus(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemStatusResult(op: GetProbeSystemStatus, status: Map[ProbeRef,ProbeStatus])

case class GetProbeSystemMetadata(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemMetadataResult(op: GetProbeSystemMetadata, metadata: Map[ProbeRef,Map[String,String]])

case class GetProbeSystemPolicy(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemPolicyResult(op: GetProbeSystemPolicy, policy: Map[ProbeRef,ProbePolicy])

case class GetProbeSystemLinks(uri: URI, paths: Option[Set[String]]) extends ProbeSystemQuery
case class GetProbeSystemLinksResult(op: GetProbeSystemLinks, links: Map[ProbeRef,ProbeLink])

case class GetProbeSystemStatusHistory(uri: URI, paths: Option[Set[String]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends ProbeSystemQuery
case class GetProbeSystemStatusHistoryResult(op: GetProbeSystemStatusHistory, history: Vector[ProbeStatus])

case class GetProbeSystemNotificationHistory(uri: URI, paths: Option[Set[String]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends ProbeSystemQuery
case class GetProbeSystemNotificationHistoryResult(op: GetProbeSystemNotificationHistory, history: Vector[ProbeNotification])
