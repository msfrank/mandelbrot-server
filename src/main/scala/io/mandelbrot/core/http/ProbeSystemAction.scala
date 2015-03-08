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

import java.util.UUID

import akka.actor._
import akka.util.Timeout
import io.mandelbrot.core.registry.ProbePolicy
import spray.routing.{Route, RequestContext}
import spray.httpx.SprayJsonSupport._
import org.joda.time.DateTime
import java.net.URI

import io.mandelbrot.core.system._
import io.mandelbrot.core.state._
import io.mandelbrot.core.notification._
import io.mandelbrot.core.history._
import io.mandelbrot.core.{ServiceOperationFailed, RetryLater, ApiException}
import JsonProtocol._

/**
 *
 */
class GetProbeSystemStatusAction(params: HttpActionParams, op: GetProbeSystemStatus) extends Actor with ActorLogging {
  import context.dispatcher

  val actionTimeout = context.system.scheduler.scheduleOnce(params.timeout.duration, self, ActionTimeout)
  val parser = new ProbeMatcherParser()
  val matchers: Set[ProbeMatcher] = op.paths match {
    case None => Set.empty
    case Some(paths) => paths.map(path => parser.parseProbeMatcher(op.uri.toString + path))
  }

  var refs = Set.empty[ProbeRef]
  var results = Map.empty[ProbeRef,ProbeStatus]

  params.services ! MatchProbeSystem(op.uri, matchers)

  def receive = {
    case result: MatchProbeSystemResult =>
      log.debug("matched probe refs {}", result.refs.mkString(","))
      refs = result.refs
      refs.foreach(ref => params.services ! GetProbeStatus(ref))

    case result: GetProbeStatusResult =>
      log.debug("got status for {}", result.op.probeRef)
      refs = refs - result.op.probeRef
      results = results + (result.op.probeRef -> result.status)
      if (refs.isEmpty) {
        actionTimeout.cancel()
        params.ctx.complete(GetProbeSystemStatusResult(op, results))
        context.stop(self)
      }

    case failure: ServiceOperationFailed =>
      params.ctx.complete(failure.failure)
      context.stop(self)

    case ActionTimeout =>
      params.ctx.complete(ApiException(RetryLater))
      context.stop(self)
  }

  override def postStop(): Unit = actionTimeout.cancel()
}

/**
 *
 */
class GetProbeSystemConditionHistoryAction(params: HttpActionParams, op: GetProbeSystemConditionHistory) extends Actor with ActorLogging {
  import context.dispatcher

  val actionTimeout = context.system.scheduler.scheduleOnce(params.timeout.duration, self, ActionTimeout)
  val parser = new ProbeMatcherParser()
  val matchers: Set[ProbeMatcher] = op.paths match {
    case None => Set.empty
    case Some(paths) => paths.map(path => parser.parseProbeMatcher(op.uri.toString + path))
  }

  var refs = Set.empty[ProbeRef]
  var results = Map.empty[ProbeRef,GetConditionHistoryResult]

  params.services ! MatchProbeSystem(op.uri, matchers)

  def receive = {
    case result: MatchProbeSystemResult =>
      log.debug("matched probe refs {}", result.refs.mkString(","))

    case failure: ServiceOperationFailed =>
      params.ctx.complete(failure.failure)
      context.stop(self)

    case ActionTimeout =>
      params.ctx.complete(ApiException(RetryLater))
      context.stop(self)
  }

  override def postStop(): Unit = actionTimeout.cancel()
}

/**
 *
 */
class GetProbeSystemNotificationHistoryAction(params: HttpActionParams, op: GetProbeSystemNotificationHistory) extends Actor with ActorLogging {
  import context.dispatcher

  val actionTimeout = context.system.scheduler.scheduleOnce(params.timeout.duration, self, ActionTimeout)
  val parser = new ProbeMatcherParser()
  val matchers: Set[ProbeMatcher] = op.paths match {
    case None => Set.empty
    case Some(paths) => paths.map(path => parser.parseProbeMatcher(op.uri.toString + path))
  }

  var refs = Set.empty[ProbeRef]
  var results = Map.empty[ProbeRef,GetNotificationHistoryResult]

  params.services ! MatchProbeSystem(op.uri, matchers)

  def receive = {
    case result: MatchProbeSystemResult =>
      log.debug("matched probe refs {}", result.refs.mkString(","))

    case failure: ServiceOperationFailed =>
      params.ctx.complete(failure.failure)
      context.stop(self)

    case ActionTimeout =>
      params.ctx.complete(ApiException(RetryLater))
      context.stop(self)
  }

  override def postStop(): Unit = actionTimeout.cancel()
}

case class HttpActionParams(ctx: RequestContext, timeout: Timeout, services: ActorRef)
case object ActionTimeout

sealed trait HttpOperation

case class GetProbeSystemStatus(uri: URI, paths: Option[Set[String]]) extends HttpOperation
case class GetProbeSystemStatusResult(op: GetProbeSystemStatus, status: Map[ProbeRef,ProbeStatus])

case class GetProbeSystemMetadata(uri: URI, paths: Option[Set[String]]) extends HttpOperation
case class GetProbeSystemMetadataResult(op: GetProbeSystemMetadata, metadata: Map[ProbeRef,Map[String,String]])

case class GetProbeSystemPolicy(uri: URI, paths: Option[Set[String]]) extends HttpOperation
case class GetProbeSystemPolicyResult(op: GetProbeSystemPolicy, policy: Map[ProbeRef,ProbePolicy])

case class GetProbeSystemConditionHistory(uri: URI, paths: Option[Set[String]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HttpOperation
case class GetProbeSystemConditionHistoryResult(op: GetProbeSystemConditionHistory, history: Vector[ProbeCondition])

case class GetProbeSystemNotificationHistory(uri: URI, paths: Option[Set[String]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HttpOperation
case class GetProbeSystemNotificationHistoryResult(op: GetProbeSystemNotificationHistory, history: Vector[ProbeNotification])

case class AcknowledgeProbeSystem(uri: URI, correlations: Map[ProbeRef,UUID]) extends HttpOperation
case class AcknowledgeProbeSystemResult(op: AcknowledgeProbeSystem, acknowledgements: Map[ProbeRef,UUID])

case class UnacknowledgeProbeSystem(uri: URI, unacknowledgements: Map[ProbeRef,UUID]) extends HttpOperation
case class UnacknowledgeProbeSystemResult(op: UnacknowledgeProbeSystem, unacknowledgements: Map[ProbeRef,UUID])

trait HttpActionPropsCreator[T] {
  def props(params: HttpActionParams, op: T): Props
}

object HttpActionProps {

  def completeAction[T](op: T)(implicit ev: HttpActionPropsCreator[T], actorRefFactory: ActorRefFactory, timeout: Timeout, serviceProxy: ActorRef) = Route { ctx =>
    val props = ev.props(HttpActionParams(ctx, timeout, serviceProxy), op)
    actorRefFactory.actorOf(props)
  }

  implicit object GetProbeSystemStatusPropsCreator extends HttpActionPropsCreator[GetProbeSystemStatus] {
    def props(params: HttpActionParams, op: GetProbeSystemStatus) = Props(classOf[GetProbeSystemStatusAction], params, op)
  }
  implicit object GetProbeSystemConditionHistoryPropsCreator extends HttpActionPropsCreator[GetProbeSystemConditionHistory] {
    def props(params: HttpActionParams, op: GetProbeSystemConditionHistory) = Props(classOf[GetProbeSystemConditionHistoryAction], params, op)
  }
  implicit object GetProbeSystemNotificationHistoryPropsCreator extends HttpActionPropsCreator[GetProbeSystemNotificationHistory] {
    def props(params: HttpActionParams, op: GetProbeSystemNotificationHistory) = Props(classOf[GetProbeSystemNotificationHistoryAction], params, op)
  }
}

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


//    /* get the metadata of probes in the system */
//    case query: GetProbeSystemMetadata =>
//      val metadata = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
//        ref -> findProbeSpec(registration, ref.path).metadata
//      }.toMap
//      if (metadata.isEmpty)
//        sender() ! ProbeSystemOperationFailed(query, ApiException(ResourceNotFound))
//      else
//        sender() ! GetProbeSystemMetadataResult(query, metadata)


//    /* get the policy of probes in the system */
//    case query: GetProbeSystemPolicy =>
//      val policy = findMatching(query.paths).map { case (ref: ProbeRef, actor: ProbeActor) =>
//        ref -> findProbeSpec(registration, ref.path).policy
//      }.toMap
//      if (policy.isEmpty)
//        sender() ! ProbeSystemOperationFailed(query, ApiException(ResourceNotFound))
//      else
//        sender() ! GetProbeSystemPolicyResult(query, policy)
