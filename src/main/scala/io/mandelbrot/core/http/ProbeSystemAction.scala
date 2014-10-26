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

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.util.Timeout
import io.mandelbrot.core.state.{GetCurrentStatusResult, GetCurrentStatus}
import io.mandelbrot.core.{RetryLater, ApiException}
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport._
import org.joda.time.DateTime
import java.net.URI
import java.util.UUID

import io.mandelbrot.core.registry._
import io.mandelbrot.core.system._
import io.mandelbrot.core.notification.ProbeNotification
import JsonProtocol._

case object ActionTimeout

sealed trait HttpOperation
case class HttpActionParams(ctx: RequestContext, timeout: Timeout, services: ActorRef)

case class GetProbeSystemStatus(uri: URI, paths: Option[Set[String]]) extends HttpOperation
case class GetProbeSystemStatusResult(op: GetProbeSystemStatus, status: Map[ProbeRef,ProbeStatus])

class GetProbeSystemStatusAction(params: HttpActionParams, op: GetProbeSystemStatus) extends Actor with ActorLogging {
  import context.dispatcher

  val actionTimeout = context.system.scheduler.scheduleOnce(params.timeout.duration, self, ActionTimeout)
  val parser = new ProbeMatcherParser()
  val matchers: Set[ProbeMatcher] = op.paths match {
    case None => Set.empty
    case Some(paths) => paths.map(path => parser.parseProbeMatcher(op.uri.toString + path))
  }
  params.services ! MatchProbeSystem(op.uri, matchers)

  def receive = {
    case result: MatchProbeSystemResult =>
      params.services ! GetCurrentStatus(Right(result.refs))

    case result: GetCurrentStatusResult =>
      actionTimeout.cancel()
      val status = result.status.map(s => s.probeRef -> s).toMap
      params.ctx.complete(GetProbeSystemStatusResult(GetProbeSystemStatus(op.uri, op.paths), status))
      context.stop(self)

    case ActionTimeout =>
      params.ctx.complete(new ApiException(RetryLater))
      context.stop(self)
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
//}


sealed trait ProbeSystemOperation { val uri: URI }
sealed trait ProbeSystemCommand extends ProbeSystemOperation
sealed trait ProbeSystemQuery extends ProbeSystemOperation
case class ProbeSystemOperationFailed(op: ProbeSystemOperation, failure: Throwable)

case class AcknowledgeProbeSystem(uri: URI, correlations: Map[ProbeRef,UUID]) extends ProbeSystemCommand
case class AcknowledgeProbeSystemResult(op: AcknowledgeProbeSystem, acknowledgements: Map[ProbeRef,UUID])

case class UnacknowledgeProbeSystem(uri: URI, unacknowledgements: Map[ProbeRef,UUID]) extends ProbeSystemCommand
case class UnacknowledgeProbeSystemResult(op: UnacknowledgeProbeSystem, unacknowledgements: Map[ProbeRef,UUID])

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
