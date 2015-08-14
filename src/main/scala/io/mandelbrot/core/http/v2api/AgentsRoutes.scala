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

package io.mandelbrot.core.http.v2api

import akka.pattern.ask
import spray.http.HttpHeaders.Location
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._

import io.mandelbrot.core._
import io.mandelbrot.core.agent._
import io.mandelbrot.core.check._
import io.mandelbrot.core.http.json._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._

/**
 * AgentsRoutes contains all HTTP routes for interacting with agents.
 */
trait AgentsRoutes extends ApiService {
  import RoutingDirectives._
  import JsonProtocol._

  /**
   * Spray routes for managing agents
   */
  val agentsRoutes = {
    path("agents") {
      /* register new agent, or fail if it already exists */
      post {
        entity(as[AgentSpec]) { case agentRegistration: AgentSpec =>
          complete {
            serviceProxy.ask(RegisterAgent(agentRegistration.agentId, agentRegistration)).map {
              case result: RegisterAgentResult =>
                HttpResponse(StatusCodes.OK,
                  headers = List(Location("/v2/agents/" + agentRegistration.agentId.toString)),
                  entity = JsonBody(result.metadata.toJson))
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      }
    } ~
    pathPrefix("agents" / AgentIdMatcher) { case agentId: AgentId =>
      pathEndOrSingleSlash {
        /* retrieve the spec for the specified agent */
        get {
          complete {
            serviceProxy.ask(GetRegistration(agentId)).map {
              case result: GetRegistrationResult =>
                result.registration
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        } ~
        /* update the spec for the specified agent */
        put {
          entity(as[AgentSpec]) { case agentRegistration: AgentSpec =>
            complete {
              serviceProxy.ask(UpdateAgent(agentId, agentRegistration)).map {
                case result: UpdateAgentResult =>
                  HttpResponse(StatusCodes.OK,
                    headers = List(Location("/v2/agents/" + agentRegistration.agentId.toString)),
                    entity = JsonBody(result.metadata.toJson))
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }
          }
        } ~
        /* unregister the agent */
        delete {
          complete {
            serviceProxy.ask(RetireAgent(agentId)).map {
              case result: RetireAgentResult =>
                result.metadata
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      pathPrefix("checks" / CheckIdMatcher) { case checkId: CheckId =>
        pathEndOrSingleSlash {
          get {
            /* describe the status of the Check */
            complete {
              serviceProxy.ask(GetCheckStatus(CheckRef(agentId, checkId))).map {
                case result: GetCheckStatusResult =>
                  result.status
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }
          } ~
          post {
            /* update the status of the Check */
            entity(as[CheckEvaluation]) { case evaluation: CheckEvaluation =>
              complete {
                serviceProxy.ask(ProcessCheckEvaluation(CheckRef(agentId, checkId), evaluation)).map {
                  case result: ProcessCheckEvaluationResult =>
                    HttpResponse(StatusCodes.OK)
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        } ~
        path("condition") {
          get {
            timeseriesParams { timeseries =>
            pagingParams { paging =>
            complete {
              val limit = paging.limit.getOrElse(settings.pageLimit)
              serviceProxy.ask(GetCheckCondition(CheckRef(agentId, checkId), timeseries.from,
                timeseries.to, limit, timeseries.fromInclusive, timeseries.toExclusive,
                timeseries.descending, paging.last)).map {
                  case result: GetCheckConditionResult =>
                    result.page
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
            }}}
          }
        } ~
        path("notifications") {
          get {
            timeseriesParams { timeseries =>
            pagingParams { paging =>
            complete {
              val limit = paging.limit.getOrElse(settings.pageLimit)
              serviceProxy.ask(GetCheckNotifications(CheckRef(agentId, checkId), timeseries.from,
                timeseries.to, limit,
                timeseries.fromInclusive, timeseries.toExclusive, timeseries.descending,
                paging.last)).map {
                  case result: GetCheckNotificationsResult =>
                    result.page
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
            }}}
          }
        } ~
        path("metrics") {
          get {
            timeseriesParams { timeseries =>
            pagingParams { paging =>
            complete {
              val limit = paging.limit.getOrElse(settings.pageLimit)
              serviceProxy.ask(GetCheckMetrics(CheckRef(agentId, checkId), timeseries.from,
                timeseries.to, limit, timeseries.fromInclusive, timeseries.toExclusive,
                timeseries.descending, paging.last)).map {
                  case result: GetCheckMetricsResult =>
                    result.page
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
            }}}
          }
        } ~
        path("acknowledge") {
          /* acknowledge an unhealthy check */
          post {
            entity(as[AcknowledgeCheck]) { case command: AcknowledgeCheck =>
            complete {
              serviceProxy.ask(command).map {
                case result: AcknowledgeCheckResult =>
                  result.condition
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        } ~
        path("unacknowledge") {
          /* acknowledge an unhealthy check */
          post {
            entity(as[UnacknowledgeCheck]) { case command: UnacknowledgeCheck =>
            complete {
              serviceProxy.ask(command).map {
                case result: UnacknowledgeCheckResult =>
                  result.condition
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        } ~
        path("squelch") {
          /* enable/disable check notifications */
          post {
            entity(as[SetCheckSquelch]) { case command: SetCheckSquelch =>
            complete {
              serviceProxy.ask(command).map {
                case result: SetCheckSquelchResult =>
                  result.condition
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        }
      }
    }
  }
}
