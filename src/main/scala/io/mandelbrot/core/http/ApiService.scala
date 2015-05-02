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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import io.mandelbrot.core.http.json.JsonBody
import spray.routing.{HttpService, ExceptionHandler}
import spray.http._
import spray.http.HttpHeaders.Location
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.util.LoggingContext
import org.joda.time.format.ISODateTimeFormat
import scala.concurrent.ExecutionContext
import java.net.URI
import java.io.{PrintStream, ByteArrayOutputStream}

import io.mandelbrot.core._
import io.mandelbrot.core.entity._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.system._

/**
 * ApiService contains the REST API logic.
 */
trait ApiService extends HttpService {
  import scala.language.postfixOps
  import json.JsonProtocol._
  import RoutingDirectives._

  val settings: HttpSettings

  implicit def log: LoggingAdapter
  implicit val system: ActorSystem
  implicit def executionContext: ExecutionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout
  def serviceProxy: ActorRef

  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  /**
   * Spray routes for managing systems
   */
  val systemsRoutes = {
    path("systems") {
      /* register new probe system, or fail if it already exists */
      post {
        entity(as[AgentRegistration]) { case agentRegistration: AgentRegistration =>
          complete {
            serviceProxy.ask(RegisterAgent(agentRegistration.agentId, agentRegistration)).map {
              case result: RegisterProbeSystemResult =>
                HttpResponse(StatusCodes.OK,
                             headers = List(Location("/v2/systems/" + agentRegistration.agentId.toString)),
                             entity = JsonBody(result.metadata.toJson))
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* enumerate all registered probe systems */
      get {
        pagingParams { paging =>
          complete {
            val limit = paging.limit.getOrElse(settings.pageLimit)
            serviceProxy.ask(ListRegistrations(limit, paging.last)).map {
              case result: ListRegistrationsResult =>
                result.page
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      }
    } ~
    pathPrefix("systems" / AgentIdMatcher) { case agentId: AgentId =>
      pathEndOrSingleSlash {
        /* retrieve the spec for the specified probe system */
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
        /* update the spec for the specified probe system */
        put {
          entity(as[AgentRegistration]) { case agentRegistration: AgentRegistration =>
            complete {
              serviceProxy.ask(UpdateAgent(agentId, agentRegistration)).map {
                case result: UpdateProbeSystemResult =>
                  HttpResponse(StatusCodes.OK,
                               headers = List(Location("/v2/systems/" + agentRegistration.agentId.toString)),
                               entity = JsonBody(result.metadata.toJson))
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }
          }
        } ~
        /* unregister the probe system */
        delete {
          complete {
            serviceProxy.ask(RetireAgent(agentId)).map {
              case result: RetireProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted)
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
              serviceProxy.ask(GetCheckStatus(ProbeRef(agentId, checkId))).map {
                case result: GetProbeStatusResult =>
                  result.status
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }
          } ~
          post {
            /* update the status of the Check */
            entity(as[ProbeEvaluation]) { case evaluation: ProbeEvaluation =>
              complete {
                serviceProxy.ask(ProcessCheckEvaluation(ProbeRef(agentId, checkId), evaluation)).map {
                  case result: ProcessProbeEvaluationResult =>
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
                serviceProxy.ask(GetCheckCondition(ProbeRef(agentId, checkId), timeseries.from, timeseries.to, limit, paging.last)).map {
                  case result: GetProbeConditionResult =>
                    result.page
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }}
          }
        } ~
        path("notifications") {
          get {
            timeseriesParams { timeseries =>
            pagingParams { paging =>
              complete {
                val limit = paging.limit.getOrElse(settings.pageLimit)
                serviceProxy.ask(GetCheckNotifications(ProbeRef(agentId, checkId), timeseries.from, timeseries.to, limit, paging.last)).map {
                  case result: GetProbeNotificationsResult =>
                    result.page
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }}
          }
        } ~
        path("metrics") {
          get {
            timeseriesParams { timeseries =>
            pagingParams { paging =>
              complete {
                val limit = paging.limit.getOrElse(settings.pageLimit)
                serviceProxy.ask(GetCheckMetrics(ProbeRef(agentId, checkId), timeseries.from, timeseries.to, limit, paging.last)).map {
                  case result: GetProbeMetricsResult =>
                    result.page
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }}
          }
        } ~
        path("acknowledge") {
          /* acknowledge an unhealthy probe */
          post {
            entity(as[AcknowledgeCheck]) { case command: AcknowledgeCheck =>
              complete {
                serviceProxy.ask(command).map {
                  case result: AcknowledgeProbeResult =>
                    result.condition
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        } ~
        path("unacknowledge") {
          /* acknowledge an unhealthy probe */
          post {
            entity(as[UnacknowledgeCheck]) { case command: UnacknowledgeCheck =>
              complete {
                serviceProxy.ask(command).map {
                  case result: UnacknowledgeProbeResult =>
                    result.condition
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        } ~
        path("squelch") {
          /* enable/disable probe notifications */
          post {
            entity(as[SetCheckSquelch]) { case command: SetCheckSquelch =>
              complete {
                serviceProxy.ask(command).map {
                  case result: SetProbeSquelchResult =>
                    result.condition
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        }
      }
    }
  }

  val shardsRoutes = {
    path("shards") {
      /* get the status of all shards in the cluster */
      get {
        complete {
          serviceProxy.ask(GetShardMapStatus()).map {
            case result: GetShardMapStatusResult =>
              result.status
            case failure: ServiceOperationFailed =>
              throw failure.failure
          }
        }
      }
    }
  }

  val nodesRoutes = {
    path("nodes") {
      /* get the status of all nodes in the cluster */
      get {
        complete {
          serviceProxy.ask(GetClusterStatus()).map {
            case result: GetClusterStatusResult =>
              result.status
            case failure: ServiceOperationFailed =>
              throw failure.failure
          }
        }
      }
    } ~
    pathPrefix("nodes" / ClusterAddress) { case address =>
      /* get the status of the specified node */
      get {
        complete {
          serviceProxy.ask(GetNodeStatus(Some(address))).map {
            case result: GetNodeStatusResult =>
              result.status
            case failure: ServiceOperationFailed =>
              throw failure.failure
          }
        }
      }
    }
  }

  val version2 = pathPrefix("v2") {
    systemsRoutes ~ shardsRoutes ~ nodesRoutes
  }

  val routes = version2

  import scala.language.implicitConversions

  /**
   * catch thrown exceptions and convert them to HTTP responses.  we bake in support
   * for catching APIFailure objects wrapped in an APIException, otherwise any other
   * Throwable results in a generic 500 Internal Server Error.
   */
  implicit def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case ex: ApiException => ctx =>
      ex.failure match {
        case failure: RetryLater =>
          ctx.complete(HttpResponse(StatusCodes.ServiceUnavailable, JsonBody(throwableToJson(ex))))
        case failure: BadRequest =>
          ctx.complete(HttpResponse(StatusCodes.BadRequest, JsonBody(throwableToJson(ex))))
        case failure: ResourceNotFound =>
          ctx.complete(HttpResponse(StatusCodes.NotFound, JsonBody(throwableToJson(ex))))
        case failure: Conflict =>
          ctx.complete(HttpResponse(StatusCodes.Conflict, JsonBody(throwableToJson(ex))))
        case failure: NotImplemented =>
          ctx.complete(HttpResponse(StatusCodes.NotImplemented, JsonBody(throwableToJson(ex))))
        case _ =>
          ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(ex))))
      }
    case t: Throwable => ctx =>
      val ex = ApiException(InternalError, t)
      ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(ex))))
  }

  def throwableToJson(t: ApiException): JsValue = {
    if (settings.debugExceptions) {
      val os = new ByteArrayOutputStream()
      val ps = new PrintStream(os)
      t.printStackTrace(ps)
      val stackTrace = os.toString
      ps.close()
      JsObject(Map("description" -> JsString(t.getMessage), "stackTrace" -> JsString(stackTrace)))
    } else JsObject(Map("description" -> JsString(t.getMessage)))
  }
}
