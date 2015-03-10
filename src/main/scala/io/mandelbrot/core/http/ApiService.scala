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
import spray.routing.{HttpService, ExceptionHandler}
import spray.http._
import spray.http.HttpHeaders.Location
import spray.util.LoggingContext
import org.joda.time.format.ISODateTimeFormat
import scala.concurrent.ExecutionContext
import java.net.URI
import java.util.UUID
import java.io.{PrintStream, ByteArrayOutputStream}

import io.mandelbrot.core._
import io.mandelbrot.core.entity._
import io.mandelbrot.core.system._
import io.mandelbrot.core.registry._

/**
 * ApiService contains the REST API logic.
 */
trait ApiService extends HttpService {
  import scala.language.postfixOps
  import spray.httpx.SprayJsonSupport._
  import spray.json._
  import JsonProtocol._
  import RoutingDirectives._
  import HttpActionProps._

  val settings: HttpSettings

  implicit def log: LoggingAdapter
  implicit val system: ActorSystem
  implicit def executionContext: ExecutionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout
  implicit val serviceProxy: ActorRef

  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  /**
   * Spray routes for managing objects
   */
  val systemsRoutes = {
    path("systems") {
      /* register new probe system, or fail if it already exists */
      post {
        entity(as[RegisterProbeSystem]) { case registerProbeSystem: RegisterProbeSystem =>
          complete {
            serviceProxy.ask(registerProbeSystem).map {
              case result: RegisterProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted,
                             headers = List(Location("/objects/systems/" + registerProbeSystem.uri.toString)),
                             entity = JsonBody(result.op.uri.toJson))
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
            val limit = paging.limit.getOrElse(100)
            val token = paging.last.map(new URI(_))
            serviceProxy.ask(ListProbeSystems(limit, token)).map {
              case result: ListProbeSystemsResult =>
                result.systems
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      }
    } ~
    pathPrefix("systems" / SystemUri) { case uri: URI =>
      pathEndOrSingleSlash {
        /* retrieve the spec for the specified probe system */
        get {
          complete {
            serviceProxy.ask(GetProbeSystemEntry(uri)).map {
              case result: GetProbeSystemEntryResult =>
                result.registration
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        } ~
        /* update the spec for the specified probe system */
        put {
          entity(as[UpdateProbeSystem]) { case updateProbeSystem: UpdateProbeSystem =>
            complete {
              serviceProxy.ask(updateProbeSystem).map {
                case result: UpdateProbeSystemResult =>
                  HttpResponse(StatusCodes.Accepted,
                               headers = List(Location("/objects/systems/" + updateProbeSystem.uri.toString)),
                               entity = JsonBody(result.op.uri.toJson))
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }
          }
        } ~
        /* unregister the probe system */
        delete {
          complete {
            serviceProxy.ask(RetireProbeSystem(uri)).map {
              case result: RetireProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted)
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      pathPrefix("probes" / ProbePath) { case path: Vector[String] =>
        get {
          /* describe the status of the Probe */
          complete {
            serviceProxy.ask(GetProbeStatus(ProbeRef(uri, path))).map {
              case result: GetProbeStatusResult =>
                result.status
              case failure: ServiceOperationFailed =>
                throw failure.failure
            }
          }
        } ~
        post {
          /* update the status of the Probe */
          entity(as[ProbeEvaluation]) { case evaluation: ProbeEvaluation =>
            complete {
              serviceProxy.ask(ProcessProbeEvaluation(ProbeRef(uri, path), evaluation)).map {
                case result: ProcessProbeEvaluationResult =>
                  HttpResponse(StatusCodes.OK)
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }
          }
        }
      } ~
      path("properties") {
        path("metadata") {
          /* return metadata attached to the ProbeSystem */
          get {
            pathParams { paths =>
            complete {
              serviceProxy.ask(GetProbeSystemMetadata(uri, paths)).map {
                case result: GetProbeSystemMetadataResult =>
                  result.metadata
                case failure: ServiceOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        } ~
        path("policy") {
          /* return the current policy */
          get {
            pathParams { paths =>
              complete {
                serviceProxy.ask(GetProbeSystemPolicy(uri, paths)).map {
                  case result: GetProbeSystemPolicyResult =>
                    result.policy
                  case failure: ServiceOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        }
      } ~
      path("collections") {
        path("conditions") {
          get {
            pathParams { paths =>
            timeseriesParams { timeseries =>
            pagingParams { paging =>
              completeAction(GetProbeSystemConditionHistory(uri, paths, timeseries.from, timeseries.to, paging.limit))
            }}}
          }
        } ~
        path("notifications") {
          get {
            pathParams { paths =>
            timeseriesParams { timeseries =>
            pagingParams { paging =>
              completeAction(GetProbeSystemNotificationHistory(uri, paths, timeseries.from, timeseries.to, paging.limit))
            }}}
          }
        } ~
        path("metrics") {
          get {
            complete {
              StatusCodes.NotImplemented
            }
          }
        }
      } ~
      pathPrefix("actions") {
        path("acknowledge") {
          /* acknowledge an unhealthy probe */
          post {
            entity(as[AcknowledgeProbeSystem]) { case command: AcknowledgeProbeSystem =>
              complete {
                serviceProxy.ask(command).map {
                  case result: AcknowledgeProbeSystemResult =>
                    result.acknowledgements
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
            entity(as[UnacknowledgeProbeSystem]) { case command: UnacknowledgeProbeSystem =>
              complete {
                serviceProxy.ask(command).map {
                  case result: UnacknowledgeProbeSystemResult =>
                    result.unacknowledgements
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
            entity(as[SetProbeSquelch]) { case command: SetProbeSquelch =>
              complete {
                serviceProxy.ask(command).map {
                  case result: SetProbeSquelchResult =>
                    result
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
