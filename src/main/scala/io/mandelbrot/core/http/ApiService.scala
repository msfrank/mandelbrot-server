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

import akka.actor.{ActorSystem, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import spray.routing.{HttpService, ExceptionHandler}
import spray.http._
import spray.http.HttpHeaders.Location
import spray.util.LoggingContext
import org.joda.time.format.ISODateTimeFormat
import java.net.URI
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.message._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.history._
import io.mandelbrot.core.notification._

/**
 * ApiService contains the REST API logic.
 */
trait ApiService extends HttpService {
  import scala.language.postfixOps
  import spray.httpx.SprayJsonSupport._
  import spray.json._
  import JsonProtocol._
  import RoutingDirectives._

  val settings: HttpSettings

  implicit def log: LoggingAdapter
  implicit val system: ActorSystem
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout

  val registryService: ActorRef
  val stateService: ActorRef
  val historyService: ActorRef
  val notificationService: ActorRef
  val messageStream: MessageStreamBus

  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  /**
   * Spray routes for managing objects
   */
  val objectsSystemsRoutes = {
    path("systems") {
      /* register new probe system, or fail if it already exists */
      post {
        entity(as[RegisterProbeSystem]) { case registerProbeSystem: RegisterProbeSystem =>
          complete {
            registryService.ask(registerProbeSystem).map {
              case result: RegisterProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted, headers = List(Location("/objects/systems/" + registerProbeSystem.uri.toString)))
              case failure: ProbeRegistryOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* enumerate all registered probe systems */
      get {
        complete {
          registryService.ask(ListProbeSystems()).map {
            case result: ListProbeSystemsResult =>
              result.systems
            case failure: ProbeRegistryOperationFailed =>
              throw failure.failure
          }
        }
      }
    } ~
    pathPrefix("systems" / Uri) { case uri: URI =>
      pathEndOrSingleSlash {
        /* retrieve the spec for the specified probe system */
        get {
          complete {
            registryService.ask(DescribeProbeSystem(uri)).map {
              case result: DescribeProbeSystemResult =>
                result.registration
              case failure: ProbeSystemOperationFailed =>
                throw failure.failure
            }
          }
        } ~
        /* update the spec for the specified probe system */
        put {
          entity(as[UpdateProbeSystem]) { case updateProbeSystem: UpdateProbeSystem =>
            complete {
              registryService.ask(updateProbeSystem).map {
                case result: UpdateProbeSystemResult =>
                  HttpResponse(StatusCodes.Accepted, headers = List(Location("/objects/systems/" + updateProbeSystem.uri.toString)))
                case failure: ProbeSystemOperationFailed =>
                  throw failure.failure
              }
            }
          }
        } ~
        /* unregister the probe system */
        delete {
          complete {
            registryService.ask(UnregisterProbeSystem(uri)).map {
              case result: UnregisterProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted)
              case failure: ProbeSystemOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      pathPrefix("properties") {
        path("status") {
          /* describe the status of the ProbeSystem */
          get {
            pathParams { paths =>
            complete {
              registryService.ask(GetProbeSystemStatus(uri, paths)).map {
                case result: GetProbeSystemStatusResult =>
                  result.state
                case failure: ProbeSystemOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        } ~
        path("metadata") {
          /* return metadata attached to the ProbeSystem */
          get {
            pathParams { paths =>
            complete {
              registryService.ask(GetProbeSystemMetadata(uri, paths)).map {
                case result: GetProbeSystemMetadataResult =>
                  result.metadata
                case failure: ProbeSystemOperationFailed =>
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
              registryService.ask(GetProbeSystemPolicy(uri, paths)).map {
                case result: GetProbeSystemPolicyResult =>
                  result.policy
                case failure: ProbeSystemOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        }
      } ~
      pathPrefix("collections") {
        path("history") {
          get {
            timeseriesParams { params =>
            pathParams { paths =>
              complete {
                registryService.ask(GetProbeSystemStatusHistory(uri, paths, params)).map {
                  case result: GetProbeSystemStatusHistoryResult =>
                    result.history
                  case failure: ProbeSystemOperationFailed =>
                    throw failure.failure
                }
              }
            }}
          }
        } ~
        path("notifications") {
          get {
            timeseriesParams { params =>
            pathParams { paths =>
              complete {
                registryService.ask(GetProbeSystemNotificationHistory(uri, paths, params)).map {
                  case result: GetProbeSystemNotificationHistoryResult =>
                    result.history
                  case failure: ProbeSystemOperationFailed =>
                    throw failure.failure
                }
              }
            }}
          }
        } ~
        path("metrics") { get { complete { StatusCodes.BadRequest }}
        } ~
        path("events") { get { complete { StatusCodes.BadRequest }}
        } ~
        path("snapshots") { get { complete { StatusCodes.BadRequest }}
        }
      } ~
      pathPrefix("actions") {
        path("submit") {
          /* publish message to the message stream */
          post {
            // FIXME: reject message if proberef doesn't match
            entity(as[Message]) { case message: Message =>
              complete {
                messageStream.publish(message)
                HttpResponse(StatusCodes.OK)
              }
            }
          }
        } ~
        path("acknowledge") {
          /* acknowledge an unhealthy probe */
          post {
            entity(as[AcknowledgeProbeSystem]) { case command: AcknowledgeProbeSystem =>
              complete {
                registryService.ask(command).map {
                  case result: AcknowledgeProbeSystemResult =>
                    result.acknowledgements
                  case failure: ProbeSystemOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        } ~
        path("comment") {
          /* add worknote to acknowledged probe */
          post {
            entity(as[AppendProbeWorknote]) { case command: AppendProbeWorknote =>
              complete {
                registryService.ask(command).map {
                  case result: AppendProbeWorknoteResult =>
                    result.worknoteId
                  case failure: ProbeOperationFailed =>
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
                registryService.ask(command).map {
                  case result: UnacknowledgeProbeSystemResult =>
                    result.unacknowledgements
                  case failure: ProbeSystemOperationFailed =>
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
                registryService.ask(command).map {
                  case result: SetProbeSquelchResult =>
                    result
                  case failure: ProbeOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        } ~
        path("invoke") {
          /* execute an external command */
          post { complete { throw new ApiException(BadRequest)}}
        }
      }
    }
  }

  val objectsWindowsRoutes = {
    path("windows") {
      /* register new maintenance window */
      post {
        entity(as[RegisterMaintenanceWindow]) { case registerMaintenanceWindow: RegisterMaintenanceWindow =>
          complete {
            notificationService.ask(registerMaintenanceWindow).map {
              case result: RegisterMaintenanceWindowResult =>
                HttpResponse(StatusCodes.Accepted, headers = List(Location("/objects/windows/" + result.id.toString)))
              case failure: NotificationManagerOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* enumerate all maintenance windows */
      get {
        complete {
          notificationService.ask(ListMaintenanceWindows()).map {
            case result: ListMaintenanceWindowsResult =>
              result.windows
            case failure: NotificationManagerOperationFailed =>
              throw failure.failure
          }
        }
      }
    } ~
    pathPrefix("windows" / JavaUUID) { case uuid: UUID =>
      /* unregister new maintenance window */
      delete {
        complete {
          notificationService.ask(UnregisterMaintenanceWindow(uuid)).map {
            case result: UnregisterMaintenanceWindowResult =>
              HttpResponse(StatusCodes.OK)
            case failure: NotificationManagerOperationFailed =>
              throw failure.failure
          }
        }
      }
    }
  }

  val objectsRoutes = pathPrefix("objects") {
    objectsSystemsRoutes ~ objectsWindowsRoutes
  }

  /**
   * Spray routes for invoking services
   */
  val servicesRoutes = pathPrefix("services") {
    pathPrefix("state") {
      path("search") {
        get {
          parameters('q.as[String], 'limit.as[Int].?) { case (q: String, limit: Option[Int]) =>
            complete {
              stateService.ask(QueryProbes(q, limit)).map {
                case result: QueryprobesResult =>
                  result.refs
              }
            }
          }
        }
      }
    }
  }

  val version1 = objectsRoutes ~ servicesRoutes

  val routes =  version1

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
        case _ =>
          ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(ex))))
      }
    case ex: Throwable => ctx =>
      log.error(ex, "caught exception in spray routing: %s".format(ex.getMessage))
      ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(new Exception("internal server error")))))
  }

  def throwableToJson(t: Throwable): JsValue = JsObject(Map("description" -> JsString(t.getMessage)))
}
