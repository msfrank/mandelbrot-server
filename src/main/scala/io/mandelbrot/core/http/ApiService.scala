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
import scala.util.{Failure, Success}
import java.net.URI
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.system._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.history._
import io.mandelbrot.core.notification._
import io.mandelbrot.core.tracking._

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

  val serviceProxy: ActorRef

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
            serviceProxy.ask(registerProbeSystem).map {
              case result: RegisterProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted,
                             headers = List(Location("/objects/systems/" + registerProbeSystem.uri.toString)),
                             entity = JsonBody(result.op.uri.toJson))
              case failure: RegistryServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* enumerate all registered probe systems */
      get {
        pagingParams { paging =>
          complete {
            serviceProxy.ask(ListProbeSystems(paging.last, paging.limit)).map {
              case result: ListProbeSystemsResult =>
                result.systems
              case failure: RegistryServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      }
    } ~
    pathPrefix("systems" / Uri) { case uri: URI =>
      pathEndOrSingleSlash {
        /* retrieve the spec for the specified probe system */
        get {
          complete {
            serviceProxy.ask(DescribeProbeSystem(uri)).map {
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
              serviceProxy.ask(updateProbeSystem).map {
                case result: UpdateProbeSystemResult =>
                  HttpResponse(StatusCodes.Accepted,
                               headers = List(Location("/objects/systems/" + updateProbeSystem.uri.toString)),
                               entity = JsonBody(result.op.uri.toJson))
                case failure: RegistryServiceOperationFailed =>
                  throw failure.failure
              }
            }
          }
        } ~
        /* unregister the probe system */
        delete {
          complete {
            serviceProxy.ask(UnregisterProbeSystem(uri)).map {
              case result: UnregisterProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted)
              case failure: RegistryServiceOperationFailed =>
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
              serviceProxy.ask(GetProbeSystemStatus(uri, paths)).map {
                case result: GetProbeSystemStatusResult =>
                  result.status
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
              serviceProxy.ask(GetProbeSystemMetadata(uri, paths)).map {
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
              serviceProxy.ask(GetProbeSystemPolicy(uri, paths)).map {
                case result: GetProbeSystemPolicyResult =>
                  result.policy
                case failure: ProbeSystemOperationFailed =>
                  throw failure.failure
              }
            }}
          }
        } ~
        path("links") {
          /* return links attached to the ProbeSystem */
          get {
            pathParams { paths =>
            complete {
              serviceProxy.ask(GetProbeSystemLinks(uri, paths)).map {
                case result: GetProbeSystemLinksResult =>
                  result.links
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
            pathParams { paths =>
            timeseriesParams { timeseries =>
            pagingParams { paging =>
              complete {
                serviceProxy.ask(GetProbeSystemStatusHistory(uri, paths, timeseries.from, timeseries.to, paging.limit)).map {
                  case result: GetProbeSystemStatusHistoryResult =>
                    result.history
                  case failure: ProbeSystemOperationFailed =>
                    throw failure.failure
                }
              }
            }}}
          }
        } ~
        path("notifications") {
          get {
            pathParams { paths =>
            timeseriesParams { timeseries =>
            pagingParams { paging =>
              complete {
                serviceProxy.ask(GetProbeSystemNotificationHistory(uri, paths, timeseries.from, timeseries.to, paging.limit)).map {
                  case result: GetProbeSystemNotificationHistoryResult =>
                    result.history
                  case failure: ProbeSystemOperationFailed =>
                    throw failure.failure
                }
              }
            }}}
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
            entity(as[ProbeEvent]) {
              case message: ProbeEvent =>
                complete {
                  serviceProxy ! message
                  HttpResponse(StatusCodes.OK)
                }
              case other =>
                throw new ApiException(BadRequest)
            }
          }
        } ~
        path("acknowledge") {
          /* acknowledge an unhealthy probe */
          post {
            entity(as[AcknowledgeProbeSystem]) { case command: AcknowledgeProbeSystem =>
              complete {
                serviceProxy.ask(command).map {
                  case result: AcknowledgeProbeSystemResult =>
                    result.acknowledgements
                  case failure: ProbeSystemOperationFailed =>
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
                serviceProxy.ask(command).map {
                  case result: SetProbeSquelchResult =>
                    result
                  case failure: ProbeOperationFailed =>
                    throw failure.failure
                }
              }
            }
          }
        } ~
        path("link") {
          /* register a probe link */
          post {
            complete { StatusCodes.BadRequest }
          }
        } ~
        path("relink") {
          /* update a probe link */
          post {
            complete { StatusCodes.BadRequest }
          }
        } ~
        path("unlink") {
          /* unregister a probe link */
          post {
            complete { StatusCodes.BadRequest }
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
            serviceProxy.ask(registerMaintenanceWindow).map {
              case result: RegisterMaintenanceWindowResult =>
                HttpResponse(StatusCodes.Accepted,
                             headers = List(Location("/objects/windows/" + result.id.toString)),
                             entity = JsonBody(result.id.toJson))
              case failure: NotificationServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* enumerate all maintenance windows */
      get {
        complete {
          serviceProxy.ask(ListMaintenanceWindows()).map {
            case result: ListMaintenanceWindowsResult =>
              result.windows
            case failure: NotificationServiceOperationFailed =>
              throw failure.failure
          }
        }
      }
    } ~
    pathPrefix("windows" / JavaUUID) { case uuid: UUID =>
      /* modify an existing maintenance window */
      put {
        entity(as[MaintenanceWindowModification]) { case modifications: MaintenanceWindowModification =>
          complete {
            serviceProxy.ask(ModifyMaintenanceWindow(uuid, modifications)).map {
              case result: ModifyMaintenanceWindowResult =>
                result.id
              case failure: NotificationServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* unregister an existing maintenance window */
      delete {
        complete {
          serviceProxy.ask(UnregisterMaintenanceWindow(uuid)).map {
            case result: UnregisterMaintenanceWindowResult =>
              HttpResponse(StatusCodes.OK)
            case failure: NotificationServiceOperationFailed =>
              throw failure.failure
          }
        }
      }
    }
  }

  val objectsTicketsRoutes = {
    path("tickets") {
      /* create new tracking ticket */
      post {
        entity(as[CreateTicket]) { case createTicket: CreateTicket =>
          complete {
            serviceProxy.ask(createTicket).map {
              case result: CreateTicketResult =>
                HttpResponse(StatusCodes.Accepted,
                  headers = List(Location("/objects/tickets/" + result.ticket.toString)),
                  entity = JsonBody(result.ticket.toJson))
              case failure: TrackingServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      } ~
      /* enumerate all tracking tickets */
      get {
        pagingParams { paging =>
        complete {
          serviceProxy.ask(ListTrackingTickets(paging.last, paging.limit)).map {
            case result: ListTrackingTicketsResult =>
              result.tickets
            case failure: TrackingServiceOperationFailed =>
              throw failure.failure
          }
        }}
      } ~
      pathPrefix("tickets" / JavaUUID) { case uuid: UUID =>
          //      /* modify an existing tracking ticket */
          //      put {
          //        entity(as[AppendWorknote]) { case appendWorknote: AppendWorknote =>
          //          complete {
          //            serviceProxy.ask(appendWorknote)).map {
          //              case result: AppendWorknoteResult =>
          //                result.ticket
          //              case failure: TrackingServiceOperationFailed =>
          //                throw failure.failure
          //            }
          //          }
          //        }
          //      } ~
          /* close an existing tracking ticket */
        delete {
          complete {
            serviceProxy.ask(ResolveTicket(uuid)).map {
              case result: ResolveTicketResult =>
                HttpResponse(StatusCodes.OK)
              case failure: TrackingServiceOperationFailed =>
                throw failure.failure
            }
          }
        }
      }
    }
  }

 val objectsRulesRoutes = {
    path("rules") {
      /* enumerate all maintenance windows */
      get {
        complete {
          serviceProxy.ask(ListNotificationRules()).map {
            case result: ListNotificationRulesResult =>
              result.rules
            case failure: NotificationServiceOperationFailed =>
              throw failure.failure
          }
        }
      }
    }
  }

  val objectsRoutes = pathPrefix("objects") {
    objectsSystemsRoutes ~ objectsWindowsRoutes ~ objectsRulesRoutes
  }

  /**
   * Spray routes for invoking services
   */
  val servicesRoutes = pathPrefix("services") {
    pathPrefix("status") {
      path("search") {
        get {
          queryParams { query =>
          pagingParams { paging =>
          complete {
            serviceProxy.ask(SearchCurrentStatus(query.qs, paging.limit)).map {
              case result: SearchCurrentStatusResult =>
                result.status
            }
          }}}
        }
      }
    } ~
    pathPrefix("history") {
      path("search") {
        get {
          queryParams { query =>
          timeseriesParams { timeseries =>
          pagingParams { paging =>
          complete {
            serviceProxy.ask(QueryProbes(query.qs, None)).flatMap {
              case Failure(failure: Throwable) =>
                throw failure
              case Success(result: QueryProbesResult) =>
                serviceProxy.ask(GetStatusHistory(Right(result.refs.toSet), timeseries.from, timeseries.to, paging.limit)).map {
                  case result: GetStatusHistoryResult =>
                    result.history
                  case failure: HistoryServiceOperationFailed =>
                    throw failure.failure
                }
            }
          }}}}
        }
      }
    } ~
    pathPrefix("notifications") {
      path("search") {
        get {
          queryParams { query =>
          timeseriesParams { timeseries =>
          pagingParams { paging =>
          complete {
            serviceProxy.ask(QueryProbes(query.qs, None)).flatMap {
              case Failure(failure: Throwable) =>
                throw failure
              case Success(result: QueryProbesResult) =>
                serviceProxy.ask(GetNotificationHistory(Right(result.refs.toSet), timeseries.from, timeseries.to, paging.limit)).map {
                  case result: GetNotificationHistoryResult =>
                    result.history
                  case failure: HistoryServiceOperationFailed =>
                    throw failure.failure
                }
            }
          }}}}
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
          log.error(ex, "caught exception processing HTTP request: {}", ex.getMessage)
          ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(ex))))
      }
    case ex: Throwable => ctx =>
      log.error(ex, "caught exception processing HTTP request: {}", ex.getMessage)
      ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(new Exception("internal server error")))))
  }

  def throwableToJson(t: Throwable): JsValue = JsObject(Map("description" -> JsString(t.getMessage)))
}
