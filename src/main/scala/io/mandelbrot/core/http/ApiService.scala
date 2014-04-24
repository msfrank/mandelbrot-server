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

import io.mandelbrot.core._
import io.mandelbrot.core.messagestream._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.history.{GetHistoryFor, GetAllHistory}

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
  val messageStream: MessageStreamBus

  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  /**
   * Spray routes for managing objects
   */
  val objectsRoutes = pathPrefix("objects") {
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
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }
          }
        }
      } ~
      /* enumerate all registered probe systems */
      get {
        complete {
          registryService.ask(ListProbeSystems()).map {
            case result: ListProbeSystemsResult =>
              result.uris
            case failure: ApiFailure =>
              throw new ApiException(failure)
          }
        }
      }
    } ~
    pathPrefix("systems" / Segment) { case uri: String =>
      pathEndOrSingleSlash {
        /* retrieve the spec for the specified probe system */
        get {
          complete {
            registryService.ask(DescribeProbeSystem(new URI(uri))).map {
              case result: DescribeProbeSystemResult =>
                result.spec
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }
          }
        } ~
        /* update the spec for the specified probe system */
        post {
          entity(as[UpdateProbeSystem]) { case updateProbeSystem: UpdateProbeSystem =>
            complete {
              registryService.ask(updateProbeSystem).map {
                case result: UpdateProbeSystemResult =>
                  HttpResponse(StatusCodes.Accepted, headers = List(Location("/objects/systems/" + updateProbeSystem.uri.toString)))
                case failure: ApiFailure =>
                  throw new ApiException(failure)
              }
            }
          }
        }
      } ~
      pathPrefix("properties") {
        path("state") {
          /* describe the state of the ProbeSystem */
          get {
            complete {
              registryService.ask(GetProbeSystemStatus(new URI(uri))).map {
                case result: GetProbeSystemStatusResult =>
                  result.state
                case failure: ProbeSystemOperationFailed =>
                  throw failure.failure
              }
            }
          }
        } ~
        path("metadata") {
          /* return all metadata attached to the ProbeSystem */
          get {
            complete {
              registryService.ask(GetProbeSystemMetadata(new URI(uri))).map {
                case result: GetProbeSystemMetadataResult =>
                  result.metadata
                case failure: ProbeSystemOperationFailed =>
                  throw failure.failure
              }
            }
          }
        }
      } ~
      pathPrefix("collections") {
        path("history") {
          get {
            timeseriesParams { case TimeseriesParams(from, to, limit, last) =>
            parameterMultiMap { case params =>
              val query = params.get("path") match {
                case Some(paths) =>
                  val probeRefs = paths.map(ProbeRef(uri, _)).toSet
                  GetHistoryFor(probeRefs, from, to, limit)
                case None =>
                  GetAllHistory(ProbeRef(uri), from, to, limit)
              }
              complete {
                stateService.ask(query).map { case _ => StatusCodes.OK }
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
            entity(as[AcknowledgeProbe]) { case command: AcknowledgeProbe =>
              complete {
                registryService.ask(command).map {
                  case result: AcknowledgeProbeResult =>
                    result
                  case failure: ProbeOperationFailed =>
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
