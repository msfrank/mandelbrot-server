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

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import spray.routing.{HttpService, ExceptionHandler}
import spray.http._
import spray.http.HttpHeaders.Location
import spray.util.LoggingContext
import java.net.URI

import io.mandelbrot.core._
import io.mandelbrot.core.registry._

/**
 * ApiService contains the REST API logic.
 */
trait ApiService extends HttpService {
  import scala.language.postfixOps
  import spray.httpx.SprayJsonSupport._
  import spray.json._
  import JsonProtocol._

  val settings: HttpSettings

  implicit def log: LoggingAdapter
  implicit def objectRegistry: ActorRef
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout

  /**
   * Spray routes for managing probe registration
   */
  val registryRoutes = {
    path("1" / "registry") {
      /* register new probe system, or fail if it already exists */
      post {
        entity(as[RegisterProbeSystem]) { case registerProbeSystem: RegisterProbeSystem =>
          complete {
            objectRegistry.ask(registerProbeSystem).map {
              case result: RegisterProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted, headers = List(Location("/1/registry/" + registerProbeSystem.uri.toString)))
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
          objectRegistry.ask(ListProbeSystems()).map {
            case result: ListProbeSystemsResult =>
              result.uris
            case failure: ApiFailure =>
              throw new ApiException(failure)
          }
        }
      }
    } ~
    path("1" / "registry" / Segment) { case uri: String =>
      /* retrieve the spec for the specified probe system */
      get {
        complete {
          objectRegistry.ask(DescribeProbeSystem(new URI(uri))).map {
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
            objectRegistry.ask(updateProbeSystem).map {
              case result: UpdateProbeSystemResult =>
                HttpResponse(StatusCodes.Accepted, headers = List(Location("/1/registry/" + updateProbeSystem.uri.toString)))
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }
          }
        }
      }
    }
  }

  /**
   * Spray routes for managing an object system
   */
  val systemRoutes = {
    pathPrefix("1" / "system" / Segment) { case uri: String =>
      pathEndOrSingleSlash {
        /* describe the state of the ProbeSystem */
        get {
          complete {
            objectRegistry.ask(GetProbeSystemState(new URI(uri))).map {
              case result: GetProbeSystemStateResult =>
                result
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }
          }
        }
      }
    }
  }

  /**
   * Spray routes for updating object lifecycle
   */
//  val actionRoutes = {
//    /* FIXME: enter/leave maintenance mode */
//    pathPrefix("1" / "action" / Segment) { case uri: String =>
//    }
//  }

  val version1 = registryRoutes ~ systemRoutes

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
