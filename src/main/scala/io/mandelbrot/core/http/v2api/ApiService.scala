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

import java.io.{ByteArrayOutputStream, PrintStream}

import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.Timeout
import io.mandelbrot.core._
import io.mandelbrot.core.http.HttpSettings
import io.mandelbrot.core.http.json.JsonBody
import spray.http._
import spray.json._
import spray.routing._
import spray.util.LoggingContext

import scala.concurrent.ExecutionContext

/**
 * ApiService contains the shared REST API logic.
 */
trait ApiService extends HttpService {
  import scala.language.postfixOps

  val settings: HttpSettings

  implicit def log: LoggingAdapter
  implicit val system: ActorSystem
  implicit def executionContext: ExecutionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout
  def serviceProxy: ActorRef

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

  /**
   * catch unhandled rejections and convert them to HTTP responses.  any rejection
   * that is not explicitly handled results in a generic 500 Internal Server Error.
   */
  implicit val rejectionHandler = RejectionHandler {
    case MalformedRequestContentRejection(message, cause) :: _ =>
      complete(HttpResponse(StatusCodes.BadRequest, JsonBody(rejectionToJson(message, cause))))
    case MissingQueryParamRejection(parameterName) :: _ =>
      complete(HttpResponse(StatusCodes.BadRequest, JsonBody(rejectionToJson(s"missing required parameter $parameterName"))))
    case MalformedQueryParamRejection(_, message, cause) :: _ =>
      complete(HttpResponse(StatusCodes.BadRequest, JsonBody(rejectionToJson(message, cause))))
    case unknown =>
      log.error(s"caught unexpected rejection: $unknown")
      val entity = JsonBody(JsObject(Map("description" -> JsString("internal server error"))))
      complete(HttpResponse(StatusCodes.InternalServerError, entity))
  }

  /**
   * convert the rejection described by message and cause into a Json entity.
   */
  def rejectionToJson(message: String, cause: Option[Throwable] = None): JsValue = cause match {
    case Some(t) if settings.debugExceptions =>
      val os = new ByteArrayOutputStream()
      val ps = new PrintStream(os)
      t.printStackTrace(ps)
      val stackTrace = os.toString
      ps.close()
      JsObject(Map("description" -> JsString(message), "stackTrace" -> JsString(stackTrace)))
    case _ =>
      JsObject(Map("description" -> JsString(message)))
  }

  /**
   * convert the specified exception into a Json entity.
   */
  def throwableToJson(t: Throwable): JsValue = {
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
