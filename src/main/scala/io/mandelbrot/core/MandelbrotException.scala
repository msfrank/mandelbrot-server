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

package io.mandelbrot.core

class MandelbrotException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(cause: Throwable) = this("", cause)
  def this(message: String) = this(message, null)
}

/**
 * abstract base class for all API failures.
 */
sealed abstract class ApiFailure(val description: String) extends Exception(description)

/**
 * trait and companion object for API failures which indicate the operation
 * should be retried at a later time.
 */
trait RetryLater
case object RetryLater extends ApiFailure("retry operation later") with RetryLater

/**
 * trait and companion object for API failures which indicate the operation
 * parameters must be modified before being submitted again.
 */
trait BadRequest
case object BadRequest extends ApiFailure("bad request") with BadRequest

/**
 * trait and companion object for API failures which indicate the resource
 * was not found.
 */
trait ResourceNotFound
case object ResourceNotFound extends ApiFailure("resource not found") with ResourceNotFound

/**
 * trait and companion object for API failures which indicate mutating the
 * specified resource conflicts with policy.
 */
trait Conflict
case object Conflict extends ApiFailure("resource conflict") with Conflict

/**
 * trait and companion object for API failures which indicate querying or
 * mutating the specified resource is forbidden by policy.
 */
trait Forbidden
case object Forbidden extends ApiFailure("forbidden action") with Forbidden

/**
 * trait and companion object for API failures which indicate the operation
 * is not implemented.  this should only be thrown by a service to indicate
 * that functionality provided by an optional extension is not available because
 * the extension is not loaded.
 */
trait NotImplemented
case object NotImplemented extends ApiFailure("not implemented") with NotImplemented

/**
 * trait and companion object for API failures which indicate the operation
 * generated an unknown internal error.  failures of this kind should log the
 * true exception at debug level.
 */
trait InternalError
case object InternalError extends ApiFailure("internal error") with InternalError

/**
 * Exception which wraps an API failure.
 */
case class ApiException(failure: ApiFailure) extends MandelbrotException(failure.description, failure)
