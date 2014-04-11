package io.mandelbrot.core

class MandelbrotException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(cause: Throwable) = this("", cause)
  def this(message: String) = this(message, null)
}

/**
 * abstract base class for all API failures.
 */
abstract class ApiFailure(val description: String)

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
 *
 */
trait Conflict
case object Conflict extends ApiFailure("resource conflict") with Conflict

/**
 * Exception which wraps an API failure.
 */
class ApiException(val failure: ApiFailure) extends MandelbrotException(failure.description)