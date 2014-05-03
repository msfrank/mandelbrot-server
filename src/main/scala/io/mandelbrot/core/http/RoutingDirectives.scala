package io.mandelbrot.core.http

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import io.mandelbrot.core.{BadRequest, ApiException}
import spray.http.{HttpHeader, HttpHeaders}
import spray.util.SSLSessionInfo

object RoutingDirectives {
  import shapeless._
  import spray.routing._
  import Directives._

  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  private val parameters2timeseriesParams: Directive[Option[String] :: Option[String] :: Option[Int] :: Option[String] :: HNil] = {
    parameters('from.as[String].?, 'to.as[String].?, 'limit.as[Int].?, 'last.as[String].?)
  }

  val timeseriesParams: Directive1[TimeseriesParams] = parameters2timeseriesParams.hmap {
    case from :: to :: limit :: last :: HNil =>
      val start = try {
        if (from.isDefined) Some(datetimeParser.parseDateTime(from.get)) else None
      } catch {
        case ex: Throwable => throw new ApiException(BadRequest)
      }
      val end = try {
        if (to.isDefined) Some(datetimeParser.parseDateTime(to.get)) else None
      } catch {
        case ex: Throwable => throw new ApiException(BadRequest)
      }
      TimeseriesParams(start, end, limit, last)
  }

  private def extractSSLSessionInfo: HttpHeader => Option[SSLSessionInfo] = {
    case header: HttpHeaders.`SSL-Session-Info` => Some(header.info)
    case _ => None
  }

  val sslSessionInfo: Directive1[Option[SSLSessionInfo]] = optionalHeaderValue(extractSSLSessionInfo)
}

case class TimeseriesParams(from: Option[DateTime], to: Option[DateTime], limit: Option[Int], last: Option[String])