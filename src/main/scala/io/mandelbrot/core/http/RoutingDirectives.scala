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

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.http.{HttpHeader, HttpHeaders}
import spray.http.Uri.Path
import spray.routing.PathMatcher1
import spray.util.SSLSessionInfo
import java.net.URI

import io.mandelbrot.core.{BadRequest, ApiException}
import spray.routing.PathMatcher.{Unmatched, Matched}
import shapeless.HNil

object RoutingDirectives {
  import shapeless._
  import spray.routing._
  import Directives._

  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  /**
   *
   */
  type PathParams = Option[Set[String]]
  private val parameterMultimap2pathParams: Directive[Map[String,List[String]] :: HNil] = parameterMultiMap
  val pathParams: Directive1[PathParams] = parameterMultimap2pathParams.hmap {
    case params :: HNil =>
      params.get("path") match {
        case Some(paths) => Some(paths.toSet)
        case None => None
      }
  }

  /**
   *
   */
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

  /**
   *
   */
  case class QueryParams(query: String, limit: Option[Int])
  private val parameters2queryParams: Directive[String :: Option[Int] :: HNil] = {
    parameters('q.as[String], 'limit.as[Int].?)
  }
  val queryParams: Directive1[QueryParams] = parameters2queryParams.hmap {
    case query :: limit :: HNil => QueryParams(query, limit)
  }

  /**
   *
   */
  private def extractSSLSessionInfo: HttpHeader => Option[SSLSessionInfo] = {
    case header: HttpHeaders.`SSL-Session-Info` => Some(header.info)
    case _ => None
  }
  val sslSessionInfo: Directive1[Option[SSLSessionInfo]] = optionalHeaderValue(extractSSLSessionInfo)
}

/**
 *
 */
case class TimeseriesParams(from: Option[DateTime], to: Option[DateTime], limit: Option[Int], last: Option[String])

/**
 *
 */
object Uri extends PathMatcher1[URI] {
  def apply(path: Path) = path match {
    case Path.Segment(segment, tail) ⇒ Matched(tail, new URI(segment) :: HNil)
    case _                           ⇒ Unmatched
  }
}