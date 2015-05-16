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

import akka.actor.{AddressFromURIString, Address}
import spray.http.{HttpHeader, HttpHeaders}
import spray.http.Uri.Path
import spray.routing.PathMatcher1
import spray.routing.PathMatcher.{Unmatched, Matched}
import spray.util.SSLSessionInfo
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import shapeless.HNil

import io.mandelbrot.core.model.{CheckId, AgentId}
import io.mandelbrot.core.{BadRequest, ApiException}

/**
 *
 */
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
  case class TimeseriesParams(from: Option[DateTime], to: Option[DateTime], fromInclusive: Boolean, toExclusive: Boolean, descending: Boolean)
  private val parameters2timeseriesParams: Directive[Option[String] :: Option[Boolean] :: Option[String] :: Option[Boolean] :: Option[Boolean] :: HNil] = {
    parameters('from.as[String].?, 'fromInclusive.as[Boolean].?,
      'to.as[String].?, 'toExclusive.as[Boolean].?, 'descending.as[Boolean].?)
  }
  private def parseDateTime(string: String): DateTime = try {
   datetimeParser.parseDateTime(string)
  } catch {
    case ex: IllegalArgumentException =>
      try {
        new DateTime(string.toLong, DateTimeZone.UTC)
      } catch {
        case ex: Throwable => throw ApiException(BadRequest)
      }
    case ex: Throwable => throw ApiException(BadRequest)
  }
  val timeseriesParams: Directive1[TimeseriesParams] = parameters2timeseriesParams.hmap {
    case from :: fromInclusive :: to :: toExclusive :: descending :: HNil =>
      TimeseriesParams(from.map(parseDateTime), to.map(parseDateTime), fromInclusive.getOrElse(false), toExclusive.getOrElse(false), descending.getOrElse(false))
  }

  /**
   *
   */
  case class QueryParams(qs: String)
  private val parameters2queryParams: Directive[String :: HNil] = {
    parameters('q.as[String])
  }
  val queryParams: Directive1[QueryParams] = parameters2queryParams.hmap {
    case qs :: HNil => QueryParams(qs)
  }

  /**
   *
   */
  case class PagingParams(last: Option[String], limit: Option[Int])
  private val parameters2pagingParams: Directive[Option[String] :: Option[Int] :: HNil] = {
    parameters('last.as[String].?, 'limit.as[Int].?)
  }
  val pagingParams: Directive1[PagingParams] = parameters2pagingParams.hmap {
    case last :: limit :: HNil => PagingParams(last, limit)
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
object AgentIdMatcher extends PathMatcher1[AgentId] {
  def apply(path: Path) = path match {
    case Path.Segment(segment, tail) =>
      Matched(tail, AgentId(segment) :: HNil)
    case _ =>
      Unmatched
  }
}

/**
 *
 */
object CheckIdMatcher extends PathMatcher1[CheckId] {
  def apply(path: Path) = path match {
    case Path.Segment(segment, tail) =>
      Matched(tail, CheckId(segment) :: HNil)
    case _ =>
      Unmatched
  }
}

/**
 *
 */
object ClusterAddress extends PathMatcher1[Address] {
  def apply(path: Path) = path match {
    case Path.Segment(segment, tail) =>
      Matched(tail, AddressFromURIString(segment) :: HNil)
    case _ =>
      Unmatched
  }
}
