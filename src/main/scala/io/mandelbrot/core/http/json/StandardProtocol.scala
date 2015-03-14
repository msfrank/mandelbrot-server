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

package io.mandelbrot.core.http.json

import spray.json._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.math.BigDecimal
import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * 
 */
trait StandardProtocol extends DefaultJsonProtocol {

  /* convert UUID class */
  implicit object UUIDFormat extends RootJsonFormat[UUID] {
    def write(uuid: UUID) = JsString(uuid.toString)
    def read(value: JsValue) = value match {
      case JsString(uuid) => UUID.fromString(uuid)
      case _ => throw new DeserializationException("expected UUID")
    }
  }

  /* convert DateTime class */
  implicit object DateTimeFormat extends RootJsonFormat[DateTime] {
    val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()
    def write(datetime: DateTime) = JsNumber(datetime.getMillis)
    def read(value: JsValue) = value match {
      case JsString(string) => datetimeParser.parseDateTime(string)
      case JsNumber(bigDecimal) => new DateTime(bigDecimal.toLong, DateTimeZone.UTC)
      case _ => throw new DeserializationException("expected DateTime")
    }
  }

  /* convert Duration class */
  implicit object DurationFormat extends RootJsonFormat[Duration] {
    def write(duration: Duration) = JsNumber(duration.toMillis)
    def read(value: JsValue) = value match {
      case JsNumber(duration) => Duration(duration.toLong, TimeUnit.MILLISECONDS)
      case _ => throw new DeserializationException("expected Duration")
    }
  }

  /* convert FiniteDuration class */
  implicit object FiniteDurationFormat extends RootJsonFormat[FiniteDuration] {
    def write(duration: FiniteDuration) = JsNumber(duration.toMillis)
    def read(value: JsValue) = value match {
      case JsNumber(duration) => FiniteDuration(duration.toLong, TimeUnit.MILLISECONDS)
      case _ => throw new DeserializationException("expected FiniteDuration")
    }
  }

  /* convert URI class */
  implicit object URIFormat extends RootJsonFormat[URI] {
    def write(uri: URI) = JsString(uri.toString)
    def read(value: JsValue) = value match {
      case JsString(uri) => new URI(uri)
      case _ => throw new DeserializationException("expected URI")
    }
  }

  /* convert BigDecimal class */
  implicit object BigDecimalFormat extends RootJsonFormat[BigDecimal] {
    def write(value: BigDecimal) = JsString(value.toString())
    def read(value: JsValue) = value match {
      case JsNumber(v) => v
      case JsString(v) => BigDecimal(v)
      case unknown => throw new DeserializationException("unknown metric value type " + unknown)
    }
  }
}
