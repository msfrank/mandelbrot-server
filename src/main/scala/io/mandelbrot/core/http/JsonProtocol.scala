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

import spray.json._
import spray.http.{ContentTypes, HttpEntity}
import org.joda.time.DateTime
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.UUID
import java.net.URI
import java.util.concurrent.TimeUnit
import java.nio.charset.Charset

import io.mandelbrot.core.registry._

object JsonProtocol extends DefaultJsonProtocol {

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
    def write(datetime: DateTime) = JsString(datetime.getMillis.toString)
    def read(value: JsValue) = value match {
      case JsString(datetime) => new DateTime(datetime.toLong)
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

  /* */
  implicit object ProbeRefFormat extends RootJsonFormat[ProbeRef] {
    def write(ref: ProbeRef) = JsString(ref.toString)
    def read(value: JsValue) = value match {
      case JsString(string) =>
        val parts = string.split('/')
        ProbeRef(new URI(parts.head), parts.tail.toVector)
      case _ => throw new DeserializationException("expected ProbeRef")
    }
  }

  /* */
  implicit val _ProbeSpecFormat: JsonFormat[ProbeSpec] = lazyFormat(jsonFormat(ProbeSpec, "objectType", "metaData", "children"))
  implicit val ProbeSpecFormat = rootFormat(_ProbeSpecFormat)

  /* */
  implicit val RegisterProbeSystemFormat = jsonFormat2(RegisterProbeSystem)

  /* */
  implicit val UpdateProbeSystemFormat = jsonFormat2(UpdateProbeSystem)
}

object JsonBody {
  val charset = Charset.defaultCharset()
  def apply(js: JsValue): HttpEntity = HttpEntity(ContentTypes.`application/json`, js.prettyPrint.getBytes(charset))
}