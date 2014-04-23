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
import io.mandelbrot.core.messagestream.{GenericMessage, Message, StatusMessage}
import org.joda.time.format.ISODateTimeFormat

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
    val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()
    def write(datetime: DateTime) = JsNumber(datetime.getMillis)
    def read(value: JsValue) = value match {
      case JsString(string) => datetimeParser.parseDateTime(string)
      case JsNumber(bigDecimal) => new DateTime(bigDecimal.toLong)
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

  /* convert ProbeRef class */
  implicit object ProbeRefFormat extends RootJsonFormat[ProbeRef] {
    def write(ref: ProbeRef) = JsString(ref.toString)
    def read(value: JsValue) = value match {
      case JsString(string) =>
        val parts = string.split('/')
        ProbeRef(new URI(parts.head), parts.tail.toVector)
      case _ => throw new DeserializationException("expected ProbeRef")
    }
  }

  /* a little extra magic here- we use lazyFormat because ProbeSpec has a recursive definition */
  implicit val _ProbeSpecFormat: JsonFormat[ProbeSpec] = lazyFormat(jsonFormat(ProbeSpec, "objectType", "metaData", "children"))
  implicit val ProbeSpecFormat = rootFormat(_ProbeSpecFormat)

  /* convert ProbeHealth class */
  implicit object ProbeHealthFormat extends RootJsonFormat[ProbeHealth] {
    def write(health: ProbeHealth) = health match {
      case ProbeHealthy => JsString("healthy")
      case ProbeDegraded => JsString("degraded")
      case ProbeFailed => JsString("failed")
      case ProbeUnknown => JsString("unknown")
      case unknown => throw new SerializationException("unknown ProbeHealth state " + unknown.getClass)
    }
    def read(value: JsValue) = value match {
      case JsString("healthy") => ProbeHealthy
      case JsString("degraded") => ProbeDegraded
      case JsString("failed") => ProbeFailed
      case JsString("unknown") => ProbeUnknown
      case unknown => throw new DeserializationException("unknown ProbeHealth state " + unknown)
    }
  }

  /* convert ProbeLifecycle class */
  implicit object ProbeLifecycleFormat extends RootJsonFormat[ProbeLifecycle] {
    def write(lifecycle: ProbeLifecycle) = lifecycle match {
      case ProbeJoining => JsString("joining")
      case ProbeKnown => JsString("known")
      case ProbeLeaving => JsString("leaving")
      case ProbeRetired => JsString("retired")
      case unknown => throw new SerializationException("unknown ProbeLifecycle state " + unknown.getClass)
    }
    def read(value: JsValue) = value match {
      case JsString("joining") => ProbeJoining
      case JsString("known") => ProbeKnown
      case JsString("leaving") => ProbeLeaving
      case JsString("retired") => ProbeRetired
      case unknown => throw new DeserializationException("unknown ProbeLifecycle state " + unknown)
    }
  }

  /* registry operations */
  implicit val RegisterProbeSystemFormat = jsonFormat2(RegisterProbeSystem)
  implicit val UpdateProbeSystemFormat = jsonFormat2(UpdateProbeSystem)

  /* probe system operations */
  implicit val ProbeStateFormat = jsonFormat9(ProbeState)
  implicit val GetProbeSystemStateFormat = jsonFormat1(GetProbeSystemState)
  implicit val GetProbeSystemStateResultFormat = jsonFormat2(GetProbeSystemStateResult)

  /* probe operations */
  implicit val AcknowledgeProbeFormat = jsonFormat2(AcknowledgeProbe)
  implicit val AcknowledgeProbeResultFormat = jsonFormat2(AcknowledgeProbeResult)
  implicit val SetProbeSquelchFormat = jsonFormat2(SetProbeSquelch)
  implicit val SetProbeSquelchResultFormat = jsonFormat2(SetProbeSquelchResult)

  /* message types */
  implicit val StatusMessageFormat = jsonFormat5(StatusMessage)

  /* */
  implicit object MessageFormat extends RootJsonFormat[Message] {
    def write(message: Message) = {
      val (messageType, payload) = message match {
        case m: StatusMessage =>
          "io.mandelbrot.message.StatusMessage" -> StatusMessageFormat.write(m)
        case m: GenericMessage =>
          m.messageType -> m.value
      }
      JsObject(Map("messageType" -> JsString(messageType), "payload" -> payload))
    }
    def read(value: JsValue) = {
      value match {
        case JsObject(fields) =>
          if (!fields.contains("payload"))
            throw new DeserializationException("missing payload")
          fields.get("messageType") match {
            case Some(JsString("io.mandelbrot.message.StatusMessage")) =>
              StatusMessageFormat.read(fields("payload"))
            case Some(JsString(unknownType)) =>
              GenericMessage(unknownType, fields("payload"))
            case None =>
              throw new DeserializationException("missing messageType")
            case unknownValue =>
              throw new DeserializationException("messageType is not a string")
          }
        case unknown => throw new DeserializationException("unknown Message format")
      }
    }
  }
}

object JsonBody {
  val charset = Charset.defaultCharset()
  def apply(js: JsValue): HttpEntity = HttpEntity(ContentTypes.`application/json`, js.prettyPrint.getBytes(charset))
}