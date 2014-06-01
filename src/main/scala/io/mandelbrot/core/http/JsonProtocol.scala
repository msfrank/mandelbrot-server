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
import org.joda.time.format.ISODateTimeFormat
import java.util.UUID
import java.net.URI
import java.util.concurrent.TimeUnit
import java.nio.charset.Charset

import io.mandelbrot.core.registry._
import io.mandelbrot.core.history._
import io.mandelbrot.core.message._
import io.mandelbrot.core.notification._

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

  /* convert ProbeMatcher class */
  implicit object ProbeMatcherFormat extends RootJsonFormat[ProbeMatcher] {
    val probeMatcherParser = new ProbeMatcherParser()
    def write(matcher: ProbeMatcher) = JsString(matcher.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => probeMatcherParser.parseProbeMatcher(string)
      case _ => throw new DeserializationException("expected ProbeMatcher")
    }
  }

  /* convert NotificationBehavior class */
  implicit object NotificationBehaviorFormat extends RootJsonFormat[NotificationBehavior] {
    def write(policyType: NotificationBehavior) = policyType match {
      case EmitNotifications => JsString("emit")
      case EscalateNotifications => JsString("escalate")
      case SquelchNotifications => JsString("squelch")
      case unknown => throw new SerializationException("unknown NotificationBehavior " + unknown.getClass)
    }

    def read(value: JsValue) = value match {
      case JsString("emit") => EmitNotifications
      case JsString("escalate") => EscalateNotifications
      case JsString("squelch") => SquelchNotifications
      case unknown => throw new DeserializationException("unknown NotificationBehavior " + unknown)
    }
  }

  /* convert NotificationPolicy class */
  implicit val NotificationPolicyFormat = jsonFormat2(NotificationPolicy)

  /* convert ProbePolicy class */
  implicit val ProbePolicyFormat = jsonFormat7(ProbePolicy)

  /* a little extra magic here- we use lazyFormat because ProbeSpec has a recursive definition */
  implicit val _ProbeSpecFormat: JsonFormat[ProbeSpec] = lazyFormat(jsonFormat(ProbeSpec, "probeType", "metadata", "policy", "children"))
  implicit val ProbeSpecFormat = rootFormat(_ProbeSpecFormat)

  /* convert ProbeRegistration class */
  implicit val ProbeRegistrationFormat = jsonFormat3(ProbeRegistration)

  /* convert ProbeSystemMetadata class */
  implicit val ProbeSystemMetadataFormat = jsonFormat3(ProbeSystemMetadata)

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

  /* convert ProbeNotification class */
  implicit object ProbeNotificationFormat extends RootJsonFormat[ProbeNotification] {
    def write(notification: ProbeNotification) = {
      val correlation = notification.correlation match {
        case Some(_correlation) =>  Map("correlation" -> _correlation.toJson)
        case None => Map.empty[String,JsValue]
      }
      JsObject(Map(
        "probeRef" -> notification.probeRef.toJson,
        "timestamp" -> notification.timestamp.toJson,
        "kind" -> JsString(notification.kind),
        "description" -> JsString(notification.description)
      ) ++ correlation)
    }
    def read(value: JsValue) = value match {
      case JsObject(fields) =>
        val probeRef = fields.get("probeRef") match {
          case Some(JsString(string)) => ProbeRef(string)
          case None => throw new DeserializationException("ProbeNotification missing field 'probeRef'")
          case unknown => throw new DeserializationException("failed to parse ProbeNotification field 'probeRef'")
        }
        val timestamp = fields.get("timestamp") match {
          case Some(JsNumber(number)) => new DateTime(number.toLong)
          case None => throw new DeserializationException("ProbeNotification missing field 'timestamp'")
          case unknown => throw new DeserializationException("failed to parse ProbeNotification field 'timestamp'")
        }
        val kind = fields.get("kind") match {
          case Some(JsString(string)) => string
          case None => throw new DeserializationException("ProbeNotification missing field 'kind'")
          case unknown => throw new DeserializationException("failed to parse ProbeNotification field 'kind'")
        }
        val description = fields.get("description") match {
          case Some(JsString(string)) => string
          case None => throw new DeserializationException("ProbeNotification missing field 'description'")
          case unknown => throw new DeserializationException("failed to parse ProbeNotification field 'description'")
        }
        val correlation = fields.get("correlation") match {
          case Some(JsString(string)) => Some(UUID.fromString(string))
          case None => None
          case unknown => throw new DeserializationException("failed to parse ProbeNotification field 'correlation'")
        }
        ProbeNotification(probeRef, timestamp, kind, description, correlation)
      case unknown => throw new DeserializationException("unknown ProbeNotification " + unknown)
    }
  }

  /* convert MaintenanceWindow class */
  implicit val MaintenanceWindowFormat = jsonFormat4(MaintenanceWindow)

  /* convert ProbeStatus class */
  implicit val ProbeStatusFormat = jsonFormat10(ProbeStatus)

  /* registry operations */
  implicit val RegisterProbeSystemFormat = jsonFormat2(RegisterProbeSystem)
  implicit val UpdateProbeSystemFormat = jsonFormat2(UpdateProbeSystem)

  /* probe system operations */
  implicit val GetProbeSystemStatusFormat = jsonFormat2(GetProbeSystemStatus)
  implicit val GetProbeSystemStatusResultFormat = jsonFormat2(GetProbeSystemStatusResult)
  implicit val GetProbeSystemMetadataFormat = jsonFormat2(GetProbeSystemMetadata)
  implicit val GetProbeSystemMetadataResultFormat = jsonFormat2(GetProbeSystemMetadataResult)
  implicit val GetProbeSystemPolicyFormat = jsonFormat2(GetProbeSystemPolicy)
  implicit val GetProbeSystemPolicyResultFormat = jsonFormat2(GetProbeSystemPolicyResult)
  implicit val AcknowledgeProbeSystemFormat = jsonFormat2(AcknowledgeProbeSystem)
  implicit val AcknowledgeProbeSystemResultFormat = jsonFormat2(AcknowledgeProbeSystemResult)
  implicit val UnacknowledgeProbeSystemFormat = jsonFormat2(UnacknowledgeProbeSystem)
  implicit val UnacknowledgeProbeSystemResultFormat = jsonFormat2(UnacknowledgeProbeSystemResult)

  /* probe operations */
  implicit val AcknowledgeProbeFormat = jsonFormat2(AcknowledgeProbe)
  implicit val AcknowledgeProbeResultFormat = jsonFormat2(AcknowledgeProbeResult)
  implicit val AppendProbeWorknoteFormat = jsonFormat4(AppendProbeWorknote)
  implicit val AppendProbeWorknoteResultFormat = jsonFormat2(AppendProbeWorknoteResult)
  implicit val SetProbeSquelchFormat = jsonFormat2(SetProbeSquelch)
  implicit val SetProbeSquelchResultFormat = jsonFormat2(SetProbeSquelchResult)

  /* history service operations */
  implicit val GetStatusHistoryFormat = jsonFormat4(GetStatusHistory)
  implicit val GetStatusHistoryResultFormat = jsonFormat2(GetStatusHistoryResult)
  implicit val GetNotificationHistoryFormat = jsonFormat4(GetNotificationHistory)
  implicit val GetNotificationHistoryResultFormat = jsonFormat2(GetNotificationHistoryResult)

  /* notification service operations */
  implicit val RegisterMaintenanceWindowFormat = jsonFormat3(RegisterMaintenanceWindow)
  implicit val RegisterMaintenanceWindowResultFormat = jsonFormat2(RegisterMaintenanceWindowResult)
  implicit val UnregisterMaintenanceWindowFormat = jsonFormat1(UnregisterMaintenanceWindow)
  implicit val UnregisterMaintenanceWindowResultFormat = jsonFormat2(UnregisterMaintenanceWindowResult)

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