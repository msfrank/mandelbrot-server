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
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.notification._

/**
 *
 */
trait NotificationProtocol extends DefaultJsonProtocol with ConstantsProtocol with ResourceProtocol {

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
          case Some(_timestamp) => DateTimeFormat.read(_timestamp)
          case None => throw new DeserializationException("ProbeNotification missing field 'timestamp'")
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
}
