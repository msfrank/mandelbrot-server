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

package io.mandelbrot.core.model.json

import spray.json._
import java.util.UUID

import io.mandelbrot.core.model._

/**
 *
 */
trait NotificationProtocol extends DefaultJsonProtocol with ConstantsProtocol with ResourceProtocol {

  /* convert CheckNotification class */
  implicit object CheckNotificationFormat extends RootJsonFormat[CheckNotification] {
    def write(notification: CheckNotification) = {
      val correlation = notification.correlation match {
        case Some(_correlation) =>  Map("correlation" -> _correlation.toJson)
        case None => Map.empty[String,JsValue]
      }
      JsObject(Map(
        "checkRef" -> notification.checkRef.toJson,
        "timestamp" -> notification.timestamp.toJson,
        "kind" -> JsString(notification.kind),
        "description" -> JsString(notification.description)
      ) ++ correlation)
    }
    def read(value: JsValue) = value match {
      case JsObject(fields) =>
        val checkRef = fields.get("checkRef") match {
          case Some(JsString(string)) => CheckRef(string)
          case None => throw new DeserializationException("CheckNotification missing field 'checkRef'")
          case unknown => throw new DeserializationException("failed to parse CheckNotification field 'checkRef'")
        }
        val timestamp = fields.get("timestamp") match {
          case Some(_timestamp) => DateTimeFormat.read(_timestamp)
          case None => throw new DeserializationException("CheckNotification missing field 'timestamp'")
        }
        val kind = fields.get("kind") match {
          case Some(JsString(string)) => string
          case None => throw new DeserializationException("CheckNotification missing field 'kind'")
          case unknown => throw new DeserializationException("failed to parse CheckNotification field 'kind'")
        }
        val description = fields.get("description") match {
          case Some(JsString(string)) => string
          case None => throw new DeserializationException("CheckNotification missing field 'description'")
          case unknown => throw new DeserializationException("failed to parse CheckNotification field 'description'")
        }
        val correlation = fields.get("correlation") match {
          case Some(JsString(string)) => Some(UUID.fromString(string))
          case None => None
          case unknown => throw new DeserializationException("failed to parse CheckNotification field 'correlation'")
        }
        CheckNotification(checkRef, timestamp, kind, description, correlation)
      case unknown => throw new DeserializationException("unknown CheckNotification " + unknown)
    }
  }
}
