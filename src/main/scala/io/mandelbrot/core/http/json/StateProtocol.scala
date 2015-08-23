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

import io.mandelbrot.core.model._

/**
 *
 */
trait StateProtocol extends DefaultJsonProtocol with ConstantsProtocol with NotificationProtocol {

  /* convert ScalarMapObservation class */
  implicit val ScalarMapObservationFormat = jsonFormat2(ScalarMapObservation)

  /* convert subclasses of Observation */
  implicit object ObservationFormat extends JsonFormat[Observation] {
    def write(observation: Observation) = observation match {
      case scalarMapObservation: ScalarMapObservation => scalarMapObservation.toJson
      case _ => throw new SerializationException("expected Observation")
    }
    def read(value: JsValue): Observation = value match {
      case JsObject(fields) if fields.contains("metrics") =>
        value.convertTo[ScalarMapObservation]
      case _ => throw new DeserializationException("expected Observation")
    }
  }

  /* convert CheckStatus class */
  implicit val CheckStatusFormat = jsonFormat11(CheckStatus)

  /* convert CheckCondition class */
  implicit val CheckConditionFormat = jsonFormat8(CheckCondition)

  /* convert CheckNotifications class */
  implicit val CheckNotificationsFormat = jsonFormat3(CheckNotifications)

  /* convert CheckMetrics class */
  implicit val ProbeObservationFormat = jsonFormat2(ProbeObservation)

  /* convert CheckCondition class */
  implicit val CheckConditionPageFormat = jsonFormat3(CheckConditionPage)

  /* convert CheckNotifications class */
  implicit val CheckNotificationsPageFormat = jsonFormat3(CheckNotificationsPage)

  /* convert CheckMetrics class */
  implicit val ProbeObservationPageFormat = jsonFormat3(ProbeObservationPage)
}
