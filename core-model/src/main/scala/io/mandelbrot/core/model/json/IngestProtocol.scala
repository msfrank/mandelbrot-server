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

import io.mandelbrot.core.model._

/**
 *
 */
trait IngestProtocol extends DefaultJsonProtocol with ConstantsProtocol with ResourceProtocol {

  /* convert DataPoint class */
  implicit val DataPointFormat = jsonFormat3(DataPoint)

  /* convert ScalarMapObservation class */
  implicit val ScalarMapObservationFormat = jsonFormat4(ScalarMapObservation)

  /* convert VectorObservation class */
  implicit val VectorObservationFormat = jsonFormat5(VectorObservation)

  /* convert subclasses of Observation */
  implicit object ObservationFormat extends RootJsonFormat[Observation] {
    def write(observation: Observation) = observation match {
      case scalarMapObservation: ScalarMapObservation => scalarMapObservation.toJson
      case vectorObservation: VectorObservation => vectorObservation.toJson
      case _ => throw new SerializationException("expected Observation")
    }
    def read(value: JsValue): Observation = value match {
      case JsObject(fields) if fields.contains("scalarMap") =>
        value.convertTo[ScalarMapObservation]
      case JsObject(fields) if fields.contains("vector") =>
        value.convertTo[VectorObservation]
      case _ => throw new DeserializationException("expected Observation")
    }
  }

  /* convert ProbeObservation class */
  implicit val ProbeObservationFormat = jsonFormat2(ProbeObservation)

  /* convert ProbeObservationPage class */
  implicit val ProbeObservationPageFormat = jsonFormat3(ProbeObservationPage)
}
