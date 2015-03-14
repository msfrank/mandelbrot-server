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

import io.mandelbrot.core.model._
import spray.json._

/**
 *
 */
trait MetricsProtocol extends DefaultJsonProtocol {

  /* convert ConsolidationFunction class */
  implicit object ConsolidationFunctionFormat extends RootJsonFormat[ConsolidationFunction] {
    def write(function: ConsolidationFunction) = JsString(function.name)
    def read(value: JsValue) = value match {
      case JsString("last") => ConsolidateLast
      case JsString("first") => ConsolidateFirst
      case JsString("min") => ConsolidateMin
      case JsString("max") => ConsolidateMax
      case JsString("mean") => ConsolidateMean
      case _ => throw new DeserializationException("expected ConsolidationFunction")
    }
  }
}
