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
import io.mandelbrot.core.system._

/**
 *
 */
trait SystemProtocol extends DefaultJsonProtocol with BasicProtocol with StateProtocol {

  /* convert ProbeMatcher class */
  implicit object ProbeMatcherFormat extends RootJsonFormat[ProbeMatcher] {
    val probeMatcherParser = new ProbeMatcherParser()
    def write(matcher: ProbeMatcher) = JsString(matcher.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => probeMatcherParser.parseProbeMatcher(string)
      case _ => throw new DeserializationException("expected ProbeMatcher")
    }
  }

  /* probe operations */
  implicit val GetProbeConditionFormat = jsonFormat5(GetProbeCondition)
  implicit val GetProbeNotificationsFormat = jsonFormat5(GetProbeNotifications)
  implicit val GetProbeMetricsFormat = jsonFormat5(GetProbeMetrics)
  implicit val AcknowledgeProbeFormat = jsonFormat2(AcknowledgeProbe)
  implicit val UnacknowledgeProbeFormat = jsonFormat2(UnacknowledgeProbe)
  implicit val SetProbeSquelchFormat = jsonFormat2(SetProbeSquelch)

  /* probe results */
  implicit val AcknowledgeProbeResultFormat = jsonFormat2(AcknowledgeProbeResult)
  implicit val UnacknowledgeProbeResultFormat = jsonFormat2(UnacknowledgeProbeResult)
  implicit val SetProbeSquelchResultFormat = jsonFormat2(SetProbeSquelchResult)
}
