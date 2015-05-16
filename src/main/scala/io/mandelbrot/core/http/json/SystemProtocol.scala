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
import io.mandelbrot.core.parser.CheckMatcherParser

/**
 *
 */
trait SystemProtocol extends DefaultJsonProtocol with ConstantsProtocol with StateProtocol {

  /* convert CheckMatcher class */
  implicit object CheckMatcherFormat extends JsonFormat[CheckMatcher] {
    def write(matcher: CheckMatcher) = JsString(matcher.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => CheckMatcherParser.parseCheckMatcher(string)
      case _ => throw new DeserializationException("expected CheckMatcher")
    }
  }

  /* check operations */
  implicit val GetCheckConditionFormat = jsonFormat8(GetCheckCondition)
  implicit val GetCheckNotificationsFormat = jsonFormat8(GetCheckNotifications)
  implicit val GetCheckMetricsFormat = jsonFormat8(GetCheckMetrics)
  implicit val AcknowledgeCheckFormat = jsonFormat2(AcknowledgeCheck)
  implicit val UnacknowledgeCheckFormat = jsonFormat2(UnacknowledgeCheck)
  implicit val SetCheckSquelchFormat = jsonFormat2(SetCheckSquelch)

  /* check results */
  implicit val AcknowledgeCheckResultFormat = jsonFormat2(AcknowledgeCheckResult)
  implicit val UnacknowledgeCheckResultFormat = jsonFormat2(UnacknowledgeCheckResult)
  implicit val SetCheckSquelchResultFormat = jsonFormat2(SetCheckSquelchResult)
}
