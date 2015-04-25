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

  /* convert ProbeEvaluation class */
  implicit val ProbeEvaluationFormat = jsonFormat4(ProbeEvaluation)

  /* convert ProbeStatus class */
  implicit val ProbeStatusFormat = jsonFormat10(ProbeStatus)

  /* convert ProbeCondition class */
  implicit val ProbeConditionFormat = jsonFormat7(ProbeCondition)

  /* convert ProbeNotifications class */
  implicit val ProbeNotificationsFormat = jsonFormat2(ProbeNotifications)

  /* convert ProbeMetrics class */
  implicit val ProbeMetricsFormat = jsonFormat2(ProbeMetrics)

  /* convert ProbeCondition class */
  implicit val ProbeConditionPageFormat = jsonFormat3(ProbeConditionPage)

  /* convert ProbeNotifications class */
  implicit val ProbeNotificationsPageFormat = jsonFormat3(ProbeNotificationsPage)

  /* convert ProbeMetrics class */
  implicit val ProbeMetricsPageFormat = jsonFormat3(ProbeMetricsPage)
}
