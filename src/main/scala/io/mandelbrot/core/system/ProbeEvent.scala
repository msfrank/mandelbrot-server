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

package io.mandelbrot.core.system

import org.joda.time.DateTime
import spray.json._
import scala.math.BigDecimal

sealed trait ProbeEvent extends ProbeOperation
case class StatusMessage(probeRef: ProbeRef, health: ProbeHealth, summary: String, detail: Option[String], timestamp: DateTime) extends ProbeEvent
case class MetricsMessage(probeRef: ProbeRef, metrics: Map[String,BigDecimal], timestamp: DateTime) extends ProbeEvent