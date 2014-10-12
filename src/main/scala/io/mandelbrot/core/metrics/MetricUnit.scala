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

package io.mandelbrot.core.metrics

sealed trait MetricUnit {
  def name: String
}

/* no unit specified */
case object Units extends MetricUnit   { val name = "units" }
case object Ops extends MetricUnit     { val name = "operations" }
case object Percent extends MetricUnit { val name = "percent" }

/* time units */
case object Years extends MetricUnit    { val name = "years" }
case object Months extends MetricUnit   { val name = "months" }
case object Weeks extends MetricUnit    { val name = "weeks" }
case object Days extends MetricUnit     { val name = "days" }
case object Hours extends MetricUnit    { val name = "hours" }
case object Minutes extends MetricUnit  { val name = "minutes" }
case object Seconds extends MetricUnit  { val name = "seconds" }
case object Millis extends MetricUnit   { val name = "milliseconds" }
case object Micros extends MetricUnit   { val name = "microseconds" }

/* size units */
case object Bytes extends MetricUnit      { val name = "bytes" }
case object KiloBytes extends MetricUnit  { val name = "kilobytes" }
case object MegaBytes extends MetricUnit  { val name = "megabytes" }
case object GigaBytes extends MetricUnit  { val name = "gigabytes" }
case object TeraBytes extends MetricUnit  { val name = "terabytes" }
case object PetaBytes extends MetricUnit  { val name = "petabytes" }
