/**
 * Copyright 2015 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.timeseries

import org.joda.time.{DateTime, DateTimeZone}
import scala.concurrent.duration.FiniteDuration

import io.mandelbrot.core.model._

/**
 *
 */
sealed abstract class Tick(timeInMillis: Long, val interval: Long) extends Ordered[Timestamp] {
  import Tick.{SMALLEST_TIME_IN_MILLIS,LARGEST_TIME_IN_MILLIS}

  if (timeInMillis <= SMALLEST_TIME_IN_MILLIS || timeInMillis > LARGEST_TIME_IN_MILLIS)
    throw new IllegalStateException()
  val instant = (timeInMillis / interval) * interval

  def span(tick: Tick): Long = math.abs(instant - tick.instant) / interval
  def contains(that: Long): Boolean = instant >= that && that < instant + interval
  def contains(dateTime: DateTime): Boolean = contains(dateTime.toDateTime(DateTimeZone.UTC).getMillis)
  def contains(timestamp: Timestamp): Boolean = contains(timestamp.toMillis)
  def compare(timestamp: Timestamp): Int = if (contains(timestamp)) 0 else {
    if (timestamp.toMillis - instant < 0) -1 else 1
  }

  def toTimestamp: Timestamp = Timestamp(instant)

  def -(duration: FiniteDuration): Tick
  def -(ticks: Long): Tick
  def +(duration: FiniteDuration): Tick
  def +(ticks: Long): Tick
}

object Tick {

  val SMALLEST_TIME_IN_MILLIS: Long = 0L              // the unix epoch
  val LARGEST_TIME_IN_MILLIS: Long = 32503680000000L  // the year 3000, in millis since the unix epoch

  def apply(timeInMillis: Long, samplingRate: SamplingRate): Tick = samplingRate match {
    case PerSecond => new SecondTick(timeInMillis)
    case PerMinute => new MinuteTick(timeInMillis)
    case PerFiveMinutes => new FiveMinuteTick(timeInMillis)
    case PerHour => new HourTick(timeInMillis)
    case PerDay => new DayTick(timeInMillis)
  }

  def apply(dateTime: DateTime, samplingRate: SamplingRate): Tick = {
    apply(dateTime.toDateTime(DateTimeZone.UTC).getMillis, samplingRate)
  }

  def apply(timestamp: Timestamp, samplingRate: SamplingRate): Tick = apply(timestamp.toMillis, samplingRate)
}

class SecondTick(timeInMillis: Long) extends Tick(timeInMillis, interval = 1000L) {
  def -(duration: FiniteDuration): Tick = new SecondTick(instant - duration.toMillis)
  def -(ticks: Long): Tick = new SecondTick(instant - (interval * ticks))
  def +(duration: FiniteDuration): Tick = new SecondTick(instant + duration.toMillis)
  def +(ticks: Long): Tick = new SecondTick(instant + (interval * ticks))
  def +(tick: Tick): Long = (instant - tick.instant) / interval
}

class MinuteTick(timeInMillis: Long) extends Tick(timeInMillis, interval = 60 * 1000L) {
  def -(duration: FiniteDuration): Tick = new MinuteTick(instant - duration.toMillis)
  def -(ticks: Long): Tick = new MinuteTick(instant - (interval * ticks))
  def +(duration: FiniteDuration): Tick = new MinuteTick(instant + duration.toMillis)
  def +(ticks: Long): Tick = new MinuteTick(instant + (interval * ticks))
}
class FiveMinuteTick(timeInMillis: Long) extends Tick(timeInMillis, interval = 5 * 60 * 1000L) {
  def -(duration: FiniteDuration): Tick = new FiveMinuteTick(instant - duration.toMillis)
  def -(ticks: Long): Tick = new FiveMinuteTick(instant - (interval * ticks))
  def +(duration: FiniteDuration): Tick = new FiveMinuteTick(instant + duration.toMillis)
  def +(ticks: Long): Tick = new FiveMinuteTick(instant + (interval * ticks))
}
class HourTick(timeInMillis: Long) extends Tick(timeInMillis, interval = 60 * 60 * 1000L) {
  def -(duration: FiniteDuration): Tick = new HourTick(instant - duration.toMillis)
  def -(ticks: Long): Tick = new HourTick(instant - (interval * ticks))
  def +(duration: FiniteDuration): Tick = new HourTick(instant + duration.toMillis)
  def +(ticks: Long): Tick = new HourTick(instant + (interval * ticks))
}
class DayTick(timeInMillis: Long) extends Tick(timeInMillis, interval = 24 * 60 * 60 * 1000L) {
  def -(duration: FiniteDuration): Tick = new DayTick(instant - duration.toMillis)
  def -(ticks: Long): Tick = new DayTick(instant - (interval * ticks))
  def +(duration: FiniteDuration): Tick = new DayTick(instant + duration.toMillis)
  def +(ticks: Long): Tick = new DayTick(instant + (interval * ticks))
}
