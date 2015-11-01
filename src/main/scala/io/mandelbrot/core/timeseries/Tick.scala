package io.mandelbrot.core.timeseries

import io.mandelbrot.core.model.Timestamp
import org.joda.time.{DateTime, DateTimeZone}

/**
 *
 */
class Tick(val value: Long) extends AnyVal {
  def toLong: Long = value
  def prevTick: Tick = new Tick(value - Tick.RESOLUTION)
  def nextTick: Tick = new Tick(value + Tick.RESOLUTION)

  def contains(that: Long): Boolean = {
    value >= that && that < value + Tick.RESOLUTION
  }

  def contains(dateTime: DateTime): Boolean = contains(dateTime.toDateTime(DateTimeZone.UTC).getMillis)

  def contains(timestamp: Timestamp): Boolean = contains(timestamp.toMillis)
}

object Tick {
  val SMALLEST_TICK: Long = 0L              // the unix epoch
  val LARGEST_TICK: Long = 32503680000000L  // the year 3000, in millis since the unix epoch
  val RESOLUTION = 60000                    // one minute, in milliseconds

  def apply(value: Long): Tick = {
    if (value <= SMALLEST_TICK || value > LARGEST_TICK) throw new IllegalStateException()
    new Tick(value % Tick.RESOLUTION)
  }

  /**
   *
   */
  def apply(dateTime: DateTime): Tick = apply(dateTime.toDateTime(DateTimeZone.UTC).getMillis)

  /**
   *
   */
  def apply(timestamp: Timestamp): Tick = apply(timestamp.toMillis)
}
