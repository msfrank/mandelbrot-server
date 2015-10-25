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

package io.mandelbrot.core.model

import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration.FiniteDuration

object TimeModel

/**
 * A timestamp with UTC timezone.
 */
sealed trait Timestamp extends Any with Comparable[Timestamp] {
  def toDateTime: DateTime
  def toMillis: Long
  def compareTo(other: Timestamp): Int = toMillis.compareTo(other.toMillis)
  def +(duration: FiniteDuration): Timestamp
  def -(duration: FiniteDuration): Timestamp
}

object Timestamp {

  /* implements the Timestamp universal trait */
  private[this] final class TimestampImpl(val value: DateTime) extends AnyVal with Timestamp {
    def toDateTime: DateTime = value
    def toMillis: Long = value.getMillis
    def +(duration: FiniteDuration): Timestamp = new TimestampImpl(value.plus(duration.toMillis))
    def -(duration: FiniteDuration): Timestamp = new TimestampImpl(value.minus(duration.toMillis))
  }

  val SMALLEST_DATETIME: DateTime = new DateTime(0, DateTimeZone.UTC)
  val SMALLEST_TIMESTAMP: Timestamp = new TimestampImpl(new DateTime(0, DateTimeZone.UTC))
  val LARGEST_DATETIME: DateTime = new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC)
  val LARGEST_TIMESTAMP: Timestamp = new TimestampImpl(new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC))

  /**
   * returns a new Timestamp marking the current time.
   */
  def apply(): Timestamp = new TimestampImpl(DateTime.now(DateTimeZone.UTC))

  /**
   * returns a new Timestamp marking the specified time in milliseconds since the UNIX epoch.
   */
  def apply(millis: Long): Timestamp = new TimestampImpl(new DateTime(millis, DateTimeZone.UTC))

  /**
   * returns a new Timestamp marking the specified time.
   */
  def apply(datetime: DateTime): Timestamp = new TimestampImpl(datetime.withZone(DateTimeZone.UTC))
}
