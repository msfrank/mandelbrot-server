package io.mandelbrot.core.util

import org.joda.time.{DateMidnight, DateTimeZone, DateTime}

/**
 * A timestamp with UTC timezone.
 */
trait Timestamp extends Any {
  def toDateTime: DateTime
  def toMillis: Long
  def toDateMidnight: DateMidnight
}

object Timestamp {

  /* implements the Timestamp universal trait */
  private[this] class TimestampImpl(val value: DateTime) extends AnyVal with Timestamp {
    def toDateTime: DateTime = value
    def toDateMidnight: DateMidnight = value.toDateMidnight
    def toMillis: Long = value.getMillis
  }

  /**
   *
   */
  val SMALLEST_TIMESTAMP: Timestamp = new TimestampImpl(new DateTime(0, DateTimeZone.UTC))

  /**
   *
   */
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
