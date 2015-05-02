package io.mandelbrot.persistence.cassandra

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import java.util.Date

/**
 *
 */
object EpochUtils {

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)

  /**
   * convert DateTime to our own 'epoch'.
   */
  def timestamp2epoch(timestamp: DateTime): Long = timestamp.withZone(DateTimeZone.UTC).toDateMidnight.getMillis

  /**
   * convert an epoch to a timestamp
   */
  def epoch2timestamp(epoch: Long): DateTime = new DateTime(epoch, DateTimeZone.UTC)

  /**
   * calculate the next epoch from the specified epoch.
   */
  def nextEpoch(epoch: Long): Long = new DateTime(epoch, DateTimeZone.UTC).toDateMidnight.plusDays(1).getMillis

  /**
   * calculate the previous epoch from the specified epoch.
   */
  def prevEpoch(epoch: Long): Long = new DateTime(epoch, DateTimeZone.UTC).toDateMidnight.minusDays(1).getMillis
}
