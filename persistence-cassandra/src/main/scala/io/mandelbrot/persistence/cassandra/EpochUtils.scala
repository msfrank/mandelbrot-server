package io.mandelbrot.persistence.cassandra

import org.joda.time.DateTime
import java.util.Date

/**
 *
 */
object EpochUtils {

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)
  val EPOCH_TERM: Long = 60 * 60 * 24   // 1 day in seconds

  /**
   * convert milliseconds since the UNIX epoch to our own 'epoch'.
   */
  def millis2epoch(millis: Long): Long = (millis / EPOCH_TERM) * EPOCH_TERM

  /**
   * convert DateTime to our own 'epoch'.
   */
  def timestamp2epoch(timestamp: DateTime): Long = millis2epoch(timestamp.getMillis)

}
