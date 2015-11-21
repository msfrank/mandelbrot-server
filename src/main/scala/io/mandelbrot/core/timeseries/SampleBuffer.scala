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

import io.mandelbrot.core.model._

/**
 *
 */
private class TimeseriesSample[T](var sample: Option[T], val tick: Tick)

/**
 * A ring buffer which stores an array of samples ordered by tick.
 */
class SampleBuffer[T](initialSize: Int, initialInstant: Long, samplingRate: SamplingRate) {

  if (initialSize <= 0) throw new IllegalArgumentException()

  private var array = new Array[TimeseriesSample[T]](initialSize)
  private var curr: Int = 0

  // initialize backing array
  private def initialize(): Unit = {
    val tick = Tick(initialInstant, samplingRate)
    array.indices.reverse.foreach { index =>
      array(index) = new TimeseriesSample[T](None, tick - index)
    }
  }

  initialize()

  /**
   *
   */
  def resize(size: Int, instant: Long): Unit = { throw new NotImplementedError() }

  /**
   * return the current number of slots in the buffer
   */
  def size: Int = array.length

  /**
   * returns the Tick of the oldest slot.
   */
  def horizon: Tick = array(lookup(array.length - 1)).tick

  /**
   * returns the Tick of the newest slot.
   */
  def tip: Tick = array(lookup(0)).tick

  /**
   * insert a sample in the correct slot of the buffer.  if the timestamp is newer
   * than the most current sample, then advance the buffer as needed and insert the
   * sample at the head.
   */
  def put(timestamp: Timestamp, sample: T): Unit = {
    // if the timestamp is older than the oldest sample, then we silently drop it
    if (array(lookup(size - 1)).tick > timestamp)
      return
    val latest = array(lookup(0))
    val tick = Tick(timestamp, samplingRate)
    val span = tick.span(latest.tick)
    if (latest.tick >= timestamp) {
      // if timestamp is older than or equal to the newest sample, then we upsert
      array(lookup(span.toInt)).sample = Some(sample)
    } else {
      advance(timestamp)
      array(lookup(0)).sample = Some(sample)
    }
  }

  /**
   * advance the sample buffer but don't insert data.
   */
  def advance(timestamp: Timestamp): Tick = {
    val latest = array(lookup(0))
    if (latest.tick < timestamp) {
      val tick = Tick(timestamp, samplingRate)
      val span = tick.span(latest.tick)
      0.until(span.toInt).map(_ + 1).foreach { index =>
        array(curr) = new TimeseriesSample[T](None, latest.tick + index)
        curr = if (curr + 1 == array.length) 0 else curr + 1
      }
      array(lookup(0)).tick
    } else latest.tick
  }

  private def lookup(index: Int): Int = {
    if (index < 0 || index >= array.length) throw new IndexOutOfBoundsException()
    if (curr - index - 1 < 0)
      array.length + (curr - index - 1)
    else
      curr - index - 1
  }

  /**
   * get the value at the specified index.
   *
   * @throws NoSuchElementException if there is no element at the specified index
   * @throws IndexOutOfBoundsException if index is larger than the buffer
   */
  def apply(index: Int): T = array(lookup(index)).sample.get

  /**
   * return an Option for the value at the specified index.
   *
   * @throws IndexOutOfBoundsException if index is larger than the buffer
   */
  def get(index: Int): Option[T] = array(lookup(index)).sample

  /**
   * get the last inserted element (index == 0)
   */
  def head: T = apply(0)

  /**
   * return an Option for the last inserted element
   */
  def headOption: Option[T] = get(0)

  /**
   * fold over each element in the buffer.
   */
  def foldLeft[A](z: A)(op: (T, A) => A): A = {
    var out = z
    for (i <- array.indices) {
      get(i) match {
        case None => return out
        case Some(v) => out = op(v, out)
      }
    }
    out
  }
}

object SampleBuffer {
  def samplingRate2millis(samplingRate: SamplingRate): Long = samplingRate match {
    case PerSecond => 1000L
    case PerMinute => 60 * 1000L
    case PerFiveMinutes => 5 * 60 * 1000L
    case PerHour => 60 * 60 * 1000L
    case PerDay => 24 * 60 * 60 * 1000L
  }
}
