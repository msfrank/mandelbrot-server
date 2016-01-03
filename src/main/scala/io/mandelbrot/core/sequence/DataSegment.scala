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

package io.mandelbrot.core.sequence

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.HdrHistogram.DoubleHistogram
import org.rocksdb.{Options, RocksDB}

import io.mandelbrot.core.model._

/**
 *
 */
class DataSegment(file: File) extends AutoCloseable {

  private val dbOptions = new Options()
  dbOptions.setCreateIfMissing(true)

  // throws RocksDBException if open() fails
  private val db = RocksDB.open(dbOptions, file.getAbsolutePath)

  private var isClosed = false

  /**
   * returns Some(histogram) if a histogram exists for the specified key, otherwise None.
   */
  def get(key: Array[Byte]): Option[DoubleHistogram] = if (!isClosed) {
    Option(db.get(key)).map { bytesRead =>
      val byteBuffer = ByteBuffer.wrap(bytesRead)
      DoubleHistogram.decodeFromByteBuffer(byteBuffer, Long.MaxValue)
    }
  } else throw new IOException("DataSegment is closed")

  /**
   * returns true if there is likely a histogram for the given key (subject to the false
   * positive probability of the bloom filter), otherwise returns false.
   */
  def probablyExists(key: Array[Byte]): Boolean = if (!isClosed) {
    val buffer = new StringBuffer(0)
    db.keyMayExist(key, buffer)
  } else throw new IOException("DataSegment is closed")

  /**
   * writes the histogram for the specified key.
   */
  def put(key: Array[Byte], histogram: DoubleHistogram): Unit = if (!isClosed) {
    val byteBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity)
    histogram.encodeIntoByteBuffer(byteBuffer)
    db.put(key, byteBuffer.array())
  } else throw new IOException("DataSegment is closed")

  /**
   * if a histogram exists for the specified key, then merge it with the specified
   * histogram and write it back.  if there is no histogram at the specified key, then
   * write the histogram.
   */
  def merge(key: Array[Byte], histogram: DoubleHistogram): Unit = if (!isClosed) {
    val merged = get(key) match {
      case None => histogram
      case Some(previous) =>
        previous.add(histogram)
        previous
    }
    put(key, merged)
  } else throw new IOException("DataSegment is closed")

  /**
   * release all held resources associated with the data segment.
   */
  def close(): Unit = if (!isClosed) {
    db.close()
    dbOptions.dispose()
    isClosed = true
  }
}

object DataSegment {
  def open(file: File): DataSegment = new DataSegment(file)
}
