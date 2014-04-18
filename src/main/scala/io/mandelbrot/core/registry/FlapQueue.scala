package io.mandelbrot.core.registry

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class FlapQueue(cycles: Int, window: FiniteDuration) {

  private val changes: Array[DateTime] = new Array[DateTime](cycles)
  private var index = 0

  // initialize array to all nulls
  for (n <- 0.until(index))
    changes.update(n, null)

  def push(change: DateTime): Unit = {
    if (index == cycles)
      index = 0
    changes(index) = change
    index = index + 1
  }

  def isFlapping: Boolean = false

  def flapStart: DateTime = changes.head
}
