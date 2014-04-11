package io.mandelbrot.core.registry

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class FlapQueue(cycles: Int, window: FiniteDuration) {

  private var changes: Vector[DateTime] = Vector.empty

  def push(change: DateTime): Unit = {

  }

  def isFlapping: Boolean = {
    false
  }

}
