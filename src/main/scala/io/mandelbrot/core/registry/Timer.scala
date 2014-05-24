/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.registry

import akka.actor.{ActorContext, Cancellable, ActorRef}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class Timer(context: ActorContext, receiver: ActorRef, message: Any) {
  import context.dispatcher
  private var timer: Option[Cancellable] = None
  private var lastTimeout: Option[FiniteDuration] = None
  private var lastArmed: Option[DateTime] = None

  /**
   * if timer is not running, then starts the timer, otherwise does nothing.
   */
  def start(timeout: FiniteDuration): Unit = if (timer.isEmpty) {
    timer = Some(context.system.scheduler.scheduleOnce(timeout, receiver, message))
    lastTimeout = Some(timeout)
    lastArmed = Some(DateTime.now(DateTimeZone.UTC))
  }

  /**
   * if the timer is running, then stops the timer, otherwise does nothing.
   */
  def stop(): Unit = {
    for (current <- timer)
      current.cancel()
    timer = None
    lastArmed = None
  }

  /**
   * returns true if the timer is running, otherwise false.
   */
  def isRunning: Boolean = timer.isDefined

  /**
   * shorthand to stop and start the timer.
   */
  def restart(timeout: FiniteDuration): Unit = {
    stop()
    start(timeout)
  }

  /**
   * first stops the current timer, if it is running.  then, checks the duration between now
   * and the last timeout, and if it is larger than the new specified timeout, it sends the
   * message to the receiver.  finally, it starts the timer with the new specified timeout.
   */
  def reset(timeout: FiniteDuration): Unit = {
    val now = DateTime.now(DateTimeZone.UTC)
    for (current <- timer)
      current.cancel()
    timer = None
    if (lastTimeout.isDefined && (now.getMillis - lastArmed.get.getMillis) > timeout.toMillis)
      receiver ! message
    start(timeout)
  }
}
