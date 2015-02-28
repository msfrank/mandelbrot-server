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

package io.mandelbrot.core.history

import akka.actor._
import io.mandelbrot.core.system.ProbeRef
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core._
import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.system.ProbeStatus

/**
 *
 */
class HistoryManager(settings: HistorySettings) extends Actor with ActorLogging {

  def receive = {
    case _ =>
  }
}

object HistoryManager {
  def props(settings: HistorySettings) = Props(classOf[HistoryManager], settings)
}

/* */
sealed trait HistoryServiceOperation extends ServiceOperation
sealed trait HistoryServiceCommand extends ServiceCommand with HistoryServiceOperation
sealed trait HistoryServiceQuery extends ServiceQuery with HistoryServiceOperation
sealed trait HistoryServiceEvent extends ServiceEvent with HistoryServiceOperation
case class HistoryServiceOperationFailed(op: HistoryServiceOperation, failure: Throwable) extends ServiceOperationFailed

/* marker trait for Archiver implementations */
trait Archiver
