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

package io.mandelbrot.core.check

import akka.actor.ActorRef
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.Timer

/**
 *
 */
trait AccessorOps {

  val checkRef: CheckRef
  val parent: ActorRef

  def generation: Long
  def children: Set[CheckRef]
  def policy: CheckPolicy
  def lifecycle: CheckLifecycle
  def health: CheckHealth
  def summary: Option[String]
  def lastChange: Option[DateTime]
  def lastUpdate: Option[DateTime]
  def correlationId: Option[UUID]
  def acknowledgementId: Option[UUID]
  def squelch: Boolean

  def getCheckStatus(timestamp: DateTime) = CheckStatus(generation, timestamp, lifecycle, summary,
    health, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)

  def getCheckStatus: CheckStatus = getCheckStatus(DateTime.now(DateTimeZone.UTC))
}

