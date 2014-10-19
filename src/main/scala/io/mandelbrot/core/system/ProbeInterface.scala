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

package io.mandelbrot.core.system

import akka.actor.ActorRef
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.util.Timer

/**
 *
 */
trait ProbeInterface {

  val probeRef: ProbeRef
  val parent: ActorRef
  val probeGeneration: Long
  val expiryTimer: Timer
  val alertTimer: Timer

  def children: Set[ProbeRef]
  def policy: ProbePolicy
  def lifecycle: ProbeLifecycle
  def health: ProbeHealth
  def summary: Option[String]
  def lastChange: Option[DateTime]
  def lastUpdate: Option[DateTime]
  def correlationId: Option[UUID]
  def acknowledgementId: Option[UUID]
  def squelch: Boolean

  def getProbeStatus(timestamp: DateTime) = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
  def getProbeStatus: ProbeStatus = getProbeStatus(DateTime.now(DateTimeZone.UTC))

  /* */
  def subscribeToMetrics(probePath: Vector[String]): Unit
  def unsubscribeFromMetrics(probePath: Vector[String]): Unit
}
