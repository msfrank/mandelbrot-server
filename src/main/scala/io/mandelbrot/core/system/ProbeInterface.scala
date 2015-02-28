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

  def getProbeStatus(timestamp: DateTime) = ProbeStatus(timestamp, lifecycle, summary, health, Map.empty, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
  def getProbeStatus: ProbeStatus = getProbeStatus(DateTime.now(DateTimeZone.UTC))

  /* */
  def subscribeToMetrics(probePath: Vector[String]): Unit
  def unsubscribeFromMetrics(probePath: Vector[String]): Unit
}

/* object lifecycle */
sealed trait ProbeLifecycle
case object ProbeInitializing extends ProbeLifecycle { override def toString = "initializing" }
case object ProbeJoining extends ProbeLifecycle { override def toString = "joining" }
case object ProbeKnown extends ProbeLifecycle   { override def toString = "known" }
case object ProbeSynthetic extends ProbeLifecycle { override def toString = "synthetic" }
case object ProbeRetired extends ProbeLifecycle { override def toString = "retired" }

/* object state */
sealed trait ProbeHealth
case object ProbeHealthy extends ProbeHealth  { override def toString = "healthy" }
case object ProbeDegraded extends ProbeHealth { override def toString = "degraded" }
case object ProbeFailed extends ProbeHealth   { override def toString = "failed" }
case object ProbeUnknown extends ProbeHealth  { override def toString = "unknown" }

/* probe status submitted by the agent */
case class ProbeEvaluation(timestamp: DateTime,
                           summary: Option[String],
                           health: Option[ProbeHealth],
                           metrics: Option[Map[String,BigDecimal]])

/* the complete status of a probe */
case class ProbeStatus(timestamp: DateTime,
                       lifecycle: ProbeLifecycle,
                       summary: Option[String],
                       health: ProbeHealth,
                       metrics: Map[String,BigDecimal],
                       lastUpdate: Option[DateTime],
                       lastChange: Option[DateTime],
                       correlation: Option[UUID],
                       acknowledged: Option[UUID],
                       squelched: Boolean)

/* key-value pairs describing a probe */
case class ProbeMetadata(probeRef: ProbeRef, metadata: Map[String,String])
