///**
// * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
// *
// * This file is part of Mandelbrot.
// *
// * Mandelbrot is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Mandelbrot is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//package io.mandelbrot.core.check
//
//import io.mandelbrot.core.parser.TimeseriesEvaluationParser
//import io.mandelbrot.core.timeseries.{TimeseriesStore, TimeseriesEvaluation}
//import org.joda.time.{DateTimeZone, DateTime}
//import scala.util.{Success, Try}
//import java.util.UUID
//
//import io.mandelbrot.core.model._
//import io.mandelbrot.core.metrics._
//
//case class TimeseriesCheckSettings(evaluation: TimeseriesEvaluation)
//
///**
// * Implements metrics check behavior.
// */
//class TimeseriesProcessor(settings: TimeseriesCheckSettings) extends BehaviorProcessor {
//
//  val evaluation = settings.evaluation
//  val timeseriesStore = new TimeseriesStore(evaluation)
//  val tick = timeseriesStore.tick()
//
//  /**
//   *
//   */
//  def initialize(checkRef: CheckRef, generation: Long): InitializeEffect = {
//    val initializers: Map[ObservationSource,CheckInitializer] = timeseriesStore.windows().map {
//      case (source: ObservationSource,window) => source -> CheckInitializer(from = Some(new DateTime(0)),
//        to = Some(Timestamp.LARGEST_TIMESTAMP.toDateTime), limit = window.size, fromInclusive = true,
//        toExclusive = false, descending = true)
//    }.toMap
//    InitializeEffect(initializers)
//  }
//
//  /**
//   *
//   */
//  def configure(checkRef: CheckRef,
//                generation: Long,
//                status: Option[CheckStatus],
//                observations: Map[ProbeId,Vector[ProbeObservation]],
//                children: Set[CheckRef]): ConfigureEffect = {
//    val timestamp = DateTime.now(DateTimeZone.UTC)
//    val initial = status match {
//      case None =>
//        CheckStatus(generation, timestamp, CheckJoining, None, CheckUnknown, Map.empty, Some(timestamp),
//          Some(timestamp), None, None, squelched = false)
//      case Some(_status) if _status.lifecycle == CheckInitializing =>
//        _status.copy(lifecycle = CheckJoining, health = CheckUnknown, summary = Some(evaluation.toString),
//          lastUpdate = Some(timestamp), lastChange = Some(timestamp))
//      case Some(_status) => _status
//    }
//    ConfigureEffect(initial, Vector.empty, Set.empty, tick)
//  }
//
//  /**
//   * if we receive a metrics message while joining or known, then update check state
//   * and send notifications.  if the previous lifecycle was joining, then we move to
//   * known.  if we transition from non-healthy to healthy, then we clear the correlation
//   * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
//   * we set the correlation if it is different from the current correlation, and we start
//   * the alert timer.
//   */
//  def processObservation(check: AccessorOps, probeId: ProbeId, observation: Observation): Option[EventEffect] = {
//    val timestamp = DateTime.now(DateTimeZone.UTC)
//    val lastUpdate = Some(timestamp)
//
//    var lifecycle = check.lifecycle
//    var health = check.health
//    var lastChange = check.lastChange
//    var correlationId = check.correlationId
//    var acknowledgementId = check.acknowledgementId
//
//    // push new metrics into the store
//    timeseriesStore.append(ObservationSource(probeId), observation)
//
//    // evaluate the store
//    health = evaluation.evaluate(timeseriesStore) match {
//      case Some(result) => if (result) CheckFailed else CheckHealthy
//      case None => CheckUnknown
//    }
//    correlationId = if (health == CheckHealthy) None else {
//      if (check.correlationId.isDefined) check.correlationId else Some(UUID.randomUUID())
//    }
//    // update lifecycle
//    if (check.lifecycle == CheckJoining) {
//      lifecycle = CheckKnown
//    }
//
//    // update last change
//    if (health != check.health) {
//      lastChange = Some(timestamp)
//    }
//
//    // we are healthy
//    if (health == CheckHealthy) {
//      correlationId = None
//      acknowledgementId = None
//    }
//
//    val status = CheckStatus(check.generation, timestamp, lifecycle, None, health, Map.empty,
//      lastUpdate, lastChange, correlationId, acknowledgementId, check.squelch)
//    var notifications = Vector.empty[CheckNotification]
//
//    // append lifecycle notification
//    if (check.lifecycle != lifecycle)
//      notifications = notifications :+ NotifyLifecycleChanges(check.checkRef, observation.timestamp, check.lifecycle, lifecycle)
//    // append health notification
//    if (health != check.health) {
//      notifications = notifications :+ NotifyHealthChanges(check.checkRef, observation.timestamp, correlationId, check.health, health)
//    } else {
//      notifications = notifications :+ NotifyHealthUpdates(check.checkRef, observation.timestamp, correlationId, health)
//    }
//    // append recovery notification
//    if (check.health == CheckHealthy && check.acknowledgementId.isDefined) {
//      notifications = notifications :+ NotifyRecovers(check.checkRef, timestamp, check.correlationId.get, check.acknowledgementId.get)
//    }
//
//    Some(EventEffect(status, notifications))
//  }
//
//  /* ignore child messages */
//  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None
//
//  /*
//   *
//   */
//  def processTick(check: AccessorOps): Option[EventEffect] = None
//}
//
//class TimeseriesCheck extends CheckBehaviorExtension {
//  type Settings = TimeseriesCheckSettings
//  class TimeseriesProcessorFactory(val settings: TimeseriesCheckSettings) extends DependentProcessorFactory {
//    def implement() = new TimeseriesProcessor(settings)
//    def observes() = settings.evaluation.sources.filter(_.scheme == "probe").map(source => ProbeId(source.id))
//  }
//  def configure(properties: Map[String,String]) = {
//    if (!properties.contains("evaluation"))
//      throw new IllegalArgumentException("missing evaluation")
//    val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation(properties("evaluation"))
//    new TimeseriesProcessorFactory(TimeseriesCheckSettings(evaluation))
//  }
//}
