package io.mandelbrot.core.system

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Failure, Try}

import io.mandelbrot.core.model._

class TestProcessor extends BehaviorProcessor {

  def enter(probe: ProbeInterface): Option[EventEffect] = None

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def update(probe: ProbeInterface, processor: BehaviorProcessor): Option[EventEffect] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def exit(probe: ProbeInterface): Option[EventEffect] = None

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())
}

class TestBehavior extends ProbeBehaviorExtension {
  override def implement(properties: Map[String, String]): BehaviorProcessor = new TestProcessor
}

class TestProcessorChange() extends BehaviorProcessor {

  def enter(probe: ProbeInterface): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, Some(timestamp), Some(timestamp), None, None, false)
    Some(EventEffect(status, Vector.empty))
  }

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def update(probe: ProbeInterface, processor: BehaviorProcessor): Option[EventEffect] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def exit(probe: ProbeInterface): Option[EventEffect] = None

}

class TestChangeBehavior extends ProbeBehaviorExtension {
  override def implement(properties: Map[String, String]): BehaviorProcessor = new TestProcessorChange
}

class TestProcessorUpdate(var properties: Map[String,String]) extends BehaviorProcessor {

  def enter(probe: ProbeInterface): Option[EventEffect] = None

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def update(probe: ProbeInterface, processor: BehaviorProcessor): Option[EventEffect] = {
    processor match {
      case update: TestProcessorUpdate =>
        properties = update.properties
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val status = ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, Some(timestamp), Some(timestamp), None, None, false)
        Some(EventEffect(status, Vector.empty))
      case _ => throw new IllegalArgumentException()
    }
  }

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def exit(probe: ProbeInterface): Option[EventEffect] = None

}

class TestUpdateBehavior extends ProbeBehaviorExtension {
  override def implement(properties: Map[String, String]): BehaviorProcessor = new TestProcessorUpdate(properties)
}
