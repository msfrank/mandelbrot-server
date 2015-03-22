package io.mandelbrot.core.system

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Failure, Try}

import io.mandelbrot.core.model._

case class TestProcessorSettings(properties: Map[String,String])

class TestProcessor(val properties: Map[String,String]) extends BehaviorProcessor {

  def configure(status: ProbeStatus, children: Set[ProbeRef]): ConfigEffect = {
    ConfigEffect(status, Vector.empty, children, Set.empty)
  }

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None
}

class TestBehavior extends ProbeBehaviorExtension {
  type Settings = TestProcessorSettings
  class TestProcessorFactory(val settings: TestProcessorSettings) extends DependentProcessorFactory {
    def implement() = new TestProcessor(settings.properties)
  }
  def configure(properties: Map[String, String]) = {
    new TestProcessorFactory(TestProcessorSettings(properties))
  }
}

class TestProcessorChange(val properties: Map[String,String]) extends BehaviorProcessor {

  def configure(status: ProbeStatus, children: Set[ProbeRef]): ConfigEffect = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, Some(timestamp), Some(timestamp), None, None, false)
    ConfigEffect(status, Vector.empty, children, Set.empty)
  }

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None
}

class TestChangeBehavior extends ProbeBehaviorExtension {
  type Settings = TestProcessorSettings
  class TestProcessorFactory(val settings: TestProcessorSettings) extends DependentProcessorFactory {
    def implement() = new TestProcessorChange(settings.properties)
  }
  def configure(properties: Map[String, String]) = {
    new TestProcessorFactory(TestProcessorSettings(properties))
  }
}
