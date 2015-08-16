package io.mandelbrot.core.check

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Failure, Try}

import io.mandelbrot.core.model._

case class TestProcessorSettings(properties: Map[String,String])

class TestProcessor(val properties: Map[String,String]) extends BehaviorProcessor {

  def initialize(check: AccessorOps): InitializeEffect = InitializeEffect(Map.empty)

  def configure(check: AccessorOps, results: Map[CheckId,Vector[CheckStatus]], children: Set[CheckRef]): ConfigureEffect = {
    ConfigureEffect(check.getCheckStatus, Vector.empty, children)
  }

  def processEvaluation(check: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None

  def processExpiryTimeout(check: AccessorOps): Option[EventEffect] = None

  def processAlertTimeout(check: AccessorOps): Option[EventEffect] = None
}

class TestBehavior extends CheckBehaviorExtension {
  type Settings = TestProcessorSettings
  class TestProcessorFactory(val settings: TestProcessorSettings) extends DependentProcessorFactory {
    def implement() = new TestProcessor(settings.properties)
  }
  def configure(properties: Map[String, String]) = {
    new TestProcessorFactory(TestProcessorSettings(properties))
  }
}

class TestProcessorChange(val properties: Map[String,String]) extends BehaviorProcessor {

  def initialize(check: AccessorOps): InitializeEffect = InitializeEffect(Map.empty)

  def configure(check: AccessorOps, results: Map[CheckId,Vector[CheckStatus]], children: Set[CheckRef]): ConfigureEffect = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val _status = CheckStatus(check.generation, timestamp, CheckKnown, None, CheckHealthy,
      Map.empty, Some(timestamp), Some(timestamp), None, None, false)
    ConfigureEffect(_status, Vector.empty, children)
  }

  def processEvaluation(check: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None

  def processExpiryTimeout(check: AccessorOps): Option[EventEffect] = None

  def processAlertTimeout(check: AccessorOps): Option[EventEffect] = None
}

class TestChangeBehavior extends CheckBehaviorExtension {
  type Settings = TestProcessorSettings
  class TestProcessorFactory(val settings: TestProcessorSettings) extends DependentProcessorFactory {
    def implement() = new TestProcessorChange(settings.properties)
  }
  def configure(properties: Map[String, String]) = {
    new TestProcessorFactory(TestProcessorSettings(properties))
  }
}
