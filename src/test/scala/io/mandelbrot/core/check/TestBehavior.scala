//package io.mandelbrot.core.check
//
//import org.joda.time.{DateTimeZone, DateTime}
//import scala.concurrent.duration._
//
//import io.mandelbrot.core.model._
//
//case class TestProcessorSettings(properties: Map[String,String])
//
//class TestProcessor(val properties: Map[String,String]) extends BehaviorProcessor {
//
//  def initialize(checkRef: CheckRef, generation: Long): InitializeEffect = InitializeEffect(Map.empty)
//
//  def configure(checkRef: CheckRef,
//                generation: Long,
//                status: Option[CheckStatus],
//                observations: Map[ProbeId, Vector[ProbeObservation]],
//                children: Set[CheckRef]): ConfigureEffect = {
//    val timestamp = DateTime.now(DateTimeZone.UTC)
//    val initial = status.getOrElse(CheckStatus(generation, timestamp, CheckKnown, None, CheckUnknown, Map.empty, Some(timestamp),
//      Some(timestamp), None, None, squelched = false))
//    ConfigureEffect(initial, Vector.empty, children, 1.minute)
//  }
//
//  def processObservation(check: AccessorOps, probeId: ProbeId, observation: Observation): Option[EventEffect] = None
//
//  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None
//
//  def processTick(check: AccessorOps): Option[EventEffect] = None
//
//  def processMetric(check: AccessorOps, metric: ProbeMetrics): Option[EventEffect] = None
//
//  def processStatus(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None
//}
//
//class TestBehavior extends CheckBehaviorExtension {
//  type Settings = TestProcessorSettings
//  class TestProcessorFactory(val settings: TestProcessorSettings) extends DependentProcessorFactory {
//    def implement() = new TestProcessor(settings.properties)
//    def observes(): Set[ProbeId] = Set.empty
//  }
//  def configure(properties: Map[String, String]) = {
//    new TestProcessorFactory(TestProcessorSettings(properties))
//  }
//}
//
//class TestProcessorChange(val properties: Map[String,String]) extends BehaviorProcessor {
//
//  def initialize(checkRef: CheckRef, generation: Long): InitializeEffect = InitializeEffect(Map.empty)
//
//  def configure(checkRef: CheckRef,
//                generation: Long,
//                status: Option[CheckStatus],
//                observations: Map[ProbeId, Vector[ProbeObservation]],
//                children: Set[CheckRef]): ConfigureEffect = {
//    val timestamp = DateTime.now(DateTimeZone.UTC)
//    val initial = status.getOrElse(CheckStatus(generation, timestamp, CheckKnown, None, CheckUnknown, Map.empty, Some(timestamp),
//      Some(timestamp), None, None, squelched = false))
//    ConfigureEffect(initial, Vector.empty, children, 1.minute)
//  }
//
//  def processObservation(check: AccessorOps, probeId: ProbeId, observation: Observation): Option[EventEffect] = None
//
//  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None
//
//  def processTick(check: AccessorOps): Option[EventEffect] = None
//
//  def processMetric(check: AccessorOps, metric: ProbeMetrics): Option[EventEffect] = None
//
//  def processStatus(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None
//}
//
//class TestChangeBehavior extends CheckBehaviorExtension {
//  type Settings = TestProcessorSettings
//  class TestProcessorFactory(val settings: TestProcessorSettings) extends DependentProcessorFactory {
//    def implement() = new TestProcessorChange(settings.properties)
//    def observes(): Set[ProbeId] = Set.empty
//  }
//  def configure(properties: Map[String, String]) = {
//    new TestProcessorFactory(TestProcessorSettings(properties))
//  }
//}
