package io.mandelbrot.core.system

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Failure, Try}

import io.mandelbrot.core.model._

case class TestBehavior() extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new TestBehaviorImpl()
}

class TestBehaviorImpl() extends ProbeBehaviorInterface {

  def enter(probe: ProbeInterface): Option[EventEffect] = None

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventEffect] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def exit(probe: ProbeInterface): Option[EventEffect] = None

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())
}

case class TestChangeBehavior() extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new TestChangeBehaviorImpl()
}

class TestChangeBehaviorImpl() extends ProbeBehaviorInterface {

  def enter(probe: ProbeInterface): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, Some(timestamp), Some(timestamp), None, None, false)
    Some(EventEffect(status, Vector.empty))
  }

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventEffect] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def exit(probe: ProbeInterface): Option[EventEffect] = None

}

case class TestUpdateBehavior(param: Int) extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new TestUpdateBehaviorImpl()
}

class TestUpdateBehaviorImpl() extends ProbeBehaviorInterface {

  def enter(probe: ProbeInterface): Option[EventEffect] = None

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(new NotImplementedError())

  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, Some(timestamp), Some(timestamp), None, None, false)
    Some(EventEffect(status, Vector.empty))
  }

  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = None

  def exit(probe: ProbeInterface): Option[EventEffect] = None

}
