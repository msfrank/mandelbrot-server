package io.mandelbrot.core.system

import org.joda.time.{DateTimeZone, DateTime}

case class TestBehavior() extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new TestBehaviorImpl()
}

class TestBehaviorImpl() extends ProbeBehaviorInterface {

  def enter(probe: ProbeInterface): Option[EventMutation] = None

  def processChild(probe: ProbeInterface, message: ProbeStatus): Option[EventMutation] = None

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventMutation] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventMutation] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventMutation] = None

  def processMetrics(probe: ProbeInterface, message: MetricsMessage): Option[EventMutation] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventMutation] = None

  def exit(probe: ProbeInterface): Option[EventMutation] = None

  def processStatus(probe: ProbeInterface, message: StatusMessage): Option[EventMutation] = None
}

case class TestChangeBehavior() extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new TestChangeBehaviorImpl()
}

class TestChangeBehaviorImpl() extends ProbeBehaviorInterface {

  def enter(probe: ProbeInterface): Option[EventMutation] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = ProbeStatus(probe.probeRef, timestamp, ProbeKnown, ProbeHealthy, None, Some(timestamp), Some(timestamp), None, None, false)
    Some(EventMutation(status, Vector.empty))
  }

  def processStatus(probe: ProbeInterface, message: StatusMessage): Option[EventMutation] = None

  def processMetrics(probe: ProbeInterface, message: MetricsMessage): Option[EventMutation] = None

  def processChild(probe: ProbeInterface, message: ProbeStatus): Option[EventMutation] = None

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventMutation] = None

  def processExpiryTimeout(probe: ProbeInterface): Option[EventMutation] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventMutation] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventMutation] = None

  def exit(probe: ProbeInterface): Option[EventMutation] = None

}

case class TestUpdateBehavior(param: Int) extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new TestUpdateBehaviorImpl()
}

class TestUpdateBehaviorImpl() extends ProbeBehaviorInterface {

  def enter(probe: ProbeInterface): Option[EventMutation] = None

  def processStatus(probe: ProbeInterface, message: StatusMessage): Option[EventMutation] = None

  def processMetrics(probe: ProbeInterface, message: MetricsMessage): Option[EventMutation] = None

  def processChild(probe: ProbeInterface, message: ProbeStatus): Option[EventMutation] = None

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventMutation] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = ProbeStatus(probe.probeRef, timestamp, ProbeKnown, ProbeHealthy, None, Some(timestamp), Some(timestamp), None, None, false)
    Some(EventMutation(status, Vector.empty))
  }

  def processExpiryTimeout(probe: ProbeInterface): Option[EventMutation] = None

  def retire(probe: ProbeInterface, lsn: Long): Option[EventMutation] = None

  def processAlertTimeout(probe: ProbeInterface): Option[EventMutation] = None

  def exit(probe: ProbeInterface): Option[EventMutation] = None

}
