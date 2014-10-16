package io.mandelbrot.core.system

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
