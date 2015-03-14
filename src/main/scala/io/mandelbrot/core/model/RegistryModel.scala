package io.mandelbrot.core.model

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration
import java.net.URI

sealed trait RegistryModel

/* a dynamic probe system registration */
case class ProbeRegistration(systemType: String,
                             metadata: Map[String,String],
                             probes: Map[String,ProbeSpec],
                             metrics: Map[MetricSource,MetricSpec]) extends RegistryModel

/* probe tunable parameters which apply to all probe types */
case class ProbePolicy(joiningTimeout: FiniteDuration,
                       probeTimeout: FiniteDuration,
                       alertTimeout: FiniteDuration,
                       leavingTimeout: FiniteDuration,
                       notifications: Option[Set[String]]) extends RegistryModel

/* probe specification */
case class ProbeSpec(probeType: String,
                     metadata: Map[String,String],
                     policy: ProbePolicy,
                     behavior: ProbeBehavior,
                     children: Map[String,ProbeSpec]) extends RegistryModel

/* metric specification */
case class MetricSpec(sourceType: SourceType,
                      metricUnit: MetricUnit,
                      step: Option[FiniteDuration],
                      heartbeat: Option[FiniteDuration],
                      cf: Option[ConsolidationFunction]) extends RegistryModel

// FIXME
import io.mandelbrot.core.system.ProbeBehaviorInterface
import io.mandelbrot.core.system.ScalarProbeBehaviorImpl
import io.mandelbrot.core.system.AggregateProbeBehaviorImpl
import io.mandelbrot.core.system.AggregateEvaluation
import io.mandelbrot.core.system.MetricsProbeBehaviorImpl
import io.mandelbrot.core.metrics.MetricsEvaluation

/**
 *
 */
trait ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface
}

/**
 *
 */
case class ScalarProbeBehavior(flapWindow: FiniteDuration, flapDeviations: Int) extends ProbeBehavior {
  def makeProbeBehavior() = new ScalarProbeBehaviorImpl()
}

/**
 *
 */
case class AggregateProbeBehavior(evaluation: AggregateEvaluation, flapWindow: FiniteDuration, flapDeviations: Int) extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new AggregateProbeBehaviorImpl(evaluation)
}


/**
 * Contains the metrics probe behavior policy.
 */
case class MetricsProbeBehavior(evaluation: MetricsEvaluation, flapWindow: FiniteDuration, flapDeviations: Int) extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new MetricsProbeBehaviorImpl(evaluation)
}

/* */
case class ProbeSystemMetadata(uri: URI, joinedOn: DateTime, lastUpdate: DateTime) extends RegistryModel

/* */
case class ProbeSystemsPage(systems: Vector[ProbeSystemMetadata], last: Option[String]) extends RegistryModel
