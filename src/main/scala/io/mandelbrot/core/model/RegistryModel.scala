package io.mandelbrot.core.model

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration
import java.net.URI

sealed trait RegistryModel

/* a dynamic probe system registration */
case class AgentRegistration(agentId: Resource,
                             agentType: String,
                             metadata: Map[String,String],
                             probes: Map[String,CheckSpec],
                             metrics: Map[MetricSource,MetricSpec]) extends RegistryModel

/* probe tunable parameters which apply to all probe types */
case class CheckPolicy(joiningTimeout: FiniteDuration,
                       probeTimeout: FiniteDuration,
                       alertTimeout: FiniteDuration,
                       leavingTimeout: FiniteDuration,
                       notifications: Option[Set[String]]) extends RegistryModel

/* probe specification */
case class CheckSpec(probeType: String,
                     policy: CheckPolicy,
                     properties: Map[String,String],
                     metadata: Map[String,String],
                     children: Map[String,CheckSpec]) extends RegistryModel

/* metric specification */
case class MetricSpec(sourceType: SourceType,
                      metricUnit: MetricUnit,
                      step: Option[FiniteDuration],
                      heartbeat: Option[FiniteDuration],
                      cf: Option[ConsolidationFunction]) extends RegistryModel

/* */
case class AgentMetadata(uri: URI, joinedOn: DateTime, lastUpdate: DateTime) extends RegistryModel

/* */
case class AgentsPage(systems: Vector[AgentMetadata], last: Option[String]) extends RegistryModel
