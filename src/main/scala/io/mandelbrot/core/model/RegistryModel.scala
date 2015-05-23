package io.mandelbrot.core.model

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration
import java.net.URI

sealed trait RegistryModel

/* a dynamic agent registration */
case class AgentRegistration(agentId: AgentId,
                             agentType: String,
                             metadata: Map[String,String],
                             checks: Map[CheckId,CheckSpec],
                             metrics: Map[CheckId,Map[String,MetricSpec]]) extends RegistryModel

/* tunable parameters which apply to all check types */
case class CheckPolicy(joiningTimeout: FiniteDuration,
                       checkTimeout: FiniteDuration,
                       alertTimeout: FiniteDuration,
                       leavingTimeout: FiniteDuration,
                       notifications: Option[Set[String]]) extends RegistryModel

/* check specification */
case class CheckSpec(checkType: String,
                     policy: CheckPolicy,
                     properties: Map[String,String],
                     metadata: Map[String,String]) extends RegistryModel

/* metric specification */
case class MetricSpec(sourceType: SourceType,
                      metricUnit: MetricUnit,
                      step: Option[FiniteDuration],
                      heartbeat: Option[FiniteDuration],
                      cf: Option[ConsolidationFunction]) extends RegistryModel

/* metadata about an agent */
case class AgentMetadata(agentId: AgentId,
                         generation: Long,
                         joinedOn: DateTime,
                         lastUpdate: DateTime,
                         expires: Option[DateTime]) extends RegistryModel

/* a page of agent metadata */
case class AgentsPage(agents: Vector[AgentMetadata], last: Option[String]) extends RegistryModel
