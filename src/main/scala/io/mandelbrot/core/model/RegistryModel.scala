package io.mandelbrot.core.model

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration

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
case class AgentsPage(agents: Vector[AgentMetadata], last: Option[String], exhausted: Boolean) extends RegistryModel

/* a page of agent metadata */
case class RegistrationsPage(agents: Vector[AgentRegistration], last: Option[String], exhausted: Boolean) extends RegistryModel

/* a page of group names */
case class GroupsPage(groups: Vector[String], last: Option[String], exhausted: Boolean) extends RegistryModel

/* convenience class containing the agent generation and lsn */
case class GenerationLsn(generation: Long, lsn: Long) extends Ordered[GenerationLsn] with RegistryModel {
  def compare(other: GenerationLsn): Int = {
    import scala.math.Ordered.orderingToOrdered
    (generation, lsn).compare((other.generation, other.lsn))
  }
}
/* a marker to delete agent registrations for a particular generation */
case class AgentTombstone(expires: DateTime, agentId: AgentId, generation: Long)
  extends Comparable[AgentTombstone]
  with Ordered[AgentTombstone]
  with RegistryModel {
  override def compare(other: AgentTombstone): Int = {
    import scala.math.Ordered.orderingToOrdered
    (expires.getMillis, agentId.toString, generation)
      .compare((other.expires.getMillis, other.agentId.toString, other.generation))
  }
  override def compareTo(other: AgentTombstone): Int = compare(other)
}
