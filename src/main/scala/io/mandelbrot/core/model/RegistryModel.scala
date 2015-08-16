package io.mandelbrot.core.model

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration

sealed trait RegistryModel

/* tunable parameters which apply to the agent */
case class AgentPolicy(retentionPeriod: FiniteDuration) extends RegistryModel

/* agent specification */
case class AgentSpec(agentId: AgentId,
                     agentType: String,
                     policy: AgentPolicy,
                     probes: Map[String,ProbeSpec],
                     checks: Map[CheckId,CheckSpec],
                     groups: Set[String] = Set.empty,
                     metadata: Map[String,String] = Map.empty) extends RegistryModel

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
                     metadata: Map[String,String] = Map.empty) extends RegistryModel

/* */
case class ProbePolicy(samplingPeriod: FiniteDuration)

/* probe specification */
case class ProbeSpec(policy: ProbePolicy,
                     metrics: Map[String,MetricSpec],
                     constants: Map[String,BigDecimal] = Map.empty,
                     metadata: Map[String,String] = Map.empty) extends RegistryModel

/* metric specification */
case class MetricSpec(sourceType: SourceType, metricUnit: MetricUnit) extends RegistryModel

/* metadata about an agent */
case class AgentMetadata(agentId: AgentId,
                         generation: Long,
                         joinedOn: DateTime,
                         lastUpdate: DateTime,
                         expires: Option[DateTime]) extends RegistryModel

/* */
case class AgentRegistration(spec: AgentSpec, metadata: AgentMetadata, lsn: Long, committed: Boolean) extends RegistryModel

/* a page of agent metadata */
case class MetadataPage(metadata: Vector[AgentMetadata], last: Option[String], exhausted: Boolean) extends RegistryModel

/* a page of agent metadata */
case class RegistrationsPage(agents: Vector[AgentSpec], last: Option[String], exhausted: Boolean) extends RegistryModel

/* a page of group names */
case class GroupsPage(groups: Vector[String], last: Option[String], exhausted: Boolean) extends RegistryModel

/* convenience class containing the agent generation and lsn */
case class GenerationLsn(generation: Long, lsn: Long) extends Ordered[GenerationLsn] with RegistryModel {
  import scala.math.Ordered.orderingToOrdered
  def compare(other: GenerationLsn): Int = {
    (generation, lsn).compare((other.generation, other.lsn))
  }
}
/* a marker to delete an agent with a particular generation */
case class Tombstone(expires: DateTime, agentId: AgentId, generation: Long) extends Ordered[Tombstone] with RegistryModel {
  import scala.math.Ordered.orderingToOrdered
  override def compare(other: Tombstone): Int = {
    (expires.getMillis, agentId.toString, generation)
      .compare((other.expires.getMillis, other.agentId.toString, other.generation))
  }
}
