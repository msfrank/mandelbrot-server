package io.mandelbrot.persistence.cassandra

import akka.actor.{OneForOneStrategy, Props, ActorLogging, Actor}
import akka.pattern.pipe
import akka.actor.SupervisorStrategy.{Stop,Restart}
import com.typesafe.config.Config
import com.datastax.driver.core.exceptions._
import io.mandelbrot.core.model.{RegistrationsPage, AgentId}
import io.mandelbrot.core.{NotImplemented, ApiException}

import io.mandelbrot.core.registry._
import io.mandelbrot.persistence.cassandra.dal.{AgentGroupDAL, AgentTombstoneDAL, AgentRegistrationDAL}
import io.mandelbrot.persistence.cassandra.task.{DescribeGroupTask, GetAgentRegistrationHistoryTask}

import scala.util.hashing.MurmurHash3

/**
 *
 */
class CassandraRegistryPersister(settings: CassandraRegistryPersisterSettings) extends Actor with ActorLogging {
  import context.dispatcher

  val session = Cassandra(context.system).getSession
  val agentRegistrationDAL = new AgentRegistrationDAL(settings, session, context.dispatcher)
  val agentGroupDAL = new AgentGroupDAL(settings, session, context.dispatcher)
  val agentTombstoneDAL = new AgentTombstoneDAL(settings, session, context.dispatcher)

  def receive = {

    case op: GetRegistration =>
      agentRegistrationDAL.getLastAgentRegistration(op).recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetRegistrationHistory =>
      val props = GetAgentRegistrationHistoryTask.props(op, sender(), agentRegistrationDAL)
      context.actorOf(props)

    case op: PutRegistration =>
      agentRegistrationDAL.updateAgentRegistration(op.agentId, op.metadata.generation, op.lsn,
        op.registration, op.metadata.joinedOn, op.metadata.lastUpdate, op.metadata.expires, committed = false).map {
        _ => PutRegistrationResult(op, op.metadata)
      }.recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: CommitRegistration =>
      agentRegistrationDAL.updateAgentRegistration(op.agentId, op.metadata.generation, op.lsn,
        op.registration, op.metadata.joinedOn, op.metadata.lastUpdate, op.metadata.expires, committed = true).map {
        _ => CommitRegistrationResult(op)
      }.recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteRegistration =>
      sender() ! RegistryServiceOperationFailed(op, ApiException(NotImplemented))

    case op: PutTombstone =>
      val partition = calculatePartition(op.agentId)
      agentTombstoneDAL.putTombstone(partition, op.expires, op.agentId, op.generation).map {
        _ => PutTombstoneResult(op)
      }.recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteTombstone =>
      val partition = calculatePartition(op.agentId)
      agentTombstoneDAL.deleteTombstone(partition, op.expires, op.agentId, op.generation).map {
        _ => DeleteTombstoneResult(op)
      }.recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: AddAgentToGroup =>
      agentGroupDAL.addToGroup(op.groupName, op.metadata).map {
        _ => AddAgentToGroupResult(op)
      }.recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: RemoveAgentFromGroup =>
      agentGroupDAL.removeFromGroup(op.groupName, op.agentId).map {
        _ => RemoveAgentFromGroupResult(op)
      }.recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DescribeGroup =>
      val props = DescribeGroupTask.props(op, sender(), agentGroupDAL)
      context.actorOf(props)
  }

  /**
   * given an AgentId, return an Integer partition key.  this is used to spread out
   * tombstones across multiple partitions.
   */
  def calculatePartition(agentId: AgentId): Int = MurmurHash3.stringHash(agentId.toString)

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3) {
    /* transient cassandra exceptions */
    case ex: QueryTimeoutException => Restart
    case ex: NoHostAvailableException => Restart
    case ex: UnavailableException => Restart
    /* if we receive any other exception then stop the task */
    case ex: Throwable => Stop
  }
}

object CassandraRegistryPersister {
  def props(managerSettings: CassandraRegistryPersisterSettings) = Props(classOf[CassandraRegistryPersister], managerSettings)

  def settings(config: Config): Option[CassandraRegistryPersisterSettings] = {
    Some(CassandraRegistryPersisterSettings())
  }
}

case class CassandraRegistryPersisterSettings()

class CassandraRegistryPersisterExtension extends RegistryPersisterExtension {
  type Settings = CassandraRegistryPersisterSettings
  def configure(config: Config): Settings = CassandraRegistryPersisterSettings()
  def props(settings: Settings): Props = CassandraRegistryPersister.props(settings)
}