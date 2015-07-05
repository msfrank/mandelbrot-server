package io.mandelbrot.core.registry

import akka.actor._
import com.typesafe.config.Config
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.model._

class TestRegistryPersister(settings: TestRegistryPersisterSettings) extends Actor with ActorLogging {
  import RegistryManager.{MinGenerationLsn,MaxGenerationLsn}

  val groups = new util.TreeMap[String,util.TreeMap[String,AgentMetadata]]()
  val registrations = new util.TreeMap[AgentId, util.TreeMap[GenerationLsn,RegistrationEvent]]()
  val tombstones = new util.TreeMap[DateTime,util.HashSet[Tombstone]]()

  def receive = {

    case op: GetRegistration =>
      registrations.get(op.agentId) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case events =>
          events.lastOption.map(_._2) match {
            case Some(event: RegistrationEvent) =>
              sender() ! GetRegistrationResult(op, event.registration, event.metadata, event.lsn, event.committed)
            case None =>
              sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
          }
      }

    case op: GetRegistrationHistory =>
      try {
        val page = registrations.get(op.agentId) match {
          // if there is no check, then raise ResourceNotFound
          case null =>
            throw ApiException(ResourceNotFound)
          // if no timeseries params are specified, return the last entry
          case history if op.from.isEmpty && op.to.isEmpty =>
            val agents = history.lastEntry() match {
              case null => Vector.empty
              case entry => Vector(entry.getValue.registration)
            }
            RegistrationsPage(agents, None, exhausted = true)
          // otherwise return the subset of entries
          case history =>
            val from: GenerationLsn = op.last.map(string2generationLsn).getOrElse(op.from.getOrElse(MinGenerationLsn))
            val fromInclusive: Boolean = if (op.last.isDefined) false else op.fromInclusive
            val to: GenerationLsn = op.to.getOrElse(MaxGenerationLsn)
            val events = history.subMap(from, fromInclusive, to, !op.toExclusive).map(entry => entry._2).toVector
            val agents = events.take(op.limit).map(_.registration)
            val last = if (events.length > agents.length) {
              val event = events(agents.length - 1)
              Some(generationLsn2string(event.metadata.generation, event.lsn))
            } else None
            RegistrationsPage(agents, last, exhausted = last.isEmpty)
        }
        sender() ! GetRegistrationHistoryResult(op, page)
      } catch {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }

    case op: PutRegistration =>
      val events = registrations.getOrDefault(op.agentId, new util.TreeMap[GenerationLsn,RegistrationEvent])
      val event = RegistrationEvent(op.registration, op.metadata, op.lsn, committed = false)
      events.put(GenerationLsn(op.metadata.generation,op.lsn), event)
      registrations.put(op.agentId, events)
      sender() ! PutRegistrationResult(op, op.metadata)

    case op: CommitRegistration =>
      val events = registrations.getOrDefault(op.agentId, new util.TreeMap[GenerationLsn,RegistrationEvent])
      val event = RegistrationEvent(op.registration, op.metadata, op.lsn, committed = true)
      events.put(GenerationLsn(op.metadata.generation,op.lsn), event)
      registrations.put(op.agentId, events)
      sender() ! CommitRegistrationResult(op)

    case op: DeleteRegistration =>
      registrations.remove(op.agentId)
      sender() ! DeleteRegistrationResult(op)

    case op: ListTombstones =>
      val expired = tombstones.headMap(op.olderThan, false).flatMap(_._2.toVector).take(op.limit).toVector
      sender() ! ListTombstonesResult(op, expired)

    case op: PutTombstone =>
      val tombstoneSet = tombstones.getOrDefault(op.expires, new util.HashSet[Tombstone])
      tombstoneSet.add(Tombstone(op.expires, op.agentId, op.generation))
      tombstones.put(op.expires, tombstoneSet)
      sender() ! PutTombstoneResult(op)

    case op: DeleteTombstone =>
      Option(tombstones.get(op.expires)).foreach { tombstoneSet =>
        tombstoneSet.remove(Tombstone(op.expires, op.agentId, op.generation))
        if (tombstoneSet.isEmpty)
          tombstones.remove(op.expires)
      }
      sender() ! DeleteTombstoneResult(op)

    case op: AddAgentToGroup =>
      val group = groups.getOrDefault(op.groupName, new util.TreeMap[String,AgentMetadata]())
      group.put(op.metadata.agentId.toString, op.metadata)
      groups.put(op.groupName, group)
      sender() ! AddAgentToGroupResult(op)

    case op: RemoveAgentFromGroup =>
      val group = groups.getOrDefault(op.groupName, new util.TreeMap[String,AgentMetadata]())
      group.remove(op.agentId.toString)
      if (group.isEmpty)
        groups.remove(op.groupName)
      else
        groups.put(op.groupName, group)
      sender() ! RemoveAgentFromGroupResult(op)

    case op: ListGroups =>
      val page = op.last match {
        case Some(from) =>
          val groupNames = groups.tailMap(from, false).keys.toVector
          val last = if (groupNames.length > op.limit) Some(groupNames(op.limit - 1)) else None
          GroupsPage(groupNames.take(op.limit), last, exhausted = last.isEmpty)
        case None =>
          val groupNames = groups.keys.toVector
          val last = if (groupNames.length > op.limit) Some(groupNames(op.limit - 1)) else None
          GroupsPage(groupNames.take(op.limit), last, exhausted = last.isEmpty)
      }
      sender() ! ListGroupsResult(op, page)

    case op: DescribeGroup =>
      groups.get(op.groupName) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case group if op.last.isDefined =>
          val members = group.tailMap(op.last.get, false).values.toVector
          val last = if (members.length > op.limit) Some(members(op.limit - 1).agentId.toString) else None
          val page = MetadataPage(members.take(op.limit), last, exhausted = last.isEmpty)
          sender() ! DescribeGroupResult(op, page)
        case group =>
          val members = group.values.toVector
          val last = if (members.length > op.limit) Some(members(op.limit - 1).agentId.toString) else None
          val page = MetadataPage(members.take(op.limit), last, exhausted = last.isEmpty)
          sender() ! DescribeGroupResult(op, page)
      }
  }

  def string2generationLsn(string: String): GenerationLsn = {
    string.split(':').map(_.toLong).toList match {
      case generation :: lsn :: Nil => GenerationLsn(generation,lsn)
      case _ => throw new IllegalArgumentException("parameter is malformed")
    }
  }

  def generationLsn2string(generation: Long, lsn: Long): String = "{}:{}".format(generation, lsn)
}

case class RegistrationEvent(registration: AgentSpec,
                             metadata: AgentMetadata,
                             lsn: Long,
                             committed: Boolean)

object TestRegistryPersister {
  def props(settings: TestRegistryPersisterSettings) = Props(classOf[TestRegistryPersister], settings)
}

case class TestRegistryPersisterSettings()

class TestRegistryPersisterExtension extends RegistryPersisterExtension {
  type Settings = TestRegistryPersisterSettings
  def configure(config: Config): Settings = TestRegistryPersisterSettings()
  def props(settings: Settings): Props = TestRegistryPersister.props(settings)
}

