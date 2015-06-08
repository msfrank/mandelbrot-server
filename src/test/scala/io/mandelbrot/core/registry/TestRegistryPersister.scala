package io.mandelbrot.core.registry

import akka.actor._
import com.typesafe.config.Config
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.model._

class TestRegistryPersister(settings: TestRegistryPersisterSettings) extends Actor with ActorLogging {
  import RegistryManager.{MinGenerationLsn,MaxGenerationLsn}

  val groups = new util.TreeMap[String,util.TreeSet[AgentMetadata]]()
  val registrations = new util.TreeMap[AgentId, util.TreeMap[GenerationLsn,RegistrationEvent]]()
  val tombstones = new util.TreeSet[AgentTombstone]()

  def receive = {

    case op: GetRegistration =>
      registrations.get(op.agentId) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case events =>
          events.lastOption.map(_._2) match {
            case Some(event: RegistrationEvent) =>
              sender() ! GetRegistrationResult(op, event.registration, event.metadata, event.lsn)
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

    case op: CreateRegistration =>
      val events = registrations.getOrDefault(op.agentId, new util.TreeMap[GenerationLsn,RegistrationEvent])
      val event = RegistrationEvent(op.registration, op.metadata, op.lsn, committed = false)
      events.put(GenerationLsn(op.metadata.generation,op.lsn), event)
      registrations.put(op.agentId, events)
      sender() ! CreateRegistrationResult(op, op.metadata)

    case op: UpdateRegistration =>
      val events = registrations.getOrDefault(op.agentId, new util.TreeMap[GenerationLsn,RegistrationEvent])
      val event = RegistrationEvent(op.registration, op.metadata, op.lsn, committed = false)
      events.put(GenerationLsn(op.metadata.generation,op.lsn), event)
      registrations.put(op.agentId, events)
      sender() ! UpdateRegistrationResult(op)

    case op: RetireRegistration =>
      val events = registrations.getOrDefault(op.agentId, new util.TreeMap[GenerationLsn,RegistrationEvent])
      val event = RegistrationEvent(op.registration, op.metadata, op.lsn, committed = false)
      events.put(GenerationLsn(op.metadata.generation,op.lsn), event)
      registrations.put(op.agentId, events)
      sender() ! RetireRegistrationResult(op)

    case op: DeleteRegistration =>
      registrations.remove(op.agentId)
      sender() ! DeleteRegistrationResult(op)

    case op: ListTombstones =>
      val expired = tombstones.headSet(AgentTombstone(op.olderThan, new AgentId(Vector.empty), 0)).take(op.limit).toVector
      sender() ! ListTombstonesResult(op, expired)

    case op: PutTombstone =>
      tombstones.add(AgentTombstone(op.expires, op.agentId, op.generation))
      sender() ! PutTombstoneResult(op)

    case op: DeleteTombstone =>
      tombstones.remove(AgentTombstone(op.expires, op.agentId, op.generation))
      sender() ! DeleteTombstoneResult(op)
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

