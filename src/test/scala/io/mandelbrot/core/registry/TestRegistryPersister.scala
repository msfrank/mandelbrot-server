package io.mandelbrot.core.registry

import akka.actor._
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.{ResourceNotFound, InternalError, ApiException}
import io.mandelbrot.core.model.{AgentId, AgentsPage}

class TestRegistryPersister(settings: TestRegistryPersisterSettings) extends Actor with ActorLogging {

  val registrations = new util.TreeMap[AgentId, List[RegistryServiceCommand]]
  val tombstones = new util.TreeSet[Tombstone]()

  def receive = {

    case op: CreateRegistration =>
      val events = registrations.getOrDefault(op.agentId, List.empty[RegistryServiceCommand])
      registrations.put(op.agentId, events :+ op)
      sender() ! CreateRegistrationResult(op, op.metadata)

    case op: UpdateRegistration =>
      val events = registrations.getOrDefault(op.agentId, List.empty[RegistryServiceCommand])
      registrations.put(op.agentId, events :+ op)
      sender() ! UpdateRegistrationResult(op)

    case op: RetireRegistration =>
      val events = registrations.getOrDefault(op.agentId, List.empty[RegistryServiceCommand])
      registrations.put(op.agentId, events :+ op)
      sender() ! RetireRegistrationResult(op)

    case op: DeleteRegistration =>
      registrations.remove(op.agentId)
      sender() ! DeleteRegistrationResult(op)

    case op: PutTombstone =>
      tombstones.add(Tombstone(op.expires, op.agentId, op.generation))
      sender() ! PutTombstoneResult(op)

    case op: DeleteTombstone =>
      tombstones.remove(Tombstone(op.expires, op.agentId, op.generation))
      sender() ! DeleteTombstoneResult(op)

    case op: ListRegistrations =>
      op.last match {
        case None =>
          val agents = registrations.values().take(op.limit).flatMap(_.lastOption).map {
            case event: CreateRegistration => event.metadata
            case event: UpdateRegistration => event.metadata
            case event: RetireRegistration => event.metadata
            case event => throw new IllegalArgumentException("unexpected event " + event.toString)
          }.toVector
          val last = if (agents.length < op.limit) None else agents.lastOption.map(_.agentId.toString)
          val page = AgentsPage(agents, last)
          sender() ! ListRegistrationsResult(op, page)
        case Some(prev) =>
          val agents = registrations.tailMap(AgentId(prev), false).values().take(op.limit).flatMap(_.lastOption).map {
            case event: CreateRegistration => event.metadata
            case event: UpdateRegistration => event.metadata
            case event: RetireRegistration => event.metadata
            case event => throw new IllegalArgumentException("unexpected event " + event.toString)
          }.toVector
          val last = if (agents.length < op.limit) None else agents.lastOption.map(_.agentId.toString)
          val page = AgentsPage(agents, last)
          sender() ! ListRegistrationsResult(op, page)
      }

    case op: GetRegistration =>
      registrations.get(op.agentId) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case events =>
          events.lastOption match {
            case None =>
              sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
            case Some(event: CreateRegistration) =>
              sender() ! GetRegistrationResult(op, event.registration, event.metadata, event.lsn)
            case Some(event: UpdateRegistration) =>
              sender() ! GetRegistrationResult(op, event.registration, event.metadata, event.lsn)
            case Some(event: RetireRegistration) =>
              sender() ! GetRegistrationResult(op, event.registration, event.metadata, event.lsn)
            case Some(event: RegistryServiceCommand) =>
              sender() ! RegistryServiceOperationFailed(op, ApiException(InternalError))
          }
      }
  }
}

case class Tombstone(expires: DateTime, agentId: AgentId, generation: Long) extends Comparable[Tombstone] with Ordered[Tombstone] {
  override def compare(other: Tombstone): Int = {
    import scala.math.Ordered.orderingToOrdered
    (expires.getMillis, agentId.toString, generation).compare((other.expires.getMillis, other.agentId.toString, other.generation))
  }
  override def compareTo(other: Tombstone): Int = compare(other)
}

object TestRegistryPersister {
  def props(settings: TestRegistryPersisterSettings) = Props(classOf[TestRegistryPersister], settings)
}

case class TestRegistryPersisterSettings()

class TestRegistryPersisterExtension extends RegistryPersisterExtension {
  type Settings = TestRegistryPersisterSettings
  def configure(config: Config): Settings = TestRegistryPersisterSettings()
  def props(settings: Settings): Props = TestRegistryPersister.props(settings)
}

