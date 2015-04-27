package io.mandelbrot.core.registry

import akka.actor._
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException}
import io.mandelbrot.core.model.{AgentId, AgentsPage, AgentRegistration, AgentMetadata}

class TestRegistryPersister(settings: TestRegistryPersisterSettings) extends Actor with ActorLogging {

  val registrations = new java.util.TreeMap[AgentId, (AgentMetadata,AgentRegistration)]

  def receive = {

    case op: CreateRegistration =>
      if (!registrations.containsKey(op.agentId)) {
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = AgentMetadata(op.agentId, timestamp, timestamp, 0)
        registrations.put(op.agentId, (metadata, op.registration))
        sender() ! CreateRegistrationResult(op, metadata)
      } else sender() ! RegistryServiceOperationFailed(op, ApiException(Conflict))

    case op: UpdateRegistration =>
      registrations.get(op.agentId) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case (metadata: AgentMetadata, registration: AgentRegistration) =>
          val timestamp = DateTime.now(DateTimeZone.UTC)
          val lsn = metadata.lsn + 1
          val updated = metadata.copy(lastUpdate = timestamp, lsn = lsn)
          registrations.put(op.agentId, (updated, registration))
          sender() ! UpdateRegistrationResult(op, updated)
      }

    case op: DeleteRegistration =>
      registrations.remove(op.agentId) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case (metadata: AgentMetadata, registration: AgentRegistration) =>
          sender() ! DeleteRegistrationResult(op, metadata.lsn)
      }

    case op: ListRegistrations =>
      op.last match {
        case None =>
          val systems = registrations.values()
            .take(op.limit)
            .map(_._1).toVector
          val last = if (systems.length < op.limit) None else systems.lastOption.map(_.agentId.toString)
          val page = AgentsPage(systems, last)
          sender() ! ListRegistrationsResult(op, page)
        case Some(prev) =>
          val systems = registrations.tailMap(AgentId(prev), false)
            .values()
            .take(op.limit)
            .map(_._1).toVector
          val last = if (systems.length < op.limit) None else systems.lastOption.map(_.agentId.toString)
          val page = AgentsPage(systems, last)
          sender() ! ListRegistrationsResult(op, page)
      }

    case op: GetRegistration =>
      registrations.get(op.agentId) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case (metadata: AgentMetadata, registration: AgentRegistration) =>
          sender() ! GetRegistrationResult(op, registration, metadata.lsn)
      }
  }
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

