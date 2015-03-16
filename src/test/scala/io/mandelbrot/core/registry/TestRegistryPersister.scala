package io.mandelbrot.core.registry

import akka.actor._
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException}
import io.mandelbrot.core.model.{ProbeSystemsPage, ProbeRegistration, ProbeSystemMetadata}

class TestRegistryPersister(settings: TestRegistryPersisterSettings) extends Actor with ActorLogging {

  val registrations = new java.util.TreeMap[URI, (ProbeSystemMetadata,ProbeRegistration,Long)]

  def receive = {

    case op: CreateProbeSystemEntry =>
      if (!registrations.containsKey(op.uri)) {
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = ProbeSystemMetadata(op.uri, timestamp, timestamp)
        registrations.put(op.uri, (metadata, op.registration, 0))
        sender() ! CreateProbeSystemEntryResult(op, 0)
      } else sender() ! RegistryServiceOperationFailed(op, ApiException(Conflict))

    case op: UpdateProbeSystemEntry =>
      registrations.get(op.uri) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case entry: (ProbeSystemMetadata,ProbeRegistration,Long) =>
          val timestamp = DateTime.now(DateTimeZone.UTC)
          val metadata = entry._1.copy(lastUpdate = timestamp)
          val lsn = entry._3 + 1
          registrations.put(op.uri, (metadata, op.registration, lsn))
          sender() ! UpdateProbeSystemEntryResult(op, lsn)
      }

    case op: DeleteProbeSystemEntry =>
      registrations.remove(op.uri) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case entry: (ProbeSystemMetadata,ProbeRegistration,Long) =>
          sender() ! DeleteProbeSystemEntryResult(op, entry._3)
      }

    case op: ListProbeSystems =>
      op.last match {
        case None =>
          val systems = registrations.values()
            .take(op.limit)
            .map(_._1).toVector
          val last = if (systems.length < op.limit) None else systems.lastOption.map(_.uri.toString)
          val page = ProbeSystemsPage(systems, last)
          sender() ! ListProbeSystemsResult(op, page)
        case Some(prev) =>
          val systems = registrations.tailMap(new URI(prev), false)
            .values()
            .take(op.limit)
            .map(_._1).toVector
          val last = if (systems.length < op.limit) None else systems.lastOption.map(_.uri.toString)
          val page = ProbeSystemsPage(systems, last)
          sender() ! ListProbeSystemsResult(op, page)
      }

    case op: GetProbeSystemEntry =>
      registrations.get(op.uri) match {
        case null =>
          sender() ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
        case entry: (ProbeSystemMetadata,ProbeRegistration,Long) =>
          sender() ! GetProbeSystemEntryResult(op, entry._2, entry._3)
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

