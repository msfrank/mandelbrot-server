package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.registry._

/**
 *
 */
class CassandraRegistryPersister(settings: CassandraRegistryPersisterSettings) extends Actor with ActorLogging {
  import context.dispatcher

  val session = Cassandra(context.system).getSession
  val registry = new RegistryDAL(settings, session, context.dispatcher)

  def receive = {

    case op: CreateRegistration =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      registry.createProbeSystem(op, timestamp).recover {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateRegistration =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      registry.updateProbeSystem(op, timestamp).recover {
        case ex: Throwable => sender () ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteRegistration =>
      registry.deleteProbeSystem(op).recover {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetRegistration =>
      registry.getProbeSystem(op).recover {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: ListRegistrations =>
      registry.listProbeSystems(op).recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())
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