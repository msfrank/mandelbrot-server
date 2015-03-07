package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.registry._
import io.mandelbrot.persistence.cassandra.CassandraRegistrar.CassandraRegistrarSettings

/**
 *
 */
class CassandraRegistrar(settings: CassandraRegistrarSettings) extends Actor with ActorLogging with Registrar {
  import context.dispatcher

  val session = Cassandra(context.system).getSession
  val registry = new RegistryDAL(settings, session, context.dispatcher)

  def receive = {

    case op: CreateProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      registry.createProbeSystem(op, timestamp).recover {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      registry.updateProbeSystem(op, timestamp).recover {
        case ex: Throwable => sender () ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteProbeSystemEntry =>
      registry.deleteProbeSystem(op).recover {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetProbeSystemEntry =>
      registry.getProbeSystem(op).recover {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: ListProbeSystems =>
      registry.listProbeSystems(op).recover {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }.pipeTo(sender())
  }
}

object CassandraRegistrar {
  def props(managerSettings: CassandraRegistrarSettings) = Props(classOf[CassandraRegistrar], managerSettings)

  case class CassandraRegistrarSettings()
  def settings(config: Config): Option[CassandraRegistrarSettings] = {
    Some(CassandraRegistrarSettings())
  }
}
