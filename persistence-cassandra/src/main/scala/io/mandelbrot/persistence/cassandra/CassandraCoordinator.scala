package io.mandelbrot.persistence.cassandra

import akka.actor._
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.cluster._
import io.mandelbrot.persistence.cassandra.CassandraCoordinator.CassandraCoordinatorSettings

/**
 *
 */
class CassandraCoordinator(settings: CassandraCoordinatorSettings) extends Actor with ActorLogging with Coordinator {
  import context.dispatcher

  val session = Cassandra(context.system).getSession
  val driver = new CoordinatorDriver(settings, session)

  def receive = {

    case op: CreateShard =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      driver.createShard(op, timestamp).recover {
        case ex: Throwable => ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateShard =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      driver.updateShard(op, timestamp).recover {
        case ex: Throwable => sender () ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetShard =>
      driver.getShard(op).recover {
        case ex: Throwable => sender() ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: ListShards =>
      driver.listShards(op).recover {
        case ex: Throwable => sender() ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: CreateEntity =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      driver.createEntity(op, timestamp).recover {
        case ex: Throwable => sender() ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteEntity =>
      driver.deleteEntity(op).recover {
        case ex: Throwable => sender() ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetEntity =>
      driver.getEntity(op).recover {
        case ex: Throwable => sender() ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: ListEntities =>
      driver.listEntities(op).recover {
        case ex: Throwable => sender() ! ClusterServiceOperationFailed(op, ex)
      }.pipeTo(sender())
  }

}

object CassandraCoordinator {
  def props(managerSettings: CassandraCoordinatorSettings) = Props(classOf[CassandraCoordinator], managerSettings)

  case class CassandraCoordinatorSettings()
  def settings(config: Config): Option[CassandraCoordinatorSettings] = {
    Some(CassandraCoordinatorSettings())
  }
}
