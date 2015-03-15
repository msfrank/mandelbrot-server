package io.mandelbrot.persistence.cassandra

import akka.actor._
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.entity._

/**
 *
 */
class CassandraEntityCoordinator(settings: CassandraEntityCoordinatorSettings) extends Actor with ActorLogging {
  import context.dispatcher

  val session = Cassandra(context.system).getSession
  val entities = new EntitiesDAL(settings, session, context.dispatcher)
  val shards = new ShardsDAL(settings, session, context.dispatcher)

  def receive = {

    case op: CreateShard =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      shards.createShard(op, timestamp).recover {
        case ex: Throwable => EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateShard =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      shards.updateShard(op, timestamp).recover {
        case ex: Throwable => sender () ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetShard =>
      shards.getShard(op).recover {
        case ex: Throwable => sender() ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: ListShards =>
      shards.listShards(op).recover {
        case ex: Throwable => sender() ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: CreateEntity =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      entities.createEntity(op, timestamp).recover {
        case ex: Throwable => sender() ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteEntity =>
      entities.deleteEntity(op).recover {
        case ex: Throwable => sender() ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetEntity =>
      entities.getEntity(op).recover {
        case ex: Throwable => sender() ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: ListEntities =>
      entities.listEntities(op).recover {
        case ex: Throwable => sender() ! EntityServiceOperationFailed(op, ex)
      }.pipeTo(sender())
  }

}

object CassandraEntityCoordinator {
  def props(managerSettings: CassandraEntityCoordinatorSettings) = Props(classOf[CassandraEntityCoordinator], managerSettings)
}

case class CassandraEntityCoordinatorSettings()

class CassandraEntityCoordinatorExtension extends EntityCoordinatorExtension {
  type Settings = CassandraEntityCoordinatorSettings
  def configure(config: Config): Settings = CassandraEntityCoordinatorSettings()
  def props(settings: Settings): Props = CassandraEntityCoordinator.props(settings)
}
