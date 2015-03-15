package io.mandelbrot.core.entity

import java.util

import akka.actor._
import com.typesafe.config.Config
import io.mandelbrot.core.{ApiException, Conflict, ResourceNotFound}

import scala.collection.JavaConversions._

class TestCoordinator(settings: TestCoordinatorSettings) extends Actor with ActorLogging with Coordinator {

  object EntityOrdering extends Ordering[Entity] {
    override def compare(x: Entity, y: Entity): Int = {
      x.shardId.compare(y.shardId) match {
        case result if result != 0 => result
        case otherwise => x.entityKey.compare(y.entityKey)
      }
    }
  }

  val shardMap = settings.shardMap
  val shardEntities = new util.TreeSet[Entity](EntityOrdering)
  val masterAddress = settings.masterAddress
  val selfAddress = settings.selfAddress

  // initialize state
  settings.initialEntities.foreach(shardEntities.add)

  def receive = {

    case message if !selfAddress.equals(masterAddress) =>
      val actorPath = RootActorPath(masterAddress) / self.path.elements
      log.debug("forwarding message {} to master {}", message, actorPath)
      context.actorSelection(actorPath) forward message

    case op: ListShards =>
      log.debug("{} requests all shards", sender().path)
      val shards = shardMap.assigned.map(entry => Shard(entry.shardId, entry.address))
      sender() ! ListShardsResult(op, shards, None)

    case op: GetShard =>
      log.debug("{} requests shard {}", sender().path, op.shardId)
      shardMap.get(op.shardId) match {
        case shard: AssignedShardEntry =>
          sender() ! GetShardResult(op, shard.shardId, shard.address)
        case shard: ShardEntry =>
          sender() ! EntityServiceOperationFailed(op, ApiException(ResourceNotFound))
      }

    case op: CreateShard =>
      shardMap.get(op.shardId) match {
        case entry: MissingShardEntry =>
          log.debug("{} creates shard {} and assigns to {}", sender().path, op.shardId, op.address)
          shardMap.assign(op.shardId, op.address)
          sender() ! CreateShardResult(op)
        case entry: ShardEntry =>
          sender() ! EntityServiceOperationFailed(op, ApiException(Conflict))
      }

    case op: UpdateShard =>
      shardMap.get(op.shardId) match {
        case entry: MissingShardEntry =>
          sender() ! EntityServiceOperationFailed(op, ApiException(ResourceNotFound))
        case entry: ShardEntry =>
          log.debug("{} updates shard {} from {} to {}", sender().path, op.shardId, op.address, op.prev)
          shardMap.assign(op.shardId, op.address)
          sender() ! UpdateShardResult(op)
      }

    case op: CreateEntity =>
      val entity = Entity(op.shardId, op.entityKey)
      if (!shardEntities.contains(entity)) {
        log.debug("{} creates entity {}:{}", sender().path, op.shardId, op.entityKey)
        shardEntities.add(entity)
        sender() ! CreateEntityResult(op)
      } else sender() ! EntityServiceOperationFailed(op, ApiException(Conflict))

    case op: DeleteEntity =>
      val entity = Entity(op.shardId, op.entityKey)
      if (shardEntities.contains(entity)) {
        log.debug("{} deletes entity {}:{}", sender().path, op.shardId, op.entityKey)
        shardEntities.remove(entity)
        sender() ! DeleteEntityResult(op)
      } else sender() ! EntityServiceOperationFailed(op, ApiException(ResourceNotFound))

    case op: ListEntities =>
      log.debug("{} requests entities for shard {}", sender().path, op.shardId)
      val subset = shardEntities.subSet(Entity(op.shardId, ""), Entity(op.shardId + 1, ""))
      sender() ! ListEntitiesResult(op, subset.toVector, None)
  }
}

object TestCoordinator {
  def props(settings: TestCoordinatorSettings) = Props(classOf[TestCoordinator], settings)
}

case class TestCoordinatorSettings(shardMap: ShardMap, initialEntities: Vector[Entity], masterAddress: Address, selfAddress: Address)

class TestEntityCoordinator extends EntityCoordinatorExtension {
  type Settings = TestCoordinatorSettings
  def configure(config: Config): Settings = throw new NotImplementedError()
  def props(settings: Settings): Props = TestCoordinator.props(settings)
}
