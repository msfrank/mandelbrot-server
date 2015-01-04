package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._
import io.mandelbrot.core.{ResourceNotFound, BadRequest, ApiException}
import org.joda.time.DateTime
import java.util

import io.mandelbrot.core.cluster.EntityFunctions.{ShardResolver, PropsCreator, KeyExtractor}

/**
 *
 */
class EntityManager(coordinator: ActorRef,
                    shardResolver: ShardResolver,
                    keyExtractor: KeyExtractor,
                    propsCreator: PropsCreator,
                    selfAddress: Address,
                    totalShards: Int,
                    initialWidth: Int) extends Actor with ActorLogging {
  import EntityManager._
  import context.dispatcher

  // state
  val shardMap = ShardMap(totalShards, initialWidth)
  val localEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()
  val bufferedMessages = new util.ArrayList[BufferedEnvelope]()
  val resolvingShards = new util.HashSet[Int]()
  val cachedNacks = new util.HashMap[Int,DateTime]()

  def receive = {

    // send the specified message to the entity, which may be remote or local
    case envelope: EntityEnvelope =>
      if (keyExtractor.isDefinedAt(envelope.message)) {
        val shardKey = shardResolver(envelope.message)
        shardMap(shardKey) match {

          // shard is assigned to this node
          case shard: AssignedShardEntry if shard.address.equals(selfAddress) =>
            val entityRefs = localEntities.get(shard.shardId)
            val entityKey = keyExtractor(envelope.message)
            entityRefs.get(entityKey) match {
              // entity doesn't exist in shard
              case null =>
                // if this message creates props, then create the actor
                if (propsCreator.isDefinedAt(envelope.message)) {
                  val props = propsCreator(envelope.message)
                  val entity = context.actorOf(props)
                  context.watch(entity)
                  entityRefs.put(entityKey, entity)
                  entityShards.put(entity, shard.shardId)
                  entity.tell(envelope.message, envelope.sender)
                } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))
              // entity exists, forward the message to it
              case entity: ActorRef =>
                entity.tell(envelope.message, envelope.sender)
            }

          // shard is assigned to a remote node
          case shard: AssignedShardEntry =>
            if (envelope.attempts > 0) {
              val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
              selection ! envelope.copy(attempts = envelope.attempts - 1)
              // if the sender is remote, then notify the sender entity manager about the stale shard mapping
              if (!envelope.sender.path.address.equals(selfAddress))
                sender() ! StaleShard(shardKey, shard.shardId, shard.address)
            } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

          // shard is in the process of being assigned to this node
          case shard: PreparingShardEntry =>
            log.debug("shard {} is preparing, buffering {}", shard.shardId, envelope.message)
            bufferedMessages.add(BufferedEnvelope(envelope, shardKey, DateTime.now()))

          // shard is in the process of being migrated from this node
          case shard: MigratingShardEntry =>
            if (envelope.attempts > 0) {
              val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
              selection ! envelope.copy(attempts = envelope.attempts - 1)
              // if the sender is remote, then notify the sender entity manager about the stale shard mapping
              if (!envelope.sender.path.address.equals(selfAddress))
                sender() ! StaleShard(shardKey, shard.shardId, shard.address)
            } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

          // shard is missing from the shard map
          case shard: MissingShardEntry =>
            log.debug("shard not known for entity {}, buffering {}", shardKey, envelope.message)
            bufferedMessages.add(BufferedEnvelope(envelope, shardKey, DateTime.now()))
            // FIXME: don't pound the crap out of the coordinator
            coordinator ! GetShard(shardKey)
        }
      } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(BadRequest))

    // start buffering messages in preparation to own shard
    case op @ PrepareShard(shardId) =>
      log.debug("{} says prepare shardId {}", sender().path, shardId)
      shardMap.prepare(shardId, selfAddress)
      sender() ! PrepareShardResult(op)

    // change proposed shard to owned
    case op @ RecoverShard(shardId) =>
      log.debug("{} says recover shardId {}", sender().path, shardId)
      coordinator ! GetShard(shardId)
      sender() ! RecoverShardResult(op)

    // shard mapping is stale, request updated data from the coordinator
    case StaleShard(shardKey, shardId, address) =>
      log.debug("{} says shardId {} is stale for shardKey {}", sender().path, shardId, shardKey)
      coordinator ! GetShard(shardKey)

    // update shard map and flush buffered messages
    case result: GetShardResult =>
      result.address match {
        case Some(address) =>
          log.debug("shard {} exists at {}", result.shardId, result.address)
          shardMap.assign(result.shardId, address)
          // if shard is local and entity map doesn't exist, create a new entity map
          if (address.equals(selfAddress) && !localEntities.containsKey(result.shardId))
            localEntities.put(result.shardId, new EntityMap)
          // flush any buffered messages for the shard
          val iterator = bufferedMessages.listIterator()
          while (iterator.hasNext) {
            val envelope = iterator.next()
            if (shardMap.contains(envelope.shardKey)) {
              iterator.remove()
              self ! envelope.envelope
            }
          }
        case None =>
          log.debug("shard {} is missing, retrying", result.shardId)
          coordinator ! GetShard(result.shardId)
      }

    // cluster service operation failed
    case failure: ClusterServiceOperationFailed =>
      log.debug("operation {} failed: {}", failure.op, failure.failure)
  }
}

object EntityManager {
  def props(coordinator: ActorRef,
            shardResolver: ShardResolver,
            keyExtractor: KeyExtractor,
            propsCreator: PropsCreator,
            selfAddress: Address,
            totalShards: Int,
            initialWidth: Int) = {
    Props(classOf[EntityManager], coordinator, shardResolver, keyExtractor, propsCreator, selfAddress, totalShards, initialWidth)
  }

  class EntityMap extends util.HashMap[String,ActorRef]
  case class BufferedEnvelope(envelope: EntityEnvelope, shardKey: Int, timestamp: DateTime)
  case class StaleShard(shardKey: Int, shardId: Int, address: Address)
}

object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class EntityEnvelope(sender: ActorRef, message: Any, attempts: Int)
case class EntityDeliveryFailed(envelope: EntityEnvelope, failure: Throwable)

