package io.mandelbrot.core.cluster

import akka.actor._
import io.mandelbrot.core.{RetryLater, ResourceNotFound, BadRequest, ApiException}
import org.joda.time.DateTime
import scala.concurrent.duration._
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

  // config
  val shardTimeout = 5.seconds
  
  // state
  val shardMap = ShardMap(totalShards, initialWidth)
  val localEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()
  val bufferedMessages = new util.ArrayList[EntityEnvelope]()
  val taskTimeouts = new util.HashMap[Int,Cancellable]()
  val staleShards = new util.HashSet[StaleShard]()
  val missingShards = new util.HashMap[Int,ActorRef]

  def receive = {

    // send the specified message to the entity, which may be remote or local
    case envelope: EntityEnvelope =>
      deliverEnvelope(envelope)

    // start buffering messages in preparation to own shard
    case op: PrepareShard =>
      shardMap.get(op.shardId) match {
        case entry: AssignedShardEntry =>
          sender() ! ClusterServiceOperationFailed(op, new ApiException(BadRequest))
        case entry: ShardEntry if taskTimeouts.containsKey(op.shardId) =>
          sender() ! ClusterServiceOperationFailed(op, new ApiException(RetryLater))
        case entry: ShardEntry =>
          log.debug("{} says prepare shardId {}", sender().path, op.shardId)
          shardMap.prepare(op.shardId, selfAddress)
          taskTimeouts.put(op.shardId, context.system.scheduler.scheduleOnce(shardTimeout, self, TaskTimeout(op.shardId)))
          sender() ! PrepareShardResult(op)
      }

    // change preparing shard to assigned
    case op: RecoverShard =>
      shardMap.get(op.shardId) match {
        case entry: PreparingShardEntry if !taskTimeouts.containsKey(op.shardId) =>
          sender() ! ClusterServiceOperationFailed(op, new ApiException(RetryLater))
        case entry: PreparingShardEntry =>
          log.debug("{} says recover shardId {}", sender().path, op.shardId)
          shardMap.assign(op.shardId, selfAddress)
          localEntities.put(op.shardId, new EntityMap)
          taskTimeouts.remove(op.shardId).cancel()
          sender() ! RecoverShardResult(op)
        case entry: ShardEntry =>
          sender() ! ClusterServiceOperationFailed(op, new ApiException(BadRequest))
      }

    // the Put or Migrate task did not complete
    case TaskTimeout(shardId) =>
      shardMap.remove(shardId)
      taskTimeouts.remove(shardId)
      if (!missingShards.containsKey(shardId)) {
        val op = LookupShard(shardId)
        val actor = context.actorOf(LookupShardTask.props(op, coordinator, self, shardTimeout))
        missingShards.put(shardId, actor)
      }

    // update shard map and flush buffered messages
    case result: LookupShardResult =>
      missingShards.remove(result.shardId)
      if (!taskTimeouts.containsKey(result.shardId)) {
        log.debug("assigning shard {} to {}", result.shardId, result.address)
        shardMap.assign(result.shardId, result.address)
        // if shard is local and entity map doesn't exist, create a new entity map
        if (result.address.equals(selfAddress) && !localEntities.containsKey(result.shardId))
          localEntities.put(result.shardId, new EntityMap)
        // flush any buffered messages for newly assigned shards
        flushBuffered()
      }

    // cluster service operation failed
    case failure: ClusterServiceOperationFailed =>
      log.debug("operation {} failed: {}", failure.op, failure.failure)

    // an entity terminated
    case Terminated(ref) =>
      val shardId = entityShards.get(ref)
      val entityMap = localEntities.get(shardId)
      // FIXME
      //entityMap.remove()
  }

  /**
   *
   */
  def deliverEnvelope(envelope: EntityEnvelope): Unit = if (keyExtractor.isDefinedAt(envelope.message)) {
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
          // if the sender is remote, then note the stale shard mapping
          if (!envelope.sender.path.address.equals(selfAddress))
            staleShards.add(StaleShard(shard.shardId, shard.address))
        } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

      // shard is in the process of being assigned to this node
      case shard: PreparingShardEntry =>
        log.debug("shard {} is preparing, buffering {}", shard.shardId, envelope.message)
        bufferedMessages.add(envelope)

      // shard is in the process of being migrated from this node
      case shard: MigratingShardEntry =>
        if (envelope.attempts > 0) {
          val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
          selection ! envelope.copy(attempts = envelope.attempts - 1)
          // if the sender is remote, then note the stale shard mapping
          if (!envelope.sender.path.address.equals(selfAddress))
            staleShards.add(StaleShard(shard.shardId, shard.address))
        } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

      // shard is missing from the shard map
      case shard: MissingShardEntry =>
        log.debug("shard not known for entity {}, buffering {}", shardKey, envelope.message)
        bufferedMessages.add(envelope)
        // if we are not performing a task on the shard...
        if (!taskTimeouts.containsKey(shard.shardId)) {
          // and we are not already trying to look up the shard...
          if (!missingShards.containsKey(shard.shardId)) {
            /// then try to repair the shard map
            val op = LookupShard(shard.shardId)
            val actor = context.actorOf(LookupShardTask.props(op, coordinator, self, shardTimeout))
            missingShards.put(shard.shardId, actor)
          }
        }
    }
  } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(BadRequest))

  /**
   *
   */
  def flushBuffered(): Unit = {
    val iterator = bufferedMessages.listIterator()
    while (iterator.hasNext) {
      val envelope = iterator.next()
      shardMap(shardResolver(envelope.message)) match {
        case entry: AssignedShardEntry =>
          iterator.remove()
          deliverEnvelope(envelope)
        case entry => // do nothing
      }
    }
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
  case class StaleShard(shardId: Int, address: Address)
  case class TaskTimeout(shardId: Int)
}

object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class EntityEnvelope(sender: ActorRef, message: Any, attempts: Int)
case class EntityDeliveryFailed(envelope: EntityEnvelope, failure: Throwable)

