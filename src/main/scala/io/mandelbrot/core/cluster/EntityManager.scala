package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._
import io.mandelbrot.core.{ResourceNotFound, BadRequest, ApiException}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.cluster.EntityFunctions.{ShardResolver, PropsCreator, KeyExtractor}

/**
 *
 */
class EntityManager(coordinator: ActorRef,
                    shardResolver: ShardResolver,
                    keyExtractor: KeyExtractor,
                    propsCreator: PropsCreator) extends Actor with ActorLogging {
  import EntityManager._
  import context.dispatcher

  // config
  val selfAddress = Cluster(context.system).selfAddress
  val statsDelay = 30.seconds
  val statsInterval = 1.minute

  // state
  val shardRing = ShardRing()
  val localEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()
  var bufferedMessages = Vector.empty[BufferedEnvelope]
  var lastUpdate = new DateTime(0)
  var updateStats: Option[Cancellable] = None

  override def preStart(): Unit = {
    lastUpdate = new DateTime(0)
    updateStats = Some(context.system.scheduler.schedule(statsDelay, statsInterval, self, PerformUpdate))
  }
  
  override def postStop(): Unit = {
    updateStats.foreach(_.cancel())
    updateStats = None
  }
  
  def receive = {

    // update shard ring and flush buffered messages
    case result: GetShardResult =>
      log.debug("shard {}+{} exists at {}", result.shardId, result.width, result.address)
      shardRing.put(result.shardId, result.width, result.address)
      if (result.address.equals(selfAddress)) {
        localEntities.put(result.shardId, new EntityMap)
      }
      val lowerBound = result.shardId
      val upperBound = result.shardId + result.width
      bufferedMessages = bufferedMessages.filter { buffered =>
          if (shardRing.contains(buffered.shardKey)) {
            self ! buffered.envelope
            false
          } else true
      }

    // update shard allocation statistics
    case PerformUpdate =>
      if (lastUpdate.getMillis > 0) {
        val allocations = localEntities.entrySet().map(e => (e.getKey, e.getValue.size())).toMap
        coordinator ! UpdateMemberShards(allocations)
      }
      lastUpdate = DateTime.now()

    // send the specified message to the entity, which may be remote or local
    case envelope: EntityEnvelope =>
      if (keyExtractor.isDefinedAt(envelope.message)) {
        val shardKey = shardResolver(envelope.message)
        shardRing(shardKey) match {

          // shard is local
          case Some((shardId, address)) if address.equals(selfAddress) =>
            val entityRefs = localEntities.get(shardId)
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
                  entityShards.put(entity, shardId)
                  entity.tell(envelope.message, envelope.sender)
                } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))
              // entity exists, forward the message to it
              case entity: ActorRef =>
                entity.tell(envelope.message, envelope.sender)
            }

          // shard is remote and its location is cached
          case Some((shardId, address)) =>
            if (envelope.attempts > 0) {
              val selection = context.system.actorSelection(RootActorPath(address) / self.path.elements)
              selection ! envelope.copy(attempts = envelope.attempts - 1)
            } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

          // shard is remote and there is no cached location
          case None =>
            bufferedMessages = bufferedMessages :+ BufferedEnvelope(envelope, shardKey, DateTime.now())
            coordinator ! GetShard(shardKey)
        }
      } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(BadRequest))
  }
  
}

object EntityManager {
  def props(coordinator: ActorRef, shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[EntityManager], coordinator, shardResolver, keyExtractor, propsCreator)
  }

  class EntityMap extends util.HashMap[String,ActorRef]
  case class BufferedEnvelope(envelope: EntityEnvelope, shardKey: Int, timestamp: DateTime)
  case object PerformUpdate
}

object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class EntityEnvelope(sender: ActorRef, message: Any, attempts: Int)
case class RedirectNack(message: Any, attempts: Int)

case class EntityDeliveryFailed(envelope: EntityEnvelope, failure: Throwable)
