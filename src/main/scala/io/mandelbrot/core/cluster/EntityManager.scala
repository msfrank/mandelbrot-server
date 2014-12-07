package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._
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
  val statsDelay = 30.seconds
  val statsInterval = 1.minute
  val selfAddress = Cluster(context.system).selfAddress

  // state
  val shardRing = ShardRing()
  val localEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()
  var bufferedMessages = Vector.empty[BufferedMessage]
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
            self.tell(buffered.message, buffered.sender)
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

    // a message was redirected to this member from elsewhere
    case redirect: RedirectMessage =>
      log.debug("dropping redirect message {}", redirect)

    // send the specified message to the entity, which may be remote or local
    case message: Any =>
      if (keyExtractor.isDefinedAt(message)) {
        val shardKey = shardResolver(message)
        shardRing(shardKey) match {

          // shard is local
          case Some((shardId, address)) if address.equals(selfAddress) =>
            val entityRefs = localEntities.get(shardId)
            val entityKey = keyExtractor(message)
            entityRefs.get(entityKey) match {
              // entity doesn't exist in shard, so create it
              case null =>
                val props = propsCreator(message)
                val entity = context.actorOf(props)
                context.watch(entity)
                entityRefs.put(entityKey, entity)
                entityShards.put(entity, shardId)
                entity forward message
              // entity exists, forward the message to it
              case entity: ActorRef =>
                entity forward message
            }

          // shard is remote and its location is cached
          case Some((shardId, address)) =>
            val selection = context.system.actorSelection(RootActorPath(address) / self.path.elements)
            selection ! RedirectMessage(sender(), message, attempts = 3)

          // shard is remote and there is no cached location
          case None =>
            bufferedMessages = bufferedMessages :+ BufferedMessage(sender(), message, shardKey, DateTime.now())
            coordinator ! GetShard(shardKey)
        }
      } else log.debug("dropped message {} because no key extractor could be found", message)
  }

  /**
   *
   */
  def resolveShard(key: String): Int = MurmurHash3.stringHash(key)
}

object EntityManager {
  def props(coordinator: ActorRef, shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[EntityManager], coordinator, shardResolver, keyExtractor, propsCreator)
  }

  class EntityMap extends util.HashMap[String,ActorRef]
  case class BufferedMessage(sender: ActorRef, message: Any, shardKey: Int, timestamp: DateTime)
  case object PerformUpdate
}

object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class RedirectMessage(sender: ActorRef, message: Any, attempts: Int)
case class RedirectNack(message: Any, attempts: Int)
