package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.cluster.EntityFunctions.{PropsCreator, KeyExtractor}

/**
 *
 */
class EntityManager(coordinator: ActorRef, keyExtractor: KeyExtractor, propsCreator: PropsCreator) extends Actor with ActorLogging {
  import EntityManager._
  import context.dispatcher

  // config
  val updateAllocationsDelay = 30.seconds
  val updateAllocationsInterval = 1.minute
  val selfAddress = Cluster(context.system).selfAddress

  // state
  val shardRing = ShardRing()
  val shardEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()
  var lastUpdate = new DateTime(0)
  var updateAllocations: Option[Cancellable] = None

  override def preStart(): Unit = {
    lastUpdate = new DateTime(0)
    updateAllocations = Some(context.system.scheduler.schedule(updateAllocationsDelay,
      updateAllocationsInterval, self, PerformUpdateStatistics))
  }
  
  override def postStop(): Unit = {
    updateAllocations.foreach(_.cancel())
    updateAllocations = None
  }
  
  def receive = {

    // notify the shard manager that the cluster is up
    case event: ClusterUp =>
      coordinator ! event

    // notify the shard manager that the cluster is down
    case event: ClusterDown =>
      coordinator ! event

    // update shard allocation statistics
    case PerformUpdateStatistics =>
      if (lastUpdate.getMillis > 0) {
        val allocations = shardEntities.entrySet().map(e => (e.getKey, e.getValue.size())).toMap
        coordinator ! UpdateMemberShards(allocations)
      }
      lastUpdate = DateTime.now()

    // a message was redirected to this member from elsewhere
    case RedirectMessage(message, attempts) =>

    // send the specified message to the entity, which may be remote or local
    case message: Any =>
      if (keyExtractor.isDefinedAt(message)) {
        val key = keyExtractor(message)
        shardRing(resolveShard(key)) match {

          // shard is local
          case Some((shard, address)) if address.equals(selfAddress) =>
            val entityRefs = shardEntities.get(shard)
            entityRefs.get(key) match {
              // entity doesn't exist in shard, so create it
              case null =>
                val props = propsCreator(message)
                val entity = context.actorOf(props)
                context.watch(entity)
                entityRefs.put(key, entity)
                entityShards.put(entity, shard)
                entity forward message
              // entity exists, forward the message to it
              case entity: ActorRef =>
                entity forward message
            }

          // shard is remote and its location is cached
          case Some((shard, address)) =>
            val selection = context.system.actorSelection(RootActorPath(address) / self.path.elements)
            selection forward RedirectMessage(message, attempts = 3)

          // shard is remote and there is no cached location
          case None =>
            coordinator ! GetShard(resolveShard(key))
        }
      } else log.debug("dropped message {} because no key extractor could be found", message)
  }

  /**
   *
   */
  def resolveShard(key: String): Int = MurmurHash3.stringHash(key)
}

object EntityManager {
  def props(coordinator: ActorRef, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[EntityManager], coordinator, keyExtractor, propsCreator)
  }

  class EntityMap extends util.HashMap[String,ActorRef]
  case object PerformUpdateStatistics
}

object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class RedirectMessage(message: Any, attempts: Int)
case class RedirectNack(message: Any, attempts: Int)
