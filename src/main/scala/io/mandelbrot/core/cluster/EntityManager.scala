package io.mandelbrot.core.cluster

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import java.util

import io.mandelbrot.core.cluster.EntityFunctions.{PropsCreator, KeyExtractor, ShardResolver}

/**
 *
 */
class EntityManager(shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) extends Actor with ActorLogging {
  import EntityManager.EntityMap

  val shardRing = ShardRing()
  val shardEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()

  def receive = {

    case proposal @ RebalanceProposal(op, proposer, mutations) =>
      // perform rebalancing
      sender() ! AppliedProposal(proposal)

    case message: Any =>
      try {
        val shard = shardResolver(message)
        shardEntities.get(shard) match {
          // shard is remote, so send message to peer
          case null =>

          // shard is local, so find the entity by extracting the key
          case entityRefs: EntityMap =>
            val key = keyExtractor(message)
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
        }
      } catch {
        case ex: Throwable =>
      }
  }
}

object EntityManager {
  def props(shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[EntityManager], shardResolver, keyExtractor, propsCreator)
  }

  class EntityMap extends util.HashMap[String,ActorRef]
}

object EntityFunctions {
  type ShardResolver = PartialFunction[Any,Int]
  type KeyExtractor = PartialFunction[Any,String]
  type PropsCreator = PartialFunction[Any,Props]
}

case class RebalanceProposal(lsn: Int, proposer: ActorRef, mutations: Vector[ShardRingMutation])
case class AppliedProposal(proposal: RebalanceProposal)
