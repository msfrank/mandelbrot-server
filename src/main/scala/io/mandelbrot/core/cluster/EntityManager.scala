package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import java.util

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.cluster.EntityFunctions.{PropsCreator, KeyExtractor, ShardResolver}

/**
 *
 */
class EntityManager(shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) extends Actor with ActorLogging {
  import EntityManager.EntityMap

  val settings = ServerConfig(context.system).settings.cluster
  val selfAddress = Cluster(context.system).selfAddress

  val shardRing = ShardRing()
  val shardEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()

  val shardManager = context.actorOf(ShardManager.props(settings.minNrMembers, settings.initialShardCount), "shard-manager")

  def receive = {

    case proposal: RebalanceProposal =>
      // perform rebalancing
      sender() ! AppliedProposal(proposal)

    // send message to the entity, which may be remote or local
    case message: Any if keyExtractor.isDefinedAt(message) =>
      shardRing(shardResolver(message)) match {
        // shard exists and is local
        case Some((shard, address)) if address.equals(selfAddress) =>
          val entityRefs = shardEntities.get(shard)
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
        // shard exists and is remote
        case Some((shard, address)) =>

        // shard doesn't exist
        case None =>
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

case class GetEntityShards()
case class GetEntityShardsResult(shardCosts: Map[Int,Int])
