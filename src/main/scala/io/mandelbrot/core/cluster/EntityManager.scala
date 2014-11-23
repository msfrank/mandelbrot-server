package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._
import scala.util.hashing.MurmurHash3
import java.util

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.cluster.EntityFunctions.{PropsCreator, KeyExtractor}

/**
 *
 */
class EntityManager(keyExtractor: KeyExtractor, propsCreator: PropsCreator) extends Actor with ActorLogging {
  import EntityManager.EntityMap

  val settings = ServerConfig(context.system).settings.cluster
  val selfAddress = Cluster(context.system).selfAddress

  val shardRing = ShardRing()
  val shardEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()

  val shardManager = context.actorOf(ShardManager.props(), "shard-manager")

  def receive = {

    // notify the shard manager that the cluster is up
    case state: ClusterUp =>
      shardManager ! state

    // notify the shard manager that the cluster is down
    case state: ClusterDown =>
      shardManager ! state

    // FIXME: this is an unnecessary message used for testing, we should remove this
    case ShardsRebalanced =>
      // do nothing

    // apply the mutations specified in the rebalance proposal
    case proposal: RebalanceProposal =>
      // perform rebalancing
      sender() ! AppliedProposal(proposal)

    // send the specified message to the entity, which may be remote or local
    case message: Any if keyExtractor.isDefinedAt(message) =>
      val key = keyExtractor(message)
      shardRing(resolveShard(key)) match {
        // shard exists and is local
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
        // shard exists and is remote
        case Some((shard, address)) =>
          val selection = context.system.actorSelection(RootActorPath(address) / self.path.elements)
          selection forward message

        // shard doesn't exist
        case None =>
          log.debug("dropped message {} because no shard could be found", message)
      }
  }

  /**
   *
   */
  def resolveShard(key: String): Int = MurmurHash3.stringHash(key)
}

object EntityManager {
  def props(keyExtractor: KeyExtractor, propsCreator: PropsCreator) = Props(classOf[EntityManager], keyExtractor, propsCreator)

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
