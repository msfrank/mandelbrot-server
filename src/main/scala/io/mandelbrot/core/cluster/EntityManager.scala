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
class EntityManager(keyExtractor: KeyExtractor, propsCreator: PropsCreator, totalShards: Int) extends Actor with ActorLogging {
  import EntityManager.EntityMap

  // config
  val settings = ServerConfig(context.system).settings.cluster
  val selfAddress = Cluster(context.system).selfAddress

  // state
  val shardRing = ShardRing()
  val shardEntities = new util.HashMap[Int,EntityMap]()
  val entityShards = new util.HashMap[ActorRef,Int]()

  val shardManager = context.actorOf(ShardManager.props(), "shard-manager")

  def receive = {

    // notify the shard manager that the cluster is up
    case event: ClusterUp =>
      shardManager ! event

    // notify the shard manager that the cluster is down
    case event: ClusterDown =>
      shardManager ! event

    case op: RequestProposal =>


    // apply the mutations specified in the rebalance proposal
    case op: ApplyProposal =>
      // FIXME: perform rebalancing
      sender() ! ApplyProposalResult(op)

    // FIXME: this is an unnecessary message used for testing, we should remove this
    case ShardsRebalanced =>
      // do nothing

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
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class RequestProposal(lsn: Int, hatched: Boolean)
case class RequestProposalResult(op: RequestProposal, mutations: Vector[ShardRingMutation])

case class ApplyProposal(lsn: Int, proposer: ActorRef, mutations: Vector[ShardRingMutation])
case class ApplyProposalResult(proposal: ApplyProposal)
