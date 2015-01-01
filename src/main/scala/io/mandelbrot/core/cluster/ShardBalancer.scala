package io.mandelbrot.core.cluster

import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable

import ShardBalancer.{State, Data}

/**
 *
 */
class ShardBalancer(coordinator: ActorRef, monitor: ActorRef, nodes: Map[Address,ActorPath], totalShards: Int, initialWidth: Int) extends LoggingFSM[State,Data] {
  import ShardBalancer._

  // config
  val timeout = 5.seconds

  // state
  val shardMap = ShardMap(totalShards, initialWidth)
  val shardDensity = new mutable.HashMap[Address,Int]()
  var missingShards = Set.empty[Int]
  var addedNodes = Set.empty[Address]
  var removedNodes = Set.empty[Address]

  override def preStart(): Unit = {
    // get the current location of all shards
    coordinator ! GetAllShards()
  }

  startWith(Initializing, NoData)

  when(Initializing) {

    // gather the current state of all shards
    case Event(result: GetAllShardsResult, NoData) =>

      // build shardMap and shardDensity from coordinator data
      result.shards.foreach { case (shardId, address) =>
        shardMap.put(shardId, address)
        val numShards = shardDensity.getOrElse(address, 0)
        shardDensity.put(address, numShards + 1)
      }

      // find shards which are not mapped to any address
      missingShards = shardMap.missing

      // find nodes which have been added or removed since the last balancing
      addedNodes = nodes.keySet diff shardDensity.keySet
      removedNodes = shardDensity.keySet.toSet diff nodes.keySet

      // transition to the next State
      balance()
  }

  when(Repairing) {

    // update state and process the next operation, if any
    case Event(result: PutShardComplete, state: Repairing) =>
      val PutShard(shardId, actorPath) = result.op
      val address = actorPath.address
      shardMap.put(shardId, address)
      val numShards = shardDensity.getOrElse(address, 0)
      shardDensity.put(address, numShards + 1)
      missingShards = missingShards - shardId
      if (state.queued.nonEmpty) {
        val inflight = context.actorOf(PutShardTask.props(state.queued.head, coordinator, self, timeout))
        stay() using Repairing(inflight, state.queued.tail)
      } else balance()

    // the operation failed for some reason
    case Event(result: PutShardFailed, state: Repairing) =>
      val PutShard(shardId, actorPath) = result.op
      log.debug("failed to put shard {} at {}: {}", shardId, actorPath.address, result.ex)
      if (state.queued.nonEmpty) {
        val inflight = context.actorOf(PutShardTask.props(state.queued.head, coordinator, self, timeout))
        stay() using Repairing(inflight, state.queued.tail)
      } else balance()
  }

  /**
   * decide the appropriate State to transition to based on the current state.
   */
  def balance(): State = {
    if (missingShards.nonEmpty) {
      // sort addresses by density
      val addressesSortedByDensity = mutable.PriorityQueue()(ordering)
      shardDensity.foreach(addressesSortedByDensity.enqueue(_))
      // create PutShard operations for any missing shards
      val ops = shardMap.missing.map { missingShardId =>
        val (address,numShards) = addressesSortedByDensity.dequeue()
        addressesSortedByDensity.enqueue((address, numShards + 1))
        PutShard(missingShardId, nodes(address))
      }.toVector
      // put the first operation in flight
      val inflight = context.actorOf(PutShardTask.props(ops.head, coordinator, self, timeout))
      goto(Repairing) using Repairing(inflight, ops.tail)
    } else {
      monitor ! ShardBalancerResult(shardMap)
      stop()
    }
  }

  /**
   * calculate the standard deviation from the specified iterable of densities.
   */
  def calculateDensityDeviation(densities: Iterable[Int]): Double = {
    val cardinality = densities.size
    val mean = densities.reduce(_ + _) / cardinality
    val variance = densities.foldLeft(0.toDouble) { case (acc,n) => math.pow(n - mean, 2) + acc } / cardinality
    math.sqrt(variance)
  }

  // must be at the end of the constructor
  initialize()
}

object ShardBalancer {
  def props(coordinator: ActorRef, monitor: ActorRef, nodes: Map[Address,ActorPath], totalShards: Int, initialWidth: Int) = {
    Props(classOf[ShardBalancer], coordinator, monitor, nodes, totalShards, initialWidth)
  }

  val ordering = Ordering.by[(Address,Int),Int](_._2).reverse

  case class MigrateShard(shardId: Int, targetNode: Address, sourceNode: Option[ActorPath])

  sealed trait State
  case object Initializing extends State
  case object Migrating extends State
  case object Repairing extends State

  sealed trait Data
  case object NoData extends Data
  case class Migrating(inflight: ActorRef, queued: Vector[MoveShard]) extends Data
  case class Repairing(inflight: ActorRef, queued: Vector[PutShard]) extends Data
}

case object ShardTaskTimeout

case class ShardBalancerResult(shardMap: ShardMap)