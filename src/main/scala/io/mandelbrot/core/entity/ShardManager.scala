/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.entity

import akka.actor._
import akka.pattern._
import akka.contrib.pattern.DistributedPubSubMediator.SendToAll
import scala.concurrent.duration._
import java.util

import io.mandelbrot.core.entity.EntityFunctions.{PropsCreator,EntityReviver}
import io.mandelbrot.core._

/**
 * The ShardManager receives EntityEnvelope messages and delivers them to the
 * appropriate shard, which may be local or remote.  The shard may also be in a
 * non-assigned state, in which case the envelope will be buffered until the
 * delivery can be completed.
 *
 * In order to deliver envelopes to the correct destination, the ShardManager
 * must maintain a cached copy of the shard map.  if a StaleShard message is
 * received, then the ShardManager must invalidate the current mapping and query
 * the coordinator for the up-to-date mapping.  The ShardManager also interacts
 * with the BalancerTask to acquire and release shards as needed to keep shards
 * balanced across the cluster.
 */
class ShardManager(services: ActorRef,
                   propsCreator: PropsCreator,
                   entityReviver: EntityReviver,
                   selfAddress: Address,
                   totalShards: Int,
                   gossiper: ActorRef) extends Actor with ActorLogging {
  import ShardManager._
  import context.dispatcher

  // config
  val listShardsLimit = 100
  val servicesTimeout = 5.seconds
  val taskTimeout = 5.seconds
  val lookupTimeout = 5.seconds
  val flushTimeout = 5.seconds

  // state
  val shardMap = ShardMap(totalShards)
  val entitiesByShard = new util.HashMap[Int,ActorRef]()
  val shardForEntities = new util.HashMap[ActorRef,Int]()
  val bufferedMessages = new util.ArrayList[EntityEnvelope]()
  val taskTimeouts = new util.HashMap[Int,Cancellable]()
  val missingShards = new util.HashMap[Int,ActorRef]
  var observedStaleShards = Set.empty[StaleShard]
  var flushStaleShards: Option[Cancellable] = None

  override def preStart(): Unit = self ! Retry

  override def postStop(): Unit = flushStaleShards.foreach(_.cancel())

  def receive = initializing

  /**
   * initializing behavior for the ShardManager is to construct the shard map from the
   * coordinator, creating ShardEntities actors for any locally hosted shards.
   */
  def initializing: Receive = {

    case Retry =>
      services.ask(ListShards(listShardsLimit, None))(servicesTimeout).pipeTo(self)

    // FIXME: iterate shards if map is larger than limit
    case ListShardsResult(op, shards, token) =>
      // update the shard map and create any local shard entities
      shards.foreach { case Shard(shardId, address) =>
        shardMap.assign(shardId, address)
        if (selfAddress.equals(StandaloneAddress) && !address.equals(selfAddress)) {
          log.warning("ignoring shard {} with address {}", shardId, address)
        }
        // if shard is local and entity map doesn't exist, create a new entity map
        else if (selfAddress.equals(StandaloneAddress) || address.equals(selfAddress)) {
          val shardEntities = context.actorOf(ShardEntities.props(services, propsCreator, entityReviver, shardId))
          entitiesByShard.put(shardId, shardEntities)
          shardForEntities.put(shardEntities, shardId)
          log.debug("created entity map for shard {}", shardId)
        }
      }
      log.debug("initialized shard map with {} shards, {} missing", shardMap.size, shardMap.numMissing)
      // start lookup tasks for any missing shards
      shardMap.missing.foreach { case entry: MissingShardEntry =>
        val op = LookupShard(entry.shardId)
        val actor = context.actorOf(LookupShardTask.props(op, services, self, lookupTimeout))
        missingShards.put(entry.shardId, actor)
      }
      context.become(running)
      // flush any envelopes which were received while initializing
      flushBuffered()

    case failure: EntityServiceOperationFailed =>
      log.debug("failed to list shards: {}", failure.failure.getCause)
      context.system.scheduler.scheduleOnce(servicesTimeout, self, Retry)

    case failure: AskTimeoutException =>
      log.debug("failed to list shards: timed out")
      context.system.scheduler.scheduleOnce(servicesTimeout, self, Retry)

    case op: ShardBalancerOperation =>
      sender() ! ShardBalancerOperationFailed(op, ApiException(RetryLater))

    case envelope: EntityEnvelope =>
      deliverEnvelope(envelope)

    // return the shard map entries
    case op: GetShardMapStatus =>
      sender() ! GetShardMapStatusResult(op, ShardMapStatus(shardMap.shards.toSet, totalShards))

    // a shard terminated  
    case Terminated(ref) =>
      val shardId = shardForEntities.remove(ref)
      entitiesByShard.remove(shardId)
      log.debug("entity map terminated for shard {}", shardId)
  }

  /**
   * running behavior for the ShardManager
   */
  def running: Receive = {

    // send the specified message to the entity, which may be remote or local
    case envelope: EntityEnvelope =>
      deliverEnvelope(envelope)

    // start buffering messages in preparation to own shard
    case op: PrepareShard =>
      shardMap.get(op.shardId) match {
        case entry: AssignedShardEntry =>
          sender() ! ShardBalancerOperationFailed(op, ApiException(BadRequest))
        case entry: ShardEntry if taskTimeouts.containsKey(op.shardId) =>
          sender() ! ShardBalancerOperationFailed(op, ApiException(RetryLater))
        case entry: ShardEntry =>
          log.debug("{} says prepare shardId {}", sender().path, op.shardId)
          shardMap.prepare(op.shardId, selfAddress)
          taskTimeouts.put(op.shardId, context.system.scheduler.scheduleOnce(taskTimeout, self, TaskTimeout(op.shardId)))
          sender() ! PrepareShardResult(op)
      }

    // change preparing shard to assigned
    case op: RecoverShard =>
      shardMap.get(op.shardId) match {
        case entry: PreparingShardEntry if !taskTimeouts.containsKey(op.shardId) =>
          sender() ! ShardBalancerOperationFailed(op, ApiException(RetryLater))
        case entry: PreparingShardEntry =>
          log.debug("{} says recover shardId {}", sender().path, op.shardId)
          shardMap.assign(op.shardId, selfAddress)
          val shardEntities = context.actorOf(ShardEntities.props(services, propsCreator, entityReviver, entry.shardId))
          entitiesByShard.put(op.shardId, shardEntities)
          shardForEntities.put(shardEntities, op.shardId)
          log.debug("created entity map for shard {}", entry.shardId)
          taskTimeouts.remove(op.shardId).cancel()
          sender() ! RecoverShardResult(op)
        case entry: ShardEntry =>
          sender() ! ShardBalancerOperationFailed(op, ApiException(BadRequest))
      }

    // the Put or Migrate task did not complete
    case TaskTimeout(shardId) =>
      shardMap.remove(shardId)
      taskTimeouts.remove(shardId)
      if (!missingShards.containsKey(shardId)) {
        val op = LookupShard(shardId)
        val actor = context.actorOf(LookupShardTask.props(op, services, self, lookupTimeout))
        missingShards.put(shardId, actor)
      }

    // update shard map and flush buffered messages
    case result: LookupShardResult =>
      missingShards.remove(result.shardId)
      if (!taskTimeouts.containsKey(result.shardId)) {
        log.debug("assigning shard {} to {}", result.shardId, result.address)
        shardMap.assign(result.shardId, result.address)
        // if shard is local and entity map doesn't exist, create a new entity map
        if (result.address.equals(selfAddress) && !entitiesByShard.containsKey(result.shardId)) {
          val shardEntities = context.actorOf(ShardEntities.props(services, propsCreator, entityReviver, result.shardId))
          entitiesByShard.put(result.shardId, shardEntities)
          shardForEntities.put(shardEntities, result.shardId)
          log.debug("created entity map for shard {}", result.shardId)
        }
        // flush any buffered messages for newly assigned shards
        flushBuffered()
      }

    // cluster service operation failed
    case failure: EntityServiceOperationFailed =>
      log.debug("operation {} failed: {}", failure.op, failure.failure)

    // periodically publish the set of observed stale shards to all peers
    case FlushStaleShards =>
      flushStaleShards = None
      gossiper ! SendToAll(self.path.toString, StaleShardSet(observedStaleShards), allButSelf = true)
      observedStaleShards = Set.empty

    // invalidate any stale assigned shards
    case StaleShardSet(staleShards) =>
      staleShards.foreach { case StaleShard(shardId, staleAddress) =>
        shardMap.get(shardId) match {
          // if the shard marked assigned, is remote, and the entry matches the gossip, then
          // set the shard to missing and get the current value from the coordinator.
          case AssignedShardEntry(_, address) if !address.equals(selfAddress) && address.equals(staleAddress) =>
            log.debug("invalidated stale assigned shard {} mapped to {}", shardId, address)
            shardMap.remove(shardId)
            val op = LookupShard(shardId)
            val actor = context.actorOf(LookupShardTask.props(op, services, self, lookupTimeout))
            missingShards.put(shardId, actor)
          // otherwise if the shard is not assigned, or is assigned locally, do nothing
          case entry =>
        }
      }

    // return the shard map entries
    case op: GetShardMapStatus =>
      sender() ! GetShardMapStatusResult(op, ShardMapStatus(shardMap.shards.toSet, totalShards))

    // a shard terminated
    case Terminated(ref) =>
      val shardId = shardForEntities.remove(ref)
      entitiesByShard.remove(shardId)
      log.debug("entity map terminated for shard {}", shardId)
  }

  /**
   *
   */
  def deliverEnvelope(envelope: EntityEnvelope): Unit = {
    shardMap(envelope.shardKey) match {

      // shard is assigned to this node
      case shard: AssignedShardEntry if shard.address.equals(selfAddress) =>
        entitiesByShard.get(shard.shardId) match {
          // entity doesn't exist in shard
          case null =>
            envelope.sender ! EntityDeliveryFailed(envelope.op, ApiException(RetryLater))
          // entity exists, forward the message to it
          case entity: ActorRef =>
            entity forward envelope
        }

      // shard is assigned to a remote node
      case shard: AssignedShardEntry =>
        if (envelope.attemptsLeft > 0) {
          val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
          selection ! envelope.copy(attemptsLeft = envelope.attemptsLeft - 1)
        } else envelope.sender ! EntityDeliveryFailed(envelope.op, ApiException(ResourceNotFound))
        // if the sender is remote, then note the stale shard mapping
        if (!envelope.sender.path.address.equals(selfAddress) && envelope.attemptsLeft < envelope.maxAttempts)
          markStaleShard(shard.shardId, selfAddress)

      // shard is in the process of being assigned to this node
      case shard: PreparingShardEntry =>
        log.debug("shard {} is preparing, buffering {}", shard.shardId, envelope.op)
        bufferedMessages.add(envelope)

      // shard is in the process of being migrated from this node
      case shard: MigratingShardEntry =>
        if (envelope.attemptsLeft > 0) {
          val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
          selection ! envelope.copy(attemptsLeft = envelope.attemptsLeft - 1)
        } else envelope.sender ! EntityDeliveryFailed(envelope.op, ApiException(ResourceNotFound))
        // if the sender is remote, then note the stale shard mapping
        if (!envelope.sender.path.address.equals(selfAddress) && envelope.attemptsLeft < envelope.maxAttempts)
          markStaleShard(shard.shardId, selfAddress)

      // shard is missing from the shard map
      case shard: MissingShardEntry =>
        log.debug("shard {} is missing, buffering {}", shard.shardId, envelope.op)
        bufferedMessages.add(envelope)
        // if we are not performing a task on the shard...
        if (!taskTimeouts.containsKey(shard.shardId)) {
          // and we are not already trying to look up the shard...
          if (!missingShards.containsKey(shard.shardId)) {
            /// then try to repair the shard map
            val op = LookupShard(shard.shardId)
            val actor = context.actorOf(LookupShardTask.props(op, services, self, lookupTimeout))
            missingShards.put(shard.shardId, actor)
          }
        }
    }
  }

  /**
   *
   */
  def flushBuffered(): Unit = {
    val iterator = bufferedMessages.listIterator()
    if (iterator.hasNext)
      log.debug("flushing buffered messages")
    while (iterator.hasNext) {
      val envelope = iterator.next()
      shardMap(envelope.shardKey) match {
        case entry: AssignedShardEntry =>
          iterator.remove()
          log.debug("delivering buffered message {}", envelope.op)
          deliverEnvelope(envelope)
        case entry => // do nothing
      }
    }
  }

  /**
   *
   */
  def markStaleShard(shardId: Int, address: Address): Unit = {
    val staleShard = StaleShard(shardId, address)
    if (!observedStaleShards.contains(staleShard))
      observedStaleShards = observedStaleShards + staleShard
    if (flushStaleShards.isEmpty)
      flushStaleShards = Some(context.system.scheduler.scheduleOnce(5.seconds, self, FlushStaleShards))
  }
}

object ShardManager {

  val StandaloneAddress = Address("local","mandelbrot", "localhost", 0)

  def props(services: ActorRef,
            propsCreator: PropsCreator,
            entityReviver: EntityReviver,
            selfAddress: Address,
            totalShards: Int,
            gossiper: ActorRef) = {
    Props(classOf[ShardManager], services, propsCreator, entityReviver, selfAddress, totalShards, gossiper)
  }

  case object Retry
  case object FlushStaleShards
  case class TaskTimeout(shardId: Int)
}


object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
  type EntityReviver = PartialFunction[String,Any]
}

case class EntityEnvelope(sender: ActorRef, op: ServiceOperation, shardKey: Int, entityKey: String, attemptsLeft: Int, maxAttempts: Int)
case class EntityDeliveryFailed(op: ServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class StaleShard(shardId: Int, address: Address)
case class StaleShardSet(staleShards: Set[StaleShard])