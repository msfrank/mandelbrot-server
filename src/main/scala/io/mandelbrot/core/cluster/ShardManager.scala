package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.{UniqueAddress, Member, Cluster}
import akka.cluster.ClusterEvent._

/**
 *
 */
class ShardManager extends LoggingFSM[ShardManagerFSMState,ShardManagerFSMData] {

  // config
  val numBuckets = 64
  val minNrMembers = 1

  // state
  var shards = new ShardRing()
  var refCache = Map.empty[Address,ActorRef]
  var regions = Map.empty[Int,ActorRef]
  var lastSeen = 0

  override def preStart(): Unit = {
    refCache = Map(Cluster(context.system).selfAddress -> self)
    Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[ClusterDomainEvent])
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
    refCache = Map.empty
  }

  startWith(Incubating, Incubating(None, Map.empty))

  when(Incubating) {

    case Event(leaderChanged: LeaderChanged, state: Incubating) =>
      leaderChanged.leader match {
        case Some(address) if state.members.contains(address) =>
          log.debug("{} is now the leader", state.members(address))
          if (state.members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members)
            else
              goto(Follower) using Follower(state.members(address), state.members)
          } else stay() using Incubating(leaderChanged.leader, state.members)
        case Some(address) =>
          log.warning("{} is now the leader but there is no known member with that address", address)
          stay() using Incubating(leaderChanged.leader, state.members)
        case None =>
          log.warning("leader is lost")
          stay() using Incubating(leaderChanged.leader, state.members)
      }

    case Event(MemberUp(member), state: Incubating) =>
      log.debug("member {} is UP", member)
      val members = state.members + (member.address -> member)
      state.leader match {
        case Some(address) if !state.members.contains(address) =>
          stay() using Incubating(state.leader, members)
        case Some(address) =>
          if (members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members)
            else
              goto(Follower) using Follower(state.members(address), state.members)
          } else stay() using Incubating(state.leader, members)
        case None =>
          stay() using Incubating(state.leader, members)
      }

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  onTransition {
    case Incubating -> Follower =>
      context.system.eventStream.publish(ShardClusterUp)
      log.debug("shard cluster is UP, we are a follower")
      nextStateData match {
        case Follower(leader, members) =>
          context.actorSelection(RootActorPath(leader.address) / self.path.elements) ! PullShards(Cluster(context.system).selfUniqueAddress)
      }
    case Incubating -> Leader =>
      context.system.eventStream.publish(ShardClusterUp)
      log.debug("shard cluster is UP, we are the leader")

    case _ -> Incubating =>
      context.system.eventStream.publish(ShardClusterDown)
      log.debug("shard cluster is DOWN, we incubate")
  }

  when(Leader) {

    case Event(LeaderChanged(Some(address)), _) =>
      stay()

    case Event(MemberUp(member), Leader(members)) =>
      stay() using Leader(members + (member.address -> member))

    case Event(MemberRemoved(member, status), Leader(members)) =>
      stay() using Leader(members - member.address)

    case Event(_: ClusterDomainEvent, _) =>
      stay()

    case Event(envelope: ShardEnvelope, _) =>
      stay()
  }

  when(Follower) {

    case Event(LeaderChanged(Some(address)), _) =>
      stay()

    case Event(MemberUp(member), Follower(leader, members)) =>
      stay() using Follower(leader, members + (member.address -> member))

    case Event(MemberRemoved(member, status), Follower(leader, members)) =>
      stay() using Follower(leader, members - member.address)

    case Event(_: ClusterDomainEvent, _) =>
      stay()

    case Event(envelope: ShardEnvelope, _) =>
      shards(envelope.address) match {
        case Some(shardAddress) if shardAddress == selfAddress =>
      }
      stay()
  }

  def selfAddress = Cluster(context.system).selfAddress
  def selfUniqueAddress = Cluster(context.system).selfUniqueAddress

  initialize()
}

object ShardManager {
  def props() = Props(classOf[ShardManager])
}

sealed trait ShardManagerFSMState
case object Incubating extends ShardManagerFSMState
case object Leader extends ShardManagerFSMState
case object Follower extends ShardManagerFSMState

sealed trait ShardManagerFSMData
case class Incubating(leader: Option[Address], members: Map[Address,Member]) extends ShardManagerFSMData
case class Leader(members: Map[Address,Member]) extends ShardManagerFSMData
case class Follower(leader: Member, members: Map[Address,Member]) extends ShardManagerFSMData

sealed trait ShardManagerEvent
case object ShardClusterUp extends ShardManagerEvent
case object ShardClusterDown extends ShardManagerEvent

case class ShardEnvelope(address: String, message: Any)
case class DeliverMessage(address: String, message: Any)

/**
 *
 */
class ShardRing() extends Serializable {
  private val shards = new java.util.TreeMap[Int,Address]()

  def apply(key: String): Option[Address] = Option(shards.floorEntry(key.hashCode())).map(_.getValue)
  def size = shards.size()
  def isEmpty = shards.isEmpty
  def nonEmpty = !isEmpty

  //def split()
  //def merge()
}
