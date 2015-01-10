package io.mandelbrot.core.cluster

import akka.actor._
import akka.pattern._
import scala.concurrent.duration._

class LookupShardTask(op: LookupShard,
                      coordinator: ActorRef,
                      monitor: ActorRef,
                      timeout: FiniteDuration) extends Actor with ActorLogging {
  import LookupShardTask.PerformQuery
  import context.dispatcher

  // config
  val delay = 5.seconds

  log.debug("attempting to resolve shard for key {}", op.shardKey)

  override def preStart(): Unit = self ! PerformQuery
  
  def receive = {

    case PerformQuery =>
      coordinator.ask(GetShard(op.shardKey))(timeout).pipeTo(self)

    case result: GetShardResult if result.address.isEmpty =>
      context.system.scheduler.scheduleOnce(delay, self, PerformQuery)
      
    case GetShardResult(_, shardId, width, Some(address)) =>
      context.stop(self)
      monitor ! LookupShardResult(op, shardId, width, address)
      
    case failure: ClusterServiceOperationFailed =>
      context.system.scheduler.scheduleOnce(delay, self, PerformQuery)

    case failure: AskTimeoutException =>
      context.system.scheduler.scheduleOnce(delay, self, PerformQuery)
  }
}

object LookupShardTask {
  def props(op: LookupShard, coordinator: ActorRef, monitor: ActorRef, timeout: FiniteDuration) = {
    Props(classOf[LookupShardTask], op, coordinator, monitor, timeout)
  }
  case object PerformQuery
}

case class LookupShard(shardKey: Int)
case class LookupShardResult(op: LookupShard, shardId: Int, width: Int, address: Address)
