package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.pattern.pipe

import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.model.MetadataPage
import io.mandelbrot.core.registry._
import io.mandelbrot.persistence.cassandra.dal.{AgentGroupMetadata, AgentGroupDAL}

/**
 *
 */
class DescribeGroupTask(op: DescribeGroup,
                        caller: ActorRef,
                        agentGroupDAL: AgentGroupDAL) extends Actor with ActorLogging {
  import context.dispatcher

  // contains the epoch and timestamp where we left off, or None
  val fromInclusive: Boolean = if (op.last.isDefined) false else true

  override def preStart(): Unit = {
    agentGroupDAL.describeGroup(op.groupName, op.last, None, op.limit,
      fromInclusive, toExclusive = false, descending = false).pipeTo(self)
  }

  def receive = {

    /* special case, if no members are found, then return ResourceNotFound */
    case result: AgentGroupMetadata if result.members.isEmpty =>
      caller ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
      context.stop(self)

    /* return members and stop the actor */
    case result: AgentGroupMetadata =>
      val members = result.members
      val page = if (members.length == op.limit) {
        MetadataPage(members, Some(members.last.agentId.toString), exhausted = false)
      } else MetadataPage(members, None, exhausted = true)
      caller ! DescribeGroupResult(op, page)
      context.stop(self)

    /* if we encounter an unhandled exception, then let the supervision strategy handle it */
    case Failure(ex: Throwable) =>
      throw ex
  }
}

object DescribeGroupTask {
  def props(op: DescribeGroup, caller: ActorRef, agentGroupDAL: AgentGroupDAL) = {
    Props(classOf[DescribeGroupTask], op, caller, agentGroupDAL)
  }
}
