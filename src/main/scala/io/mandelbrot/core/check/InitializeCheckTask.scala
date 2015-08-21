package io.mandelbrot.core.check

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.collection.mutable

import io.mandelbrot.core.model._
import io.mandelbrot.core.state._

/**
 *
 */
class InitializeCheckTask(checkRef: CheckRef,
                          generation: Long,
                          initializers: Map[CheckId,CheckInitializer],
                          caller: ActorRef,
                          services: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher

  // config
  val queryTimeout = 1.second

  // state
  var inflight: Set[CheckId] = initializers.keySet
  val results = new mutable.HashMap[CheckId,Vector[CheckStatus]]

  override def preStart(): Unit = {
    log.debug("initializing check {}", checkRef)
    if (initializers.nonEmpty) {
      initializers.foreach {
        case (checkId, query) =>
          val op = GetStatusHistory(CheckRef(checkRef.agentId, checkId), generation, query.from, query.to,
            query.limit, query.fromInclusive, query.toExclusive, query.descending)
          log.debug("sending query {}", op)
          services.ask(op)(queryTimeout).pipeTo(self)
      }
    } else {
      caller ! InitializeCheckTaskComplete(Map.empty)
      context.stop(self)
    }
  }

  def receive = {

    case result: GetStatusHistoryResult =>
      log.debug("received history for {}: {}", result.op.checkRef, result.page)
      val history = results.getOrElse(result.op.checkRef.checkId, Vector.empty[CheckStatus])
      results.put(result.op.checkRef.checkId, history ++ result.page.history)
      if (!result.page.exhausted) {
        val op = result.op.copy(last = result.page.last)
        log.debug("sending query {}", op)
        services.ask(op)(queryTimeout).pipeTo(self)
      } else {
        inflight = inflight - result.op.checkRef.checkId
        if (inflight.isEmpty) {
          caller ! InitializeCheckTaskComplete(results.toMap)
          context.stop(self)
        }
      }

    case StateServiceOperationFailed(op: GetStatusHistory, ApiException(ResourceNotFound)) =>
      log.debug("no history found for {}", op.checkRef)
      results.put(op.checkRef.checkId, Vector.empty)
      inflight = inflight - op.checkRef.checkId
      if (inflight.isEmpty) {
        caller ! InitializeCheckTaskComplete(results.toMap)
        context.stop(self)
      }

    case StateServiceOperationFailed(op, failure) =>
      log.debug("failed to get check status: {}", failure)
      caller ! InitializeCheckTaskFailed(failure)
      context.stop(self)
  }

}

object InitializeCheckTask {
  def props(checkRef: CheckRef,
            generation: Long,
            initializers: Map[CheckId,CheckInitializer],
            caller: ActorRef,
            services: ActorRef) = {
    Props(classOf[InitializeCheckTask], checkRef, generation, initializers, caller, services)
  }
}

case class CheckInitializer(from: Option[DateTime],
                            to: Option[DateTime],
                            limit: Int,
                            fromInclusive: Boolean,
                            toExclusive: Boolean,
                            descending: Boolean)

case class InitializeCheckTaskComplete(results: Map[CheckId,Vector[CheckStatus]])
case class InitializeCheckTaskFailed(ex: Throwable)