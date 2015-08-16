package io.mandelbrot.core.check

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.pattern.pipe
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
  val queryTimeout = 5.seconds

  // state
  var inflight: Set[CheckId] = initializers.keySet
  val results = new mutable.HashMap[CheckId,Vector[CheckStatus]]

  override def preStart(): Unit = if (initializers.nonEmpty) {
    initializers.foreach {
      case (checkId, query) =>
        val op = GetStatusHistory(CheckRef(checkRef.agentId, checkId), generation, query.from, query.to,
          query.limit, query.fromInclusive, query.toExclusive, query.descending)
        services.ask(op)(queryTimeout).pipeTo(self)
    }
  } else {
    caller ! InitializeCheckTaskComplete(Map.empty)
    context.stop(self)
  }

  def receive = {

    case result: GetStatusHistoryResult =>
      val history = results.getOrElse(result.op.checkRef.checkId, Vector.empty[CheckStatus])
      results.put(result.op.checkRef.checkId, history ++ result.page.history)
      if (!result.page.exhausted) {
        services.ask(result.op.copy(last = result.page.last))(queryTimeout)
      } else {
        inflight = inflight - result.op.checkRef.checkId
        if (inflight.isEmpty) {
          caller ! InitializeCheckTaskComplete(results.toMap)
          context.stop(self)
        }
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