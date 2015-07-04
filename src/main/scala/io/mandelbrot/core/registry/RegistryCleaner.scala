package io.mandelbrot.core.registry

import akka.actor._

import io.mandelbrot.core.model.AgentId
import org.joda.time.{DateTimeZone, DateTime}

/**
 *
 */
class RegistryCleaner(settings: RegistrySettings, services: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher
  import RegistryCleaner._

  // state
  var pullWork: Option[Cancellable] = None
  var task: Option[ActorRef] = None
  var restartImmediately: Boolean = false

  override def preStart(): Unit = {
    pullWork = Some(context.system.scheduler.schedule(settings.reaperInterval,
      settings.reaperInterval, self, PullWork))
  }

  def receive = {

    /* ignore the pull request if we are waiting to restart */
    case PullWork if restartImmediately =>
      // do nothing

    /* if the reaper takes longer than the reaper interval, kill it and restart */
    case PullWork if task.nonEmpty =>
      task.foreach(context.stop)
      restartImmediately = true

    /* otherwise we are currently idle, start a new reaper */
    case PullWork =>
      task = Some(spawnReaper())

    case ReaperComplete(seen, deleted) =>
      log.debug("registry cleaner found {} tombstones, deleted {}", seen, deleted)

    /* reaper has terminated, possibly restart immediately if flagged */
    case Terminated(actorRef) =>
      task = None
      if (restartImmediately) {
        task = Some(spawnReaper())
        restartImmediately = false
      }
  }

  def spawnReaper(): ActorRef = {
    val olderThan = DateTime.now(DateTimeZone.UTC)
    val props = ReapTombstonesTask.props(olderThan, settings.reaperTimeout,
      settings.maxDeletesInFlight, services, self)
    val task = context.actorOf(props)
    context.watch(task)
    task
  }

  override def postStop(): Unit = pullWork.foreach(_.cancel())
}

object RegistryCleaner {
  def props(settings: RegistrySettings, services: ActorRef) = Props(classOf[RegistryCleaner], settings, services)

  case object PullWork
  case class AgentGeneration(agentId: AgentId, generation: Long)
}
