package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe

import io.mandelbrot.persistence.cassandra.dal.{ProbeObservationIndexDAL, ProbeObservationDAL}
import io.mandelbrot.persistence.cassandra.EpochUtils
import io.mandelbrot.core.state.{AppendObservationResult, AppendObservation, StateServiceOperationFailed}

/**
 * given a ProbeRef and the observation, persist the observation, updating the
 * index if necessary.  If writing to the index or the observation tables fails,
 * then give up and pass the exception back to the caller.
 */
class AppendProbeObservationTask(op: AppendObservation,
                                 caller: ActorRef,
                                 probeObservationIndexDAL: ProbeObservationIndexDAL,
                                 probeObservationDAL: ProbeObservationDAL) extends Actor with ActorLogging {
  import AppendProbeObservationTask._
  import context.dispatcher

  val epoch = EpochUtils.timestamp2epoch(op.observation.timestamp)

  override def preStart(): Unit = {
    probeObservationIndexDAL.putEpoch(op.probeRef, op.generation, epoch)
      .map(_ => PutEpoch)
      .pipeTo(self)
  }

  def receive = {

    case PutEpoch =>
      probeObservationDAL.updateProbeObservation(op.probeRef, op.generation, epoch, op.observation)
        .map(_ => PutStatus)
        .pipeTo(self)

    case PutStatus =>
      caller ! AppendObservationResult(op)

    case Failure(ex: Throwable) =>
      caller ! StateServiceOperationFailed(op, ex)
      context.stop(self)
  }
}

object AppendProbeObservationTask {
  def props(op: AppendObservation,
            caller: ActorRef,
            probeObservationIndexDAL: ProbeObservationIndexDAL,
            probeObservationDAL: ProbeObservationDAL) = {
    Props(classOf[AppendProbeObservationTask], op, caller, probeObservationIndexDAL, probeObservationDAL)
  }

  case object PutEpoch
  case object PutStatus
}
