package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config

import io.mandelbrot.core.state._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

/**
 *
 */
class CassandraPersister(settings: CassandraPersisterSettings) extends Actor with ActorLogging with Persister {
  import context.dispatcher

  val session = Cassandra(context.system).getSession
  val state = new StateDAL(settings, session)

  def receive = {

    case op: InitializeProbeState =>
      state.initializeProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: GetProbeState =>
      state.getProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateProbeState =>
      state.updateProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteProbeState =>
      state.deleteProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())
  }
}

object CassandraPersister {

  def props(managerSettings: CassandraPersisterSettings) = Props(classOf[CassandraPersister], managerSettings)

  case class CassandraPersisterSettings()
  def settings(config: Config): Option[CassandraPersisterSettings] = {
    Some(CassandraPersisterSettings())
  }
}