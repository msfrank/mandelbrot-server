package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import akka.util.ByteString
import com.typesafe.config.Config
import com.datastax.driver.core.BoundStatement

import io.mandelbrot.core.state._
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings
import io.mandelbrot.core.{ResourceNotFound, ApiException}

/**
 *
 */
class CassandraPersister(settings: CassandraPersisterSettings) extends Actor with ActorLogging with Persister with PersisterStatements {

  val session = Cassandra(context.system).getSession
  val keyspaceName = Cassandra(context.system).keyspaceName
  val tableName = "state"

  session.execute(createStateTable)

  def receive = {

    case op: InitializeProbeState =>
      try {
        val resultSet = session.execute(bindGetState(op.ref))
        if (!resultSet.isExhausted) {
          val state = row2ProbeState(resultSet.one())
          sender() ! InitializeProbeStateResult(op, state.status, state.lsn)
        } else {
          val status = ProbeStatus(op.ref, op.timestamp, ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
          sender() ! InitializeProbeStateResult(op, status, 0)
        }
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

    case op: GetProbeState =>
      try {
        val resultSet = session.execute(bindGetState(op.probeRef))
        if (!resultSet.isExhausted) {
          val state = row2ProbeState(resultSet.one())
          sender() ! GetProbeStateResult(op, state.status, state.lsn)
        } else {
          sender() ! StateServiceOperationFailed(op, new ApiException(ResourceNotFound))
        }
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

    case op: UpdateProbeState =>
      try {
        val resultSet = session.execute(bindSetState(op.status, op.lsn, None))
          sender() ! UpdateProbeStateResult(op)
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

    case op: DeleteProbeState =>
      try {
        val resultSet = session.execute(bindDeleteState(op.ref))
        sender() ! DeleteProbeStateResult(op)
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

  }

  private val preparedGetState = session.prepare(getStateStatement)
  def bindGetState(probeRef: ProbeRef) = {
    new BoundStatement(preparedGetState).bind(probeRef.toString)
  }

  private val preparedSetState = session.prepare(setStateStatement)
  def bindSetState(status: ProbeStatus, lsn: Long, context: Option[ByteString]) = {
    new BoundStatement(preparedSetState).bind(status.probeRef.toString, lsn: java.lang.Long,
      status.timestamp.toDate, status.lifecycle.toString, status.health.toString, status.summary.orNull,
      status.lastUpdate.map(_.toDate).orNull, status.lastChange.map(_.toDate).orNull, status.correlation.orNull,
      status.acknowledged.orNull, status.squelched: java.lang.Boolean, context.map(_.asByteBuffer).orNull)
  }

  private val preparedDeleteState = session.prepare(deleteStateStatement)
  def bindDeleteState(probeRef: ProbeRef) = {
    new BoundStatement(preparedSetState).bind(probeRef.toString)
  }

}

object CassandraPersister {

  def props(managerSettings: CassandraPersisterSettings) = Props(classOf[CassandraPersister], managerSettings)

  case class CassandraPersisterSettings()
  def settings(config: Config): Option[CassandraPersisterSettings] = {
    Some(CassandraPersisterSettings())
  }
}