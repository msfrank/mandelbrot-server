package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{BoundStatement, Row, Session}
import scala.concurrent.{ExecutionContext, Future}
import org.joda.time.DateTime

import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.core.model._

/**
 *
 */
class CommittedIndexDAL(settings: CassandraStatePersisterSettings,
                        val session: Session,
                        implicit val ec: ExecutionContext) extends AbstractDriver {

  val tableName: String = "committed_index"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  probe_ref text,
       |  initial timestamp,
       |  current timestamp,
       |  last timestamp,
       |  PRIMARY KEY (probe_ref)
       |)
     """.stripMargin)

  private val preparedGetCommittedIndex = session.prepare(
    s"""
       |SELECT probe_ref, initial, current, last
       |FROM $tableName
       |WHERE probe_ref = ?
     """.stripMargin)

  def getCommittedIndex(probeRef: ProbeRef): Future[CommittedIndex] = {
    val statement = new BoundStatement(preparedGetCommittedIndex)
    statement.bind(probeRef.toString)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row2committedIndex(row) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedInitializeCommittedIndex = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, initial, current, last)
       |VALUES (?, ?, ?, ?)
     """.stripMargin)

  def initializeCommittedIndex(probeRef: ProbeRef, initial: DateTime): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _initial = initial.toDate
    val current = initial.toDate
    val statement = new BoundStatement(preparedInitializeCommittedIndex)
    statement.bind(_probeRef, _initial, current, null)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedUpdateCommittedIndex = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, current, last)
       |VALUES (?, ?, ?)
     """.stripMargin)

  def updateCommittedIndex(probeRef: ProbeRef, current: DateTime, last: DateTime): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _current = current.toDate
    val _last = last.toDate
    val statement = new BoundStatement(preparedUpdateCommittedIndex)
    statement.bind(_probeRef, _current, _last)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedDeleteCommittedIndex = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE probe_ref = ?
     """.stripMargin)

  def deleteCommittedIndex(probeRef: ProbeRef): Future[Unit] = {
    val statement = new BoundStatement(preparedDeleteCommittedIndex)
    statement.bind(probeRef.toString)
    executeAsync(statement).map { _ => Unit }
  }

  def flushCommittedIndex(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  def row2committedIndex(row: Row): CommittedIndex = {
    val probeRef = ProbeRef(row.getString(0))
    val initial = new DateTime(row.getDate(1))
    val current = new DateTime(row.getDate(2))
    val last = Option(row.getDate(3)).map(new DateTime(_))
    CommittedIndex(probeRef, initial, current, last)
  }
}

case class CommittedIndex(probeRef: ProbeRef, initial: DateTime, current: DateTime, last: Option[DateTime])
