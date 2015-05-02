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
       |  check_ref text,
       |  initial timestamp,
       |  current timestamp,
       |  last timestamp,
       |  PRIMARY KEY (check_ref)
       |)
     """.stripMargin)

  private val preparedGetCommittedIndex = session.prepare(
    s"""
       |SELECT check_ref, initial, current, last
       |FROM $tableName
       |WHERE check_ref = ?
     """.stripMargin)

  def getCommittedIndex(checkRef: CheckRef): Future[CommittedIndex] = {
    val statement = new BoundStatement(preparedGetCommittedIndex)
    statement.bind(checkRef.toString)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row2committedIndex(row) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedInitializeCommittedIndex = session.prepare(
    s"""
       |INSERT INTO $tableName (check_ref, initial, current, last)
       |VALUES (?, ?, ?, ?)
     """.stripMargin)

  def initializeCommittedIndex(checkRef: CheckRef, initial: DateTime): Future[Unit] = {
    val _checkRef = checkRef.toString
    val _initial = initial.toDate
    val current = initial.toDate
    val statement = new BoundStatement(preparedInitializeCommittedIndex)
    statement.bind(_checkRef, _initial, current, null)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedUpdateCommittedIndex = session.prepare(
    s"""
       |INSERT INTO $tableName (check_ref, current, last)
       |VALUES (?, ?, ?)
     """.stripMargin)

  def updateCommittedIndex(checkRef: CheckRef, current: DateTime, last: DateTime): Future[Unit] = {
    val _checkRef = checkRef.toString
    val _current = current.toDate
    val _last = last.toDate
    val statement = new BoundStatement(preparedUpdateCommittedIndex)
    statement.bind(_checkRef, _current, _last)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedDeleteCommittedIndex = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE check_ref = ?
     """.stripMargin)

  def deleteCommittedIndex(checkRef: CheckRef): Future[Unit] = {
    val statement = new BoundStatement(preparedDeleteCommittedIndex)
    statement.bind(checkRef.toString)
    executeAsync(statement).map { _ => Unit }
  }

  def flushCommittedIndex(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  def row2committedIndex(row: Row): CommittedIndex = {
    val checkRef = CheckRef(row.getString(0))
    val initial = new DateTime(row.getDate(1))
    val current = new DateTime(row.getDate(2))
    val last = Option(row.getDate(3)).map(new DateTime(_))
    CommittedIndex(checkRef, initial, current, last)
  }
}

case class CommittedIndex(checkRef: CheckRef, initial: DateTime, current: DateTime, last: Option[DateTime])
