package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{BoundStatement, Session}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._
import org.joda.time.DateTime

import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.core.model._

/**
 *
 */
class CheckStatusIndexDAL(settings: CassandraStatePersisterSettings,
                          val session: Session,
                          implicit val ec: ExecutionContext) extends AbstractDriver {

  val tableName: String = "check_status_index"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  check_ref text,
       |  epoch bigint,
       |  PRIMARY KEY (check_ref, epoch)
       |)
     """.stripMargin)

  private val preparedGetFirstEpoch = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstEpoch(checkRef: CheckRef): Future[Long] = {
    val statement = new BoundStatement(preparedGetFirstEpoch)
    statement.bind(checkRef.toString)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row.getLong(0) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedGetLastEpoch = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin)

  def getLastEpoch(checkRef: CheckRef): Future[Long] = {
    val statement = new BoundStatement(preparedGetLastEpoch)
    statement.bind(checkRef.toString)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row.getLong(0) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedListEpochsInclusiveAscending = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ? AND epoch >= ? AND epoch <= ?
       |ORDER BY epoch ASC
       |LIMIT ?
     """.stripMargin)

  def listEpochsInclusiveAscending(checkRef: CheckRef, from: DateTime, to: DateTime, limit: Int): Future[EpochList] = {
    val statement = new BoundStatement(preparedListEpochsInclusiveAscending)
    val _from: java.lang.Long = from.getMillis
    val _to: java.lang.Long = to.getMillis
    val _limit: java.lang.Integer = limit
    statement.bind(checkRef.toString, _from, _to, _limit)
    executeAsync(statement).map { resultSet =>
      EpochList(resultSet.all().map(row => row.getLong(0)).toList)
    }
  }

  private val preparedListEpochsInclusiveDescending = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ? AND epoch >= ? AND epoch <= ?
       |ORDER BY epoch DESC
       |LIMIT ?
     """.stripMargin)

  def listEpochsInclusiveDescending(checkRef: CheckRef, from: DateTime, to: DateTime, limit: Int): Future[EpochList] = {
    val statement = new BoundStatement(preparedListEpochsInclusiveDescending)
    val _from: java.lang.Long = from.getMillis
    val _to: java.lang.Long = to.getMillis
    val _limit: java.lang.Integer = limit
    statement.bind(checkRef.toString, _from, _to, _limit)
    executeAsync(statement).map { resultSet =>
      EpochList(resultSet.all().map(row => row.getLong(0)).toList)
    }
  }

  private val preparedPutEpoch = session.prepare(
    s"""
       |INSERT INTO $tableName (check_ref, epoch) VALUES (?, ?)
     """.stripMargin)

  def putEpoch(checkRef: CheckRef, epoch: Long): Future[Unit] = {
    val _checkRef = checkRef.toString
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedPutEpoch)
    statement.bind(_checkRef, _epoch)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedDeleteIndex = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE check_ref = ?
     """.stripMargin)

  def deleteIndex(checkRef: CheckRef): Future[Unit] = {
    val statement = new BoundStatement(preparedDeleteIndex)
    statement.bind(checkRef.toString)
    executeAsync(statement).map { _ => Unit }
  }

  def flushCommittedIndex(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }
}

case class EpochList(epochs: List[Long])