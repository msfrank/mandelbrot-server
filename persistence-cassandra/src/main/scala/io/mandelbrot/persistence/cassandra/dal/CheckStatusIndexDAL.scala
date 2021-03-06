package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.{BatchStatement, BoundStatement, Session}
import io.mandelbrot.core.model._
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.CassandraStatePersisterSettings
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

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
       |  generation bigint,
       |  epoch bigint,
       |  PRIMARY KEY ((check_ref, generation), epoch)
       |)
     """.stripMargin)

  private val preparedGetFirstEpoch = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstEpoch(checkRef: CheckRef, generation: Long): Future[Long] = {
    val statement = new BoundStatement(preparedGetFirstEpoch)
    statement.bind(checkRef.toString, generation: java.lang.Long)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row.getLong(0) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedGetLastEpoch = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin)

  def getLastEpoch(checkRef: CheckRef, generation: Long): Future[Long] = {
    val statement = new BoundStatement(preparedGetLastEpoch)
    statement.bind(checkRef.toString, generation: java.lang.Long)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row.getLong(0) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedListEpochsInclusiveAscending = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch >= ? AND epoch <= ?
       |ORDER BY epoch ASC
       |LIMIT ?
     """.stripMargin)

  def listEpochsInclusiveAscending(checkRef: CheckRef,
                                   generation: Long,
                                   from: DateTime,
                                   to: DateTime,
                                   limit: Int): Future[EpochList] = {
    val statement = new BoundStatement(preparedListEpochsInclusiveAscending)
    val _generation: java.lang.Long = generation
    val _from: java.lang.Long = from.getMillis
    val _to: java.lang.Long = to.getMillis
    val _limit: java.lang.Integer = limit
    statement.bind(checkRef.toString, _generation, _from, _to, _limit)
    executeAsync(statement).map { resultSet =>
      EpochList(resultSet.all().map(row => row.getLong(0)).toList)
    }
  }

  private val preparedListEpochsInclusiveDescending = session.prepare(
    s"""
       |SELECT epoch
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch >= ? AND epoch <= ?
       |ORDER BY epoch DESC
       |LIMIT ?
     """.stripMargin)

  def listEpochsInclusiveDescending(checkRef: CheckRef,
                                    generation: Long,
                                    from: DateTime,
                                    to: DateTime,
                                    limit: Int): Future[EpochList] = {
    val statement = new BoundStatement(preparedListEpochsInclusiveDescending)
    val _generation: java.lang.Long = generation
    val _from: java.lang.Long = from.getMillis
    val _to: java.lang.Long = to.getMillis
    val _limit: java.lang.Integer = limit
    statement.bind(checkRef.toString, _generation, _from, _to, _limit)
    executeAsync(statement).map { resultSet =>
      EpochList(resultSet.all().map(row => row.getLong(0)).toList)
    }
  }

  private val preparedPutEpoch = session.prepare(
    s"""
       |INSERT INTO $tableName (check_ref, generation, epoch) VALUES (?, ?, ?)
     """.stripMargin)

  def putEpoch(checkRef: CheckRef, generation: Long, epoch: Long): Future[Unit] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedPutEpoch)
    statement.bind(_checkRef, _generation, _epoch)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedDeleteEpoch = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ?
     """.stripMargin)

  def deleteEpochs(checkRef: CheckRef, generation: Long, epochs: List[Long]): Future[Unit] = {
    // we actually want to use an unlogged batch here :)  see:
    // http://christopher-batey.blogspot.com/2015/02/cassandra-anti-pattern-misuse-of.html
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
    val _generation: java.lang.Long = generation
    epochs.foreach { epoch =>
      val statement = new BoundStatement(preparedDeleteEpoch)
      val _epoch: java.lang.Long = epoch
      statement.bind(checkRef.toString, _generation, _epoch)
      batch.add(statement)
    }
    executeAsync(batch).map { _ => Unit }
  }

  private val preparedDeleteIndex = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE check_ref = ? AND generation = ?
     """.stripMargin)

  def deleteIndex(checkRef: CheckRef, generation: Long): Future[Unit] = {
    val statement = new BoundStatement(preparedDeleteIndex)
    statement.bind(checkRef.toString, generation: java.lang.Long)
    executeAsync(statement).map { _ => Unit }
  }

  def flushCommittedIndex(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }
}

case class EpochList(epochs: List[Long])