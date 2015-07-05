package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.{BoundStatement, Session}
import spray.json._
import org.joda.time.{DateTime, DateTimeZone}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.CassandraRegistryPersisterSettings

/**
 *
 */
class AgentTombstoneDAL(settings: CassandraRegistryPersisterSettings,
                        val session: Session,
                        implicit val ec: ExecutionContext) extends AbstractDriver {

  val tableName: String = "agent_tombstone"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  p int,
       |  tombstone timestamp,
       |  agent_id text,
       |  generation bigint,
       |  PRIMARY KEY(p, tombstone, agent_id, generation)
       |);
     """.stripMargin)

  private val preparedPutTombstone = session.prepare(
    s"""
       |INSERT INTO $tableName (p, tombstone, agent_id, generation)
       |VALUES (?, ?, ?, ?)
     """.stripMargin)

  def putTombstone(partition: Int, tombstone: DateTime, agentId: AgentId, generation: Long): Future[Unit] = {
    val _partition: java.lang.Integer = partition
    val _tombstone = tombstone.toDate
    val _agentId = agentId.toString
    val _generation: java.lang.Long = generation
    executeAsync(new BoundStatement(preparedPutTombstone)
      .bind(_partition, _tombstone, _agentId, _generation))
      .map { _ => }
  }

  private val preparedDeleteTombstone = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE p = ? AND tombstone = ? AND agent_id = ? AND generation = ?
     """.stripMargin)

  def deleteTombstone(partition: Int, tombstone: DateTime, agentId: AgentId, generation: Long): Future[Unit] = {
    val _partition: java.lang.Integer = partition
    val _tombstone = tombstone.toDate
    val _agentId = agentId.toString
    val _generation: java.lang.Long = generation
    executeAsync(new BoundStatement(preparedDeleteTombstone)
      .bind(_partition, _tombstone, _agentId, _generation))
      .map { _ => }
  }

  private val preparedListExpiredTombstones = session.prepare(
    s"""
       |SELECT tombstone, agent_id, generation FROM $tableName
       |WHERE p = ? AND tombstone < ?
       |LIMIT ?
     """.stripMargin)

  def listExpiredTombstones(partition: Int, olderThan: DateTime, limit: Int): Future[Vector[AgentTombstone]] = {
    val _partition: java.lang.Integer = partition
    val _olderThan = olderThan.toDate
    val _limit: java.lang.Integer = limit
    executeAsync(new BoundStatement(preparedListExpiredTombstones).bind(_partition, _olderThan, _limit)).map { resultSet =>
      resultSet.all().map { row =>
        val expires = new DateTime(row.getDate(0), DateTimeZone.UTC)
        val agentId = AgentId(row.getString(1))
        val generation = row.getLong(2)
        AgentTombstone(partition, agentId, generation, expires)
      }.toVector
    }
  }

  def flushTombstones(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => }
  }
}

case class AgentTombstone(partition: Int, agentId: AgentId, generation: Long, expires: DateTime)
