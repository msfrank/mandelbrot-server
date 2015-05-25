package io.mandelbrot.persistence.cassandra.dal

import java.util

import com.datastax.driver.core.querybuilder.{QueryBuilder, Clause}
import com.datastax.driver.core.{BoundStatement, Session}
import spray.json._
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConversions._

import io.mandelbrot.core.model.{AgentMetadata, AgentId, AgentRegistration}
import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.registry.{GetRegistration, GetRegistrationResult}
import io.mandelbrot.persistence.cassandra.CassandraRegistryPersisterSettings
import io.mandelbrot.core.{ResourceNotFound, ApiException}

class AgentRegistrationDAL(settings: CassandraRegistryPersisterSettings,
                           val session: Session,
                           implicit val ec: ExecutionContext) extends AbstractDriver {


  val tableName: String = "agent_status"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  agent_id text,
       |  generation bigint,
       |  lsn bigint,
       |  registration text,
       |  joined_on timestamp,
       |  updated_on timestamp,
       |  expires_on timestamp,
       |  PRIMARY KEY (agent_id, generation, lsn)
       |)
     """.stripMargin)

  private val preparedUpdateAgentRegistration = session.prepare(
    s"""
       |INSERT INTO $tableName (agent_id, generation, lsn, registration, joined_on, updated_on, expires_on)
       |VALUES (?, ?, ?, ?, ?, ?, ?)
     """.stripMargin)

  /**
   *
   */
  def updateAgentRegistration(agentId: AgentId,
                              generation: Long,
                              lsn: Long,
                              registration: AgentRegistration,
                              joinedOn: DateTime,
                              updatedOn: DateTime,
                              expiresOn: Option[DateTime]): Future[Unit] = {
    val _agentId = agentId.toString
    val _generation: java.lang.Long = generation
    val _lsn: java.lang.Long = lsn
    val _registration = agentRegistration2string(registration)
    val _joinedOn = joinedOn.toDate
    val _updatedOn = updatedOn.toDate
    val _expiresOn = expiresOn.map(_.toDate).orNull
    val statement = new BoundStatement(preparedUpdateAgentRegistration)
    statement.bind(_agentId, _generation, _lsn, _registration, _joinedOn, _updatedOn, _expiresOn)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedGetAgentRegistration = session.prepare(
    s"""
       |SELECT registration, joined_on, updated_on, expires_on
       |FROM $tableName
       |WHERE agent_id = ? AND generation = ? AND lsn = ?
     """.stripMargin)

  def getAgentRegistration(agentId: AgentId, generation: Long, lsn: Long): Future[(AgentRegistration,AgentMetadata)] = {
    val _agentId = agentId.toString
    val _generation: java.lang.Long = generation
    val _lsn: java.lang.Long = lsn
    val statement = new BoundStatement(preparedGetAgentRegistration)
    statement.bind(_agentId, _generation, _lsn)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row != null) {
          val registration = string2agentRegistration(row.getString(0))
          val joinedOn = new DateTime(row.getDate(1), DateTimeZone.UTC)
          val updatedOn = new DateTime(row.getDate(2), DateTimeZone.UTC)
          val expiresOn = Option(row.getDate(3)).map(new DateTime(_, DateTimeZone.UTC))
          val metadata = AgentMetadata(agentId, generation, joinedOn, updatedOn, expiresOn)
          (registration,metadata)
        } else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedGetLastAgentRegistration = session.prepare(
    s"""
       |SELECT generation, lsn, registration, joined_on, updated_on, expires_on
       |FROM $tableName
       |WHERE agent_id = ?
       |ORDER BY generation DESC, lsn DESC
       |LIMIT 1
     """.stripMargin)

  def getAgentRegistration(op: GetRegistration): Future[GetRegistrationResult] = {
    val statement = new BoundStatement(preparedGetLastAgentRegistration)
    statement.bind(op.agentId.toString)
    executeAsync(statement).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val generation = row.getLong(0)
        val lsn = row.getLong(1)
        val registration = string2agentRegistration(row.getString(2))
        val joinedOn = new DateTime(row.getDate(3), DateTimeZone.UTC)
        val updatedOn = new DateTime(row.getDate(4), DateTimeZone.UTC)
        val expiresOn = Option(row.getDate(5)).map(new DateTime(_, DateTimeZone.UTC))
        val metadata = AgentMetadata(op.agentId, generation, joinedOn, updatedOn, expiresOn)
        GetRegistrationResult(op, registration, metadata, lsn)
      } else throw ApiException(ResourceNotFound)
    }
  }

  private val generationLsnColumns: java.util.List[String] = util.Arrays.asList("generation", "lsn")
  private val largestGenerationLsn = util.Arrays.asList(Long.MaxValue, Long.MaxValue).asInstanceOf[util.List[Object]]
  private val smallestGenerationLsn = util.Arrays.asList(0L, 0L).asInstanceOf[util.List[Object]]

  /**
   *
   */
  def startClause(from: Option[(Long,Long)], fromExclusive: Boolean): Clause = from match {
    case None if fromExclusive => QueryBuilder.gt(generationLsnColumns, smallestGenerationLsn)
    case None => QueryBuilder.gte(generationLsnColumns, smallestGenerationLsn)
    case Some((generation,lsn)) =>
      val generationLsn = util.Arrays.asList(generation, lsn).asInstanceOf[util.List[Object]]
      if (fromExclusive)
        QueryBuilder.gt(generationLsnColumns, generationLsn)
      else
        QueryBuilder.gte(generationLsnColumns, generationLsn)
  }

  /**
   *
   */
  def endClause(to: Option[(Long,Long)], toInclusive: Boolean): Clause = to match {
    case None if toInclusive => QueryBuilder.lte(generationLsnColumns, largestGenerationLsn)
    case None => QueryBuilder.lt(generationLsnColumns, largestGenerationLsn)
    case Some((generation,lsn)) =>
      val generationLsn = util.Arrays.asList(generation, lsn).asInstanceOf[util.List[Object]]
      if (toInclusive)
        QueryBuilder.lte(generationLsnColumns, generationLsn)
      else
        QueryBuilder.lt(generationLsnColumns, generationLsn)
  }

  /**
   *
   */
  def getAgentRegistrationHistory(agentId: AgentId,
                                  from: Option[(Long,Long)],
                                  to: Option[(Long,Long)],
                                  limit: Int,
                                  fromExclusive: Boolean,
                                  toInclusive: Boolean,
                                  descending: Boolean): Future[AgentRegistrationHistory] = {
    val start = startClause(from, fromExclusive)
    val end = endClause(to, toInclusive)
    val ordering = if (descending) {
      List(QueryBuilder.desc("generation"), QueryBuilder.desc("lsn"))
    } else {
      List(QueryBuilder.asc("generation"), QueryBuilder.asc("lsn"))
    }
    val select = QueryBuilder.select("generation", "lsn", "registration", "joined_on", "updated_on", "expires_on")
      .from(tableName)
      .where(QueryBuilder.eq("agent_id", agentId.toString))
        .and(start)
        .and(end)
      .orderBy(ordering :_*)
      .limit(limit)
    select.setFetchSize(limit)
    executeAsync(select).map { resultSet =>
      val snapshots = resultSet.all().map { row =>
        val generation = row.getLong(0)
        val lsn = row.getLong(1)
        val registration = string2agentRegistration(row.getString(2))
        val joinedOn = new DateTime(row.getDate(3), DateTimeZone.UTC)
        val updatedOn = new DateTime(row.getDate(4), DateTimeZone.UTC)
        val expiresOn = Option(row.getDate(5)).map(new DateTime(_, DateTimeZone.UTC))
        val metadata = AgentMetadata(agentId, generation, joinedOn, updatedOn, expiresOn)
        AgentRegistrationSnapshot(agentId, registration, metadata, lsn)
      }.toVector
      AgentRegistrationHistory(snapshots)
    }
  }

  private val preparedDeleteAgentRegistrationAtLsn = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE agent_id = ? AND generation = ? AND lsn = ?
     """.stripMargin)

  def deleteAgentRegistration(agentId: AgentId, generation: Long, lsn: Long): Future[Unit] = {
    val statement = new BoundStatement(preparedDeleteAgentRegistrationAtLsn)
    val _generation: java.lang.Long = generation
    val _lsn: java.lang.Long = lsn
    statement.bind(agentId.toString, _generation, _lsn)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedDeleteAgentRegistration = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE agent_id = ?
     """.stripMargin)

  def deleteAgentRegistration(agentId: AgentId): Future[Unit] = {
    val statement = new BoundStatement(preparedDeleteAgentRegistration)
    statement.bind(agentId.toString)
    executeAsync(statement).map { _ => Unit }
  }

  def flushAgentRegistrations(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  def string2agentRegistration(string: String): AgentRegistration = string.parseJson.convertTo[AgentRegistration]

  def agentRegistration2string(registration: AgentRegistration): String = registration.toJson.prettyPrint
}

case class AgentRegistrationSnapshot(agentId: AgentId, registration: AgentRegistration, metadata: AgentMetadata, lsn: Long)
case class AgentRegistrationHistory(snapshots: Vector[AgentRegistrationSnapshot])