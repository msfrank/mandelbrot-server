package io.mandelbrot.persistence.cassandra

import java.net.URI

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config
import com.datastax.driver.core.{Row, BoundStatement}
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._

import io.mandelbrot.core.registry._
import io.mandelbrot.core.http.JsonProtocol
import io.mandelbrot.persistence.cassandra.CassandraRegistrar.CassandraRegistrarSettings
import io.mandelbrot.core.{ResourceNotFound, ApiException}

/**
 *
 */
class CassandraRegistrar(settings: CassandraRegistrarSettings) extends Actor with ActorLogging with Registrar with RegistrarStatements {
  import spray.json._
  import JsonProtocol._

  val session = Cassandra(context.system).getSession

  val keyspaceName = Cassandra(context.system).keyspaceName
  val tableName = "registry"

  session.execute(createRegistryTable)

  def receive = {

    case op: CreateProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      try {
        session.execute(bindRegisterProbeSystem(op.uri, op.registration, timestamp))
        sender() ! CreateProbeSystemEntryResult(op, 1)
      } catch {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }

    case op: UpdateProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      try {
        session.execute(bindUpdateProbeSystem(op.uri, op.registration, op.lsn, timestamp))
        sender() ! UpdateProbeSystemEntryResult(op, op.lsn + 1)
      } catch {
        case ex: Throwable => sender () ! RegistryServiceOperationFailed(op, ex)
      }

    case op: DeleteProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      try {
        sender() ! DeleteProbeSystemEntryResult(op, op.lsn)
      } catch {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }

    case op: GetProbeSystemEntry =>
      try {
        session.execute(bindGetProbeSystem(op.uri)).one() match {
          case null =>
            sender() ! RegistryServiceOperationFailed(op, new ApiException(ResourceNotFound))
          case row: Row =>
            val registration = JsonParser(row.getString(0)).convertTo[ProbeRegistration]
            val lsn = row.getLong(1)
            sender() ! GetProbeSystemEntryResult(op, registration, lsn)
        }
      } catch {
        case ex: Throwable => sender() ! RegistryServiceOperationFailed(op, ex)
      }

    case op: ListProbeSystems =>
      try {
        val limit = op.limit match {
          case Some(i) if i > 100 => 100
          case Some(i) => i
          case None => 100
        }
        val statement = op.last match {
          case Some(s) =>
            bindListProbeSystems(limit)
          case None =>
            bindListProbeSystems(limit)
        }
        val results = session.execute(statement).all().map { row =>
          val uri = new URI(row.getString(0))
          val lsn = row.getLong(1)
          val lastUpdate = new DateTime(row.getDate(2))
          val joinedOn = new DateTime(row.getDate(3))
          uri -> ProbeSystemMetadata(joinedOn, lastUpdate)
        }.toMap
        sender() ! ListProbeSystemsResult(op, results, None)
      } catch {
        case ex: Throwable => RegistryServiceOperationFailed(op, ex)
      }

  }

  private val preparedRegisterProbeSystem = session.prepare(registerProbeSystemStatement)
  def bindRegisterProbeSystem(uri: URI, registration: ProbeRegistration, timestamp: DateTime) = {
    new BoundStatement(preparedRegisterProbeSystem).bind(uri.toString, registration.toJson.toString(), timestamp.toDate, timestamp.toDate)
  }

  private val preparedUpdateProbeSystem = session.prepare(updateProbeSystemStatement)
  def bindUpdateProbeSystem(uri: URI, registration: ProbeRegistration, lsn: Long, timestamp: DateTime) = {
    val currLsn: java.lang.Long = lsn
    val newLsn: java.lang.Long = currLsn + 1
    new BoundStatement(preparedUpdateProbeSystem).bind(registration.toJson.toString(), newLsn, timestamp.toDate, uri.toString, currLsn)
  }

  private val preparedDeleteProbeSystem = session.prepare(deleteProbeSystemStatement)
  def bindDeleteProbeSystem(uri: URI, lsn: Long) = {
    new BoundStatement(preparedDeleteProbeSystem).bind(uri.toString, lsn: java.lang.Long)
  }

  private val preparedGetProbeSystem = session.prepare(getProbeSystemStatement)
  def bindGetProbeSystem(uri: URI) = {
    new BoundStatement(preparedGetProbeSystem).bind(uri.toString)
  }

  private val preparedListProbeSystems = session.prepare(listProbeSystemsStatement)
  def bindListProbeSystems(limit: Int) = {
    new BoundStatement(preparedListProbeSystems).bind(limit: java.lang.Integer)
  }

}

object CassandraRegistrar {
  def props(managerSettings: CassandraRegistrarSettings) = Props(classOf[CassandraRegistrar], managerSettings)

  case class CassandraRegistrarSettings()
  def settings(config: Config): Option[CassandraRegistrarSettings] = {
    Some(CassandraRegistrarSettings())
  }
}
