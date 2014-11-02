package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config
import com.datastax.driver.core.{Row, BoundStatement}
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._

import io.mandelbrot.core.history._
import io.mandelbrot.core.http.JsonProtocol
import io.mandelbrot.persistence.cassandra.CassandraArchiver.CassandraArchiverSettings
import io.mandelbrot.core.{ResourceNotFound, ApiException}

/**
 *
 */
class CassandraArchiver(settings: CassandraArchiverSettings) extends Actor with ActorLogging with Archiver with ArchiverStatements {
  import spray.json._
  import JsonProtocol._

  val session = Cassandra(context.system).getSession

  val keyspaceName = Cassandra(context.system).keyspaceName
  val tableName = "history"

  def receive = {
    case op: HistoryServiceOperation => sender() ! HistoryServiceOperationFailed(op, new ApiException(ResourceNotFound))
  }

}

object CassandraArchiver {
  def props(managerSettings: CassandraArchiverSettings) = Props(classOf[CassandraArchiver], managerSettings)

  case class CassandraArchiverSettings()
  def settings(config: Config): Option[CassandraArchiverSettings] = {
    Some(CassandraArchiverSettings())
  }
}
