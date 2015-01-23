/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.persistence.slick

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config
import scala.slick.driver.JdbcProfile
import scala.slick.jdbc.JdbcBackend.Database
import org.joda.time.{DateTimeZone, DateTime}
import scala.util._
import java.io.File
import java.net.URI

import io.mandelbrot.core.registry._
import io.mandelbrot.core.{Conflict, ResourceNotFound, ApiException}
import io.mandelbrot.core.http.JsonProtocol
import io.mandelbrot.persistence.slick.H2Registrar.H2RegistrarSettings

/*
 this multi-db code is heavily inspired by
 https://github.com/slick/slick-examples/blob/master/src/main/scala/com/typesafe/slick/examples/lifted/MultiDBCakeExample.scala
 */

trait RegistrarProfile {
  val profile: JdbcProfile
}
/**
 *
 */
trait RegistryEntriesComponent { this: RegistrarProfile =>
  import profile.simple._
  import RegistryEntries._
  import spray.json._
  import JsonProtocol._

  class RegistryEntries(tag: Tag) extends Table[(Long,String,String,Long,Long,Long)](tag, "registry_entries") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def probeSystem = column[String]("probeSystem")
    def probeSystemIdx = index("idx_probeSystem", probeSystem, unique = true)
    def registration = column[String]("registration")
    def lsn = column[Long]("lsn")
    def joinedOn = column[Long]("joinedOn")
    def lastUpdate = column[Long]("lastUpdate")
    def * = (id, probeSystem, registration, lsn, joinedOn, lastUpdate)
  }

  val registryEntries = TableQuery[RegistryEntries]

  def list(last: Long, limit: Int)(implicit session: Session): Iterable[RegistryEntry] = {
    registryEntries.filter(_.id > last).take(limit).list()
  }

  def get(systemUri: URI)(implicit session: Session): Option[RegistryEntry] = {
    registryEntries.filter(_.probeSystem === systemUri.toString).firstOption
  }

  def insert(systemUri: URI, registration: ProbeRegistration, timestamp: DateTime)(implicit session: Session): Try[Long] = {
    val probeSystem: String = systemUri.toString
    registryEntries.filter(_.probeSystem === probeSystem).firstOption match {
      case None =>
        val registrationString: String = registration.toJson.prettyPrint
        val lsn = 1L
        val joinedOn: Long = timestamp.getMillis
        registryEntries += ((0, probeSystem, registrationString, lsn, joinedOn, joinedOn))
        Success(lsn)
      case _ =>
        Failure(new ApiException(Conflict))
    }
  }

  def update(systemUri: URI, registration: ProbeRegistration, timestamp: DateTime)(implicit session: Session): Try[Long] = {
    val probeSystem: String = systemUri.toString
    registryEntries.filter(_.probeSystem === probeSystem).map(e => e.lsn).firstOption match {
      case Some(lsn: Long) =>
        val registrationString: String = registration.toJson.prettyPrint
        val updatedLsn = lsn + 1
        val lastUpdate: Long = timestamp.getMillis
        registryEntries.filter(_.probeSystem === probeSystem)
          .map(e => (e.registration,e.lsn,e.lastUpdate))
          .update((registrationString, updatedLsn, lastUpdate))
        Success(updatedLsn)
      case None =>
        Failure(new ApiException(ResourceNotFound))
    }
  }

  def delete(systemUri: URI, timestamp: DateTime)(implicit session: Session): Try[Long] = {
    val probeSystem = systemUri.toString
    registryEntries.filter(_.probeSystem === probeSystem).firstOption match {
      case Some((_, _, _, lsn: Long, _, _)) =>
        registryEntries.filter(_.probeSystem === probeSystem).delete
        Success(lsn)
      case None =>
        Failure(new ApiException(ResourceNotFound))
    }
  }

  object RegistryEntries {
    type RegistryEntry = (Long,String,String,Long,Long,Long)
  }
}

/**
 *
 */
class RegistryDAL(override val profile: JdbcProfile) extends RegistryEntriesComponent with RegistrarProfile {
  import profile.simple._

  def create(implicit session: Session): Unit = {
    try {
      registryEntries.ddl.create
    } catch {
      case ex: Throwable =>
    }
  }
}


/**
 *
 */
trait SlickRegistrar extends Actor with ActorLogging {
  import scala.slick.jdbc.JdbcBackend.Database
  import RegistryManager._
  import spray.json._
  import DefaultJsonProtocol._
  import JsonProtocol._

  // abstract members
  val db: Database
  val dal: RegistryDAL

  def receive = {

    case command: CreateProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      db.withSession { implicit session =>
        dal.insert(command.uri, command.registration, timestamp) match {
          case Success(lsn) => sender() ! CreateProbeSystemEntryResult(command, 1)
          case Failure(ex) => sender() ! RegistryServiceOperationFailed(command, ex)
        }
      }

    case command: UpdateProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      db.withSession { implicit session =>
        dal.update(command.uri, command.registration, timestamp) match {
          case Success(lsn) => sender() ! UpdateProbeSystemEntryResult(command, 1)
          case Failure(ex) => sender() ! RegistryServiceOperationFailed(command, ex)
        }
      }

    case command: DeleteProbeSystemEntry =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      db.withSession { implicit session =>
        dal.delete(command.uri, timestamp) match {
          case Success(lsn) => sender() ! DeleteProbeSystemEntryResult(command, 1)
          case Failure(ex) => sender() ! RegistryServiceOperationFailed(command, ex)
        }
      }

    case query: GetProbeSystemEntry =>
      db.withSession { implicit session =>
        dal.get(query.uri) match {
          case Some((_, _, registration, lsn, _, _)) =>
            sender() ! GetProbeSystemEntryResult(query, JsonParser(registration).convertTo[ProbeRegistration], lsn)
          case None =>
            sender() ! RegistryServiceOperationFailed(query, new ApiException(ResourceNotFound))
        }
      }

    case query: ListProbeSystems =>
      val last = query.last match {
        case Some(s) => s.toLong
        case None => 0L
      }
      val limit = query.limit match {
        case Some(i) if i > 100 => 100
        case Some(i) => i
        case None => 100
      }
      db.withSession { implicit session =>
        val entries = dal.list(last, limit).toVector
        val systems = entries.map { case (id,probeSystem,_,lsn,joinedOn,lastUpdate) =>
          new URI(probeSystem) -> ProbeSystemMetadata(new DateTime(joinedOn), new DateTime(lastUpdate))
        }.toMap
        entries.lastOption match {
          case Some((id, _, _, _, _, _)) =>
            sender() ! ListProbeSystemsResult(query, systems, Some(id.toString))
          case None =>
            sender() ! ListProbeSystemsResult(query, Map.empty, None)
        }
      }
  }
}

/**
 *
 */
class H2Registrar(managerSettings: H2RegistrarSettings) extends SlickRegistrar with Registrar {
  import scala.slick.driver.H2Driver

  // config
  val url = "jdbc:h2:" + {
    if (managerSettings.inMemory) "mem:history" else "file:" + managerSettings.databasePath.getAbsolutePath + "/registry"
  } + ";" + {
    if (managerSettings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!managerSettings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
  }

  val db = Database.forURL(url = url, driver = "org.h2.Driver")
  val dal = new RegistryDAL(H2Driver)
  db.withSession { implicit session => dal.create }
  log.debug("initialized registry DAL")
}

object H2Registrar {
  def props(managerSettings: H2RegistrarSettings) = Props(classOf[H2Registrar], managerSettings)

  case class H2RegistrarSettings(databasePath: File, inMemory: Boolean, h2databaseToUpper: Boolean)
  def settings(config: Config): Option[H2RegistrarSettings] = {
    val databasePath = new File(config.getString("database-path"))
    val inMemory = config.getBoolean("in-memory")
    val h2databaseToUpper = config.getBoolean("h2-database-to-upper")
    Some(H2RegistrarSettings(databasePath, inMemory, h2databaseToUpper))
  }
}
