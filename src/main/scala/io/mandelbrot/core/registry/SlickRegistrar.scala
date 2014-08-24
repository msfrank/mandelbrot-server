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

package io.mandelbrot.core.registry

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config
import scala.slick.driver.JdbcProfile
import scala.slick.jdbc.JdbcBackend.Database
import org.joda.time.{DateTimeZone, DateTime}
import java.io.File
import java.net.URI

import io.mandelbrot.core.{Conflict, ResourceNotFound, ApiException}
import io.mandelbrot.core.http.JsonProtocol
import io.mandelbrot.core.registry.H2Registrar.H2RegistrarSettings

/*
 this multi-db code is heavily inspired by
 https://github.com/slick/slick-examples/blob/master/src/main/scala/com/typesafe/slick/examples/lifted/MultiDBCakeExample.scala
 */

trait Profile {
  val profile: JdbcProfile
}
/**
 *
 */
trait RegistryEntriesComponent { this: Profile =>
  import profile.simple._
  import RegistryEntries._
  import spray.json._
  import JsonProtocol._

  class RegistryEntries(tag: Tag) extends Table[(Long,String,String,Long,Long,Long)](tag, "registry_entries") {
    def id = column[Long]("id", O.AutoInc)
    def probeSystem = column[String]("probeSystem", O.PrimaryKey)
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

  def insert(systemUri: URI, registration: ProbeRegistration, timestamp: DateTime)(implicit session: Session): Long = {
    registryEntries.filter(_.probeSystem === systemUri.toString).firstOption match {
      case None =>
        val probeSystem: String = systemUri.toString
        val registrationString: String = registration.toJson.prettyPrint
        val lsn = 1
        val joinedOn: Long = timestamp.getMillis
        registryEntries += ((0, probeSystem, registrationString, lsn, joinedOn, joinedOn))
        lsn
      case _ =>
        throw new ApiException(Conflict)
    }
  }

  def update(systemUri: URI, registration: ProbeRegistration, timestamp: DateTime)(implicit session: Session): Long = {
    val probeSystem: String = systemUri.toString
    registryEntries.filter(_.probeSystem === probeSystem).map(e => e.lsn).firstOption match {
      case Some(lsn: Long) =>
        val registrationString: String = registration.toJson.prettyPrint
        val updatedLsn = lsn + 1
        val lastUpdate: Long = timestamp.getMillis
        registryEntries.filter(_.probeSystem === probeSystem)
          .map(e => (e.registration,e.lsn,e.lastUpdate))
          .update((registrationString, updatedLsn, lastUpdate))
        updatedLsn
      case None =>
        throw new ApiException(ResourceNotFound)
    }
  }

  def delete(systemUri: URI, timestamp: DateTime)(implicit session: Session): Long = {
    val probeSystem = systemUri.toString
    registryEntries.filter(_.probeSystem === probeSystem).firstOption match {
      case Some((_, _, _, lsn: Long, _, _)) =>
        registryEntries.filter(_.probeSystem === probeSystem).delete
        lsn
      case None =>
        throw new ApiException(ResourceNotFound)
    }
  }

  object RegistryEntries {
    type RegistryEntry = (Long,String,String,Long,Long,Long)
  }
}

/**
 *
 */
class DAL(override val profile: JdbcProfile) extends RegistryEntriesComponent with Profile {
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
  val dal: DAL

  def receive = {

    case RecoverProbeSystems =>
      db.withSession { implicit session =>
        var last = 0L
        var systems: Vector[(Long,String,String,Long,Long,Long)] = dal.list(last, 100).toVector
        while (systems.nonEmpty) {
          systems.foreach { case (id, probeSystem, registration, lsn, _, _) =>
            val uri = new URI(probeSystem)
            sender() ! ProbeSystemRecovers(uri, JsonParser(registration).convertTo[ProbeRegistration], lsn)
            last = id
            log.debug("recovered probe system {} has lsn {}", uri, lsn)
          }
          systems = dal.list(last, 100).toVector
        }
      }

    case command: RegisterProbeSystem =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      try {
        db.withSession { implicit session =>
          val lsn = dal.insert(command.uri, command.registration, timestamp)
          sender() ! ProbeSystemRegisters(command, timestamp, lsn)
        }
      } catch {
        case ex: Throwable => sender() ! ProbeRegistryOperationFailed(command, ex)
      }

    case command: UpdateProbeSystem =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      try {
        db.withSession { implicit session =>
          val lsn = dal.update(command.uri, command.registration, timestamp)
          sender() ! ProbeSystemUpdates(command, timestamp, lsn)
        }
      } catch {
        case ex: Throwable => sender() ! ProbeRegistryOperationFailed(command, ex)
      }

    case command: UnregisterProbeSystem =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      try {
        db.withSession { implicit session =>
          val lsn = dal.delete(command.uri, timestamp)
          sender() ! ProbeSystemUnregisters(command, timestamp, lsn)
        }
      } catch {
        case ex: Throwable => sender() ! ProbeRegistryOperationFailed(command, ex)
      }

    case query: ListProbeSystems =>
      try {
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
      } catch {
        case ex: Throwable => sender() ! ProbeRegistryOperationFailed(query, ex)
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
    if (managerSettings.inMemory) "mem:history" else "file:" + managerSettings.databasePath.getAbsolutePath
  } + ";" + {
    if (managerSettings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!managerSettings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
  }

  val db = Database.forURL(url = url, driver = "org.h2.Driver")
  val dal = new DAL(H2Driver)
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
