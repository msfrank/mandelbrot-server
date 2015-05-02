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
import io.mandelbrot.core.http.HttpProtocol
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
  import HttpProtocol._

  class RegistryEntries(tag: Tag) extends Table[(String,String,Long,Long,Long)](tag, "registry_entries") {
    def checkSystem = column[String]("checkSystem", O.PrimaryKey)
    def registration = column[String]("registration")
    def lsn = column[Long]("lsn")
    def joinedOn = column[Long]("joinedOn")
    def lastUpdate = column[Long]("lastUpdate")
    def * = (checkSystem, registration, lsn, joinedOn, lastUpdate)
  }

  val registryEntries = TableQuery[RegistryEntries]

  def list(limit: Int, token: Option[URI])(implicit session: Session): Iterable[RegistryEntry] = {
    token match {
      case Some(_uri) =>
        val uri = _uri.toString
        registryEntries.filter(_.checkSystem > uri).take(limit).list()
      case None =>
        registryEntries.take(limit).list()
    }
  }

  def get(systemUri: URI)(implicit session: Session): Option[RegistryEntry] = {
    registryEntries.filter(_.checkSystem === systemUri.toString).firstOption
  }

  def insert(systemUri: URI, registration: CheckRegistration, timestamp: DateTime)(implicit session: Session): Try[Long] = {
    val checkSystem: String = systemUri.toString
    registryEntries.filter(_.checkSystem === checkSystem).firstOption match {
      case None =>
        val registrationString: String = registration.toJson.prettyPrint
        val lsn = 1L
        val joinedOn: Long = timestamp.getMillis
        registryEntries += ((checkSystem, registrationString, lsn, joinedOn, joinedOn))
        Success(lsn)
      case _ =>
        Failure(ApiException(Conflict))
    }
  }

  def update(systemUri: URI, registration: CheckRegistration, timestamp: DateTime)(implicit session: Session): Try[Long] = {
    val checkSystem: String = systemUri.toString
    registryEntries.filter(_.checkSystem === checkSystem).map(e => e.lsn).firstOption match {
      case Some(lsn: Long) =>
        val registrationString: String = registration.toJson.prettyPrint
        val updatedLsn = lsn + 1
        val lastUpdate: Long = timestamp.getMillis
        registryEntries.filter(_.checkSystem === checkSystem)
          .map(e => (e.registration,e.lsn,e.lastUpdate))
          .update((registrationString, updatedLsn, lastUpdate))
        Success(updatedLsn)
      case None =>
        Failure(ApiException(ResourceNotFound))
    }
  }

  def delete(systemUri: URI, timestamp: DateTime)(implicit session: Session): Try[Long] = {
    val checkSystem = systemUri.toString
    registryEntries.filter(_.checkSystem === checkSystem).firstOption match {
      case Some((_, _, lsn: Long, _, _)) =>
        registryEntries.filter(_.checkSystem === checkSystem).delete
        Success(lsn)
      case None =>
        Failure(ApiException(ResourceNotFound))
    }
  }

  object RegistryEntries {
    type RegistryEntry = (String,String,Long,Long,Long)
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
  import HttpProtocol._

  // abstract members
  val db: Database
  val dal: RegistryDAL

  def receive = {

    case command: CreateRegistration =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      db.withSession { implicit session =>
        dal.insert(command.uri, command.registration, timestamp) match {
          case Success(lsn) => sender() ! CreateRegistrationResult(command, 1)
          case Failure(ex) => sender() ! RegistryServiceOperationFailed(command, ex)
        }
      }

    case command: UpdateRegistration =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      db.withSession { implicit session =>
        dal.update(command.uri, command.registration, timestamp) match {
          case Success(lsn) => sender() ! UpdateRegistrationResult(command, 1)
          case Failure(ex) => sender() ! RegistryServiceOperationFailed(command, ex)
        }
      }

    case command: DeleteRegistration =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      db.withSession { implicit session =>
        dal.delete(command.uri, timestamp) match {
          case Success(lsn) => sender() ! DeleteRegistrationResult(command, 1)
          case Failure(ex) => sender() ! RegistryServiceOperationFailed(command, ex)
        }
      }

    case query: GetRegistration =>
      db.withSession { implicit session =>
        dal.get(query.uri) match {
          case Some((_, registration, lsn, _, _)) =>
            sender() ! GetRegistrationResult(query, JsonParser(registration).convertTo[CheckRegistration], lsn)
          case None =>
            sender() ! RegistryServiceOperationFailed(query, ApiException(ResourceNotFound))
        }
      }

    case query: ListRegistrations =>
      db.withSession { implicit session =>
        val entries = dal.list(query.limit, query.token).toVector
        val systems = entries.map { case (checkSystem,_,lsn,joinedOn,lastUpdate) =>
          new URI(checkSystem) -> CheckSystemMetadata(new DateTime(joinedOn), new DateTime(lastUpdate))
        }.toMap
        entries.lastOption match {
          case Some((checkSystem, _, _, _, _)) =>
            sender() ! ListRegistrationsResult(query, systems, Some(new URI(checkSystem)))
          case None =>
            sender() ! ListRegistrationsResult(query, Map.empty, None)
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
