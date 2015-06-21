package io.mandelbrot.persistence.slick


import com.typesafe.config.Config
import akka.actor.{Props, ActorLogging, Actor}
import scala.slick.driver.JdbcProfile
import scala.slick.jdbc.JdbcBackend.Database
import org.joda.time.DateTime
import java.io.File
import java.util.UUID

import io.mandelbrot.core.check._
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ResourceNotFound, ApiException, ServerConfig}
import io.mandelbrot.persistence.slick.H2Persister.H2PersisterSettings

/*
 this multi-db code is heavily inspired by
 https://github.com/slick/slick-examples/blob/master/src/main/scala/com/typesafe/slick/examples/lifted/MultiDBCakeExample.scala
 */

trait PersisterProfile {
  val profile: JdbcProfile
}

/**
 *
 */
trait StateEntriesComponent { this: PersisterProfile =>
  import profile.simple._
  import StateEntries._

  class StateEntries(tag: Tag) extends Table[(String,Long,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean,String)](tag, "state_entries") {
    def checkRef = column[String]("checkRef")
    def generation = column[Long]("generation")
    def timestamp = column[Long]("timestamp")
    def lifecycle = column[String]("lifecycle")
    def health = column[String]("health")
    def summary = column[Option[String]]("summary")
    def lastUpdate = column[Option[Long]]("lastUpdate")
    def lastChange = column[Option[Long]]("lastChange")
    def correlation = column[Option[UUID]]("correlation")
    def acknowledged = column[Option[UUID]]("acknowledged")
    def squelched = column[Boolean]("squelched")
    def context = column[String]("context")
    def * = (checkRef, generation, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched, context)
  }

  val stateEntries = TableQuery[StateEntries]

  def get(ref: CheckRef)(implicit session: Session): Option[CheckState] = {
    val checkRef = ref.toString
    stateEntries.filter(_.checkRef === checkRef).firstOption.map(stateEntry2CheckState)
  }

  def update(status: CheckStatus, lsn: Long)(implicit session: Session): Unit = {
    val checkRef = status.checkRef.toString
    val timestamp = status.timestamp.getMillis
    val lifecycle = status.lifecycle.toString
    val health = status.health.toString
    val summary = status.summary
    val lastUpdate = status.lastUpdate.map(_.getMillis)
    val lastChange = status.lastChange.map(_.getMillis)
    val correlation = status.correlation
    val acknowledged = status.acknowledged
    val squelched = status.squelched
    stateEntries += ((checkRef, lsn, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched, ""))
  }

  def delete(ref: CheckRef)(implicit session: Session): Unit = {

  }

  object StateEntries {
    type StateEntry = (String,Long,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean,String)

    def stateEntry2CheckState(entry: StateEntry): CheckState = {
      val checkRef = CheckRef(entry._1)
      val generation = entry._2
      val timestamp = new DateTime(entry._3)
      val lifecycle = entry._4 match {
        case "initializing" => CheckInitializing
        case "joining" => CheckJoining
        case "known" => CheckKnown
        case "synthetic" => CheckSynthetic
        case "retired" => CheckRetired
      }
      val health = entry._5 match {
        case "healthy" => CheckHealthy
        case "degraded" => CheckDegraded
        case "failed" => CheckFailed
        case "unknown" => CheckUnknown
      }
      val summary = entry._6
      val lastUpdate = entry._7.map(new DateTime(_))
      val lastChange = entry._8.map(new DateTime(_))
      val correlation = entry._9
      val acknowledged = entry._10
      val squelched = entry._11
      val status = CheckStatus(checkRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
      CheckState(status, generation, None)
    }
  }
}

/**
 *
 */
class PersistDAL(override val profile: JdbcProfile) extends StateEntriesComponent with PersisterProfile {
  import profile.simple._

  def create(implicit session: Session): Unit = {
    try {
      stateEntries.ddl.create
    } catch {
      case ex: Throwable =>
    }
  }
}

/**
 *
 */
trait SlickPersister extends Actor with ActorLogging {
  import scala.slick.jdbc.JdbcBackend.Database

  // abstract members
  val db: Database
  val dal: PersistDAL

  // config
  val settings = ServerConfig(context.system).settings.state

  def receive = {

    case op: InitializeCheckStatus =>
      db.withSession { implicit session =>
        dal.get(op.ref) match {
          case Some(state) =>
            sender() ! InitializeCheckStatusResult(op, state.status, state.lsn)
          case None =>
            val status = CheckStatus(op.ref, op.timestamp, CheckInitializing, CheckUnknown, None, None, None, None, None, false)
            sender() ! InitializeCheckStatusResult(op, status, 0)
        }
      }

    case op: GetCheckState =>
      db.withSession { implicit session =>
        dal.get(op.checkRef) match {
          case Some(state) =>
            sender() ! GetCheckStateResult(op, state.status, state.lsn)
          case None =>
            sender() ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
        }
      }

    case op: UpdateCheckStatus =>
      db.withSession { implicit session =>
        try {
          dal.update(op.status, op.lsn)
          sender() ! UpdateCheckStatusResult(op)
        } catch {
          case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
        }
      }

    case op: DeleteCheckStatus =>
      db.withSession { implicit session =>
        try {
          dal.delete(op.ref)
          sender() ! DeleteCheckStatusResult(op)
        } catch {
          case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
        }
      }
  }

}

/**
 *
 */
class H2Persister(managerSettings: H2PersisterSettings) extends SlickPersister with Persister {
  import scala.slick.driver.H2Driver

   // config
  val url = "jdbc:h2:" + {
    if (managerSettings.inMemory) "mem:history" else "file:" + managerSettings.databasePath.getAbsolutePath + "/history"
  } + ";" + {
    if (managerSettings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!managerSettings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
  }

  val db = Database.forURL(url = url, driver = "org.h2.Driver")
  val dal = new PersistDAL(H2Driver)

  db.withSession { implicit session => dal.create }
}

object H2Persister {

  def props(managerSettings: H2PersisterSettings) = Props(classOf[H2Persister], managerSettings)

  case class H2PersisterSettings(databasePath: File, inMemory: Boolean, h2databaseToUpper: Boolean)
  def settings(config: Config): Option[H2PersisterSettings] = {
    val databasePath = new File(config.getString("database-path"))
    val inMemory = config.getBoolean("in-memory")
    val h2databaseToUpper = config.getBoolean("h2-database-to-upper")
    Some(H2PersisterSettings(databasePath, inMemory, h2databaseToUpper))
  }
}
