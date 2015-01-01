package io.mandelbrot.persistence.slick


import com.typesafe.config.Config
import akka.actor.{Props, ActorLogging, Actor}
import scala.slick.driver.JdbcProfile
import scala.slick.jdbc.JdbcBackend.Database
import org.joda.time.DateTime
import java.io.File
import java.util.UUID

import io.mandelbrot.core.system._
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
    def probeRef = column[String]("probeRef")
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
    def * = (probeRef, generation, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched, context)
  }

  val stateEntries = TableQuery[StateEntries]

  def get(ref: ProbeRef)(implicit session: Session): Option[ProbeState] = {
    val probeRef = ref.toString
    stateEntries.filter(_.probeRef === probeRef).firstOption.map(stateEntry2ProbeState)
  }

  def update(status: ProbeStatus, lsn: Long)(implicit session: Session): Unit = {
    val probeRef = status.probeRef.toString
    val timestamp = status.timestamp.getMillis
    val lifecycle = status.lifecycle.toString
    val health = status.health.toString
    val summary = status.summary
    val lastUpdate = status.lastUpdate.map(_.getMillis)
    val lastChange = status.lastChange.map(_.getMillis)
    val correlation = status.correlation
    val acknowledged = status.acknowledged
    val squelched = status.squelched
    stateEntries += ((probeRef, lsn, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched, ""))
  }

  def delete(ref: ProbeRef)(implicit session: Session): Unit = {

  }

  object StateEntries {
    type StateEntry = (String,Long,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean,String)

    def stateEntry2ProbeState(entry: StateEntry): ProbeState = {
      val probeRef = ProbeRef(entry._1)
      val generation = entry._2
      val timestamp = new DateTime(entry._3)
      val lifecycle = entry._4 match {
        case "initializing" => ProbeInitializing
        case "joining" => ProbeJoining
        case "known" => ProbeKnown
        case "synthetic" => ProbeSynthetic
        case "retired" => ProbeRetired
      }
      val health = entry._5 match {
        case "healthy" => ProbeHealthy
        case "degraded" => ProbeDegraded
        case "failed" => ProbeFailed
        case "unknown" => ProbeUnknown
      }
      val summary = entry._6
      val lastUpdate = entry._7.map(new DateTime(_))
      val lastChange = entry._8.map(new DateTime(_))
      val correlation = entry._9
      val acknowledged = entry._10
      val squelched = entry._11
      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
      ProbeState(status, generation, None)
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

    case op: InitializeProbeState =>
      db.withSession { implicit session =>
        dal.get(op.ref) match {
          case Some(state) =>
            sender() ! InitializeProbeStateResult(op, state.status, state.lsn)
          case None =>
            val status = ProbeStatus(op.ref, op.timestamp, ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
            sender() ! InitializeProbeStateResult(op, status, 0)
        }
      }

    case op: GetProbeState =>
      db.withSession { implicit session =>
        dal.get(op.probeRef) match {
          case Some(state) =>
            sender() ! GetProbeStateResult(op, state.status, state.lsn)
          case None =>
            sender() ! StateServiceOperationFailed(op, new ApiException(ResourceNotFound))
        }
      }

    case op: UpdateProbeState =>
      db.withSession { implicit session =>
        try {
          dal.update(op.status, op.lsn)
          sender() ! UpdateProbeStateResult(op)
        } catch {
          case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
        }
      }

    case op: DeleteProbeState =>
      db.withSession { implicit session =>
        try {
          dal.delete(op.ref)
          sender() ! DeleteProbeStateResult(op)
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
