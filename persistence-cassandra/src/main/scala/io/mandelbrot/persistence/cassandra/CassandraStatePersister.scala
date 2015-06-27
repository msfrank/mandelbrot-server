package io.mandelbrot.persistence.cassandra

import akka.actor.{OneForOneStrategy, Props, ActorLogging, Actor}
import akka.actor.SupervisorStrategy.{Restart, Stop}
import com.typesafe.config.Config
import com.datastax.driver.core.exceptions._
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core._
import io.mandelbrot.core.state._
import io.mandelbrot.persistence.cassandra.dal.{CheckStatusIndexDAL, CheckStatusDAL}
import io.mandelbrot.persistence.cassandra.task._

/**
 *
 */
class CassandraStatePersister(settings: CassandraStatePersisterSettings) extends Actor with ActorLogging {

  // state
  val session = Cassandra(context.system).getSession
  val checkStatusIndexDAL = new CheckStatusIndexDAL(settings, session, context.dispatcher)
  val checkStatusDAL = new CheckStatusDAL(settings, session, context.dispatcher)

  def receive = {

    case op: GetStatus =>
      val props = InitializeCheckStatusTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    case op: UpdateStatus =>
      val props = UpdateCheckStatusTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    case op: DeleteStatus =>
      val props = DeleteCheckStatusTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve the last condition for the specified CheckRef */
    case op: GetConditionHistory if op.from.isEmpty && op.to.isEmpty =>
      val props = LastCheckConditionTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve condition history for the specified CheckRef */
    case op: GetConditionHistory =>
      val props = GetCheckConditionTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve the last notifications for the specified CheckRef */
    case op: GetNotificationsHistory if op.from.isEmpty && op.to.isEmpty =>
      val props = LastCheckNotificationsTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve notification history for the specified CheckRef */
    case op: GetNotificationsHistory =>
      val props = GetCheckNotificationsTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve the last metrics for the specified CheckRef */
    case op: GetMetricsHistory if op.from.isEmpty && op.to.isEmpty =>
      val props = LastCheckMetricsTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve metrics history for the specified CheckRef */
    case op: GetMetricsHistory =>
      val props = GetCheckMetricsTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)
  }

  /**
   * convert the specified string containing seconds since the UNIX epoch
   * to a DateTime with UTC timezone.
   */
  def last2timestamp(last: String): DateTime = new DateTime(last.toLong).withZone(DateTimeZone.UTC)

  /**
   * convert the specified DateTime with UTC timezone to a string containing
   * seconds since the UNIX epoch.
   */
  def timestamp2last(timestamp: DateTime): String = timestamp.getMillis.toString

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3) {
    /* transient cassandra exceptions */
    case ex: QueryTimeoutException => Restart
    case ex: NoHostAvailableException => Restart
    case ex: UnavailableException => Restart
    /* if we receive any other exception then stop the task */
    case ex: Throwable => Stop
  }
}

object CassandraStatePersister {
  def props(settings: CassandraStatePersisterSettings) = Props(classOf[CassandraStatePersister], settings)
}

case class CassandraStatePersisterSettings()

class CassandraStatePersisterExtension extends StatePersisterExtension {
  type Settings = CassandraStatePersisterSettings
  def configure(config: Config): Settings = CassandraStatePersisterSettings()
  def props(settings: Settings): Props = CassandraStatePersister.props(settings)
}
