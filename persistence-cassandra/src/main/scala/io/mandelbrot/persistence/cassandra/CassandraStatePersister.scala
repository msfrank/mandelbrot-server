package io.mandelbrot.persistence.cassandra

import akka.actor.Status.Failure
import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future

import io.mandelbrot.core.state._
import io.mandelbrot.core.model._
import io.mandelbrot.core._

import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class CassandraStatePersister(settings: CassandraStatePersisterSettings) extends Actor with ActorLogging {
  import context.dispatcher

  // state
  val session = Cassandra(context.system).getSession
  val checkStatusIndexDAL = new CheckStatusIndexDAL(settings, session, context.dispatcher)
  val checkStatusDAL = new CheckStatusDAL(settings, session, context.dispatcher)

  def receive = {

    case op: InitializeCheckStatus =>
      val props = InitializeCheckStatusTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    case op: UpdateCheckStatus =>
      val props = UpdateCheckStatusTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    case op: DeleteCheckStatus =>
      sender() ! StateServiceOperationFailed(op, ApiException(NotImplemented))

    case op: TrimCheckHistory =>
      sender() ! StateServiceOperationFailed(op, ApiException(NotImplemented))

    /* retrieve condition history for the specified CheckRef */
    case op: GetConditionHistory =>
      val props = QueryCheckConditionTask.props(op, sender(), checkStatusIndexDAL, checkStatusDAL)
      context.actorOf(props)

    /* retrieve notification history for the specified CheckRef */
    case op: GetNotificationsHistory =>
      sender() ! StateServiceOperationFailed(op, ApiException(NotImplemented))

    /* retrieve metrics history for the specified CheckRef */
    case op: GetMetricsHistory =>
      sender() ! StateServiceOperationFailed(op, ApiException(NotImplemented))
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
