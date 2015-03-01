package io.mandelbrot.persistence.cassandra

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.DateTime
import scala.concurrent.Future
import scala.concurrent.duration._
import java.util.Date

import io.mandelbrot.core.history._
import io.mandelbrot.core.system._
import io.mandelbrot.core.notification._

/**
 *
 */
//class CassandraArchiver(settings: CassandraArchiverSettings) extends Actor with ActorLogging with Archiver {
//  import CassandraArchiver._
//  import context.dispatcher
//
//  // config
//  val defaultLimit = 100
//
//  // state
//  var currEpoch: Long = millis2epoch(System.currentTimeMillis())
//  var prevEpoch: Long = currEpoch - EPOCH_TERM
//  var nextEpoch: Long = currEpoch + EPOCH_TERM
//  var checkEpoch: Option[Cancellable] = None
//
//  val session = Cassandra(context.system).getSession
//  val notificationHistory = new NotificationsDAL(settings, session)
//  val statusHistory = new ConditionsDAL(settings, session)
//
//  override def preStart(): Unit = {
//    checkEpoch = Some(context.system.scheduler.schedule(1.minute, 1.minute, self, CheckEpoch))
//  }
//
//  override def postStop(): Unit = checkEpoch.foreach(_.cancel())
//
//  def receive = {
//
//    /* recalculate the epoch if necessary */
//    case CheckEpoch =>
//      val epoch = System.currentTimeMillis()
//      if (epoch >= nextEpoch) {
//        prevEpoch = currEpoch
//        currEpoch = nextEpoch
//        nextEpoch = nextEpoch + EPOCH_TERM
//      }
//
//    /* append probe status to history */
//    case status: ProbeStatus =>
//      getEpoch(status.timestamp) match {
//        case Some(epoch) => statusHistory.insertStatus(status, epoch)
//        case None =>  // drop status
//      }
//
//    /* append notification to history */
//    case notification: ProbeNotification =>
//      getEpoch(notification.timestamp) match {
//        case Some(epoch) => notificationHistory.insertNotification(notification, epoch)
//        case None =>  // drop notification
//      }
//
//    /* retrieve status history for the ProbeRef and all its children */
//    case op: GetStatusHistory =>
//      val getEpoch = op.from match {
//        case Some(from) => Future.successful[Long](timestamp2epoch(from))
//        case None => statusHistory.getFirstStatusEpoch(op.probeRef).map[Long](_.getOrElse(-1))
//      }
//      getEpoch.flatMap[Vector[ProbeStatus]] { epoch =>
//        statusHistory.getStatusHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
//      }.map { history => GetStatusHistoryResult(op, history) }.recover {
//        case ex: Throwable => HistoryServiceOperationFailed(op, ex)
//      }.pipeTo(sender())
//
//    /* retrieve notification history for the ProbeRef and all its children */
//    case op: GetNotificationHistory =>
//      val getEpoch = op.from match {
//        case Some(from) => Future.successful[Long](timestamp2epoch(from))
//        case None => notificationHistory.getFirstNotificationEpoch(op.probeRef).map[Long](_.getOrElse(-1))
//      }
//      getEpoch.flatMap[Vector[ProbeNotification]] { epoch =>
//        notificationHistory.getNotificationHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
//      }.map { history => GetNotificationHistoryResult(op, history) }.recover {
//        case ex: Throwable => HistoryServiceOperationFailed(op, ex)
//      }.pipeTo(sender())
//
//    /* delete history older than statusHistoryAge */
//    case CleanHistory(mark) =>
//      //session.execute(bindCleanHistory(mark))
//  }
//
//  def getEpoch(timestamp: DateTime): Option[Long] = {
//    val millis = timestamp.getMillis
//    if (millis >= currEpoch && millis < nextEpoch)
//      Some(currEpoch)
//    else if (millis >= prevEpoch && millis < currEpoch)
//      Some(prevEpoch)
//    else if (millis >= nextEpoch && millis < currEpoch + EPOCH_TERM)
//      Some(nextEpoch)
//    else
//      None
//  }
//
//  def millis2epoch(millis: Long): Long = (millis / EPOCH_TERM) * EPOCH_TERM
//
//  def timestamp2epoch(timestamp: DateTime): Long = millis2epoch(timestamp.getMillis)
//
//}
//
//object CassandraArchiver {
//
//  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
//  val SMALLEST_DATE = new Date(0)
//  val EPOCH_TERM: Long = 60 * 60 * 24   // 1 day in seconds
//
//  def props(managerSettings: CassandraArchiverSettings) = Props(classOf[CassandraArchiver], managerSettings)
//
//  case class CassandraArchiverSettings()
//  def settings(config: Config): Option[CassandraArchiverSettings] = {
//    Some(CassandraArchiverSettings())
//  }
//
//  case object CheckEpoch
//}
