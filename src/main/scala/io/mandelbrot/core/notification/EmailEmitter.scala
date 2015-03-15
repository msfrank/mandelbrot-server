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

package io.mandelbrot.core.notification

import com.typesafe.config.Config
import akka.actor._
import akka.pattern.pipe
import javax.mail.{Transport, MessagingException, Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.event.{TransportEvent, ConnectionEvent, TransportListener, ConnectionListener}
import scala.concurrent.Future
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

import io.mandelbrot.core.model._

/**
 *
 */
class EmailEmitter(notifierSettings: EmailEmitterSettings) extends LoggingFSM[EmailEmitter.State,EmailEmitter.Data] {
  import EmailEmitter._
  import context.dispatcher

  // config
  val maxQueued = 100   // TODO: implement bounded command buffer
  val senderAddress = new InternetAddress(notifierSettings.senderAddress)
  val sleepingTimeout = 30.seconds
  val connectTimeout = 10.seconds
  val readTimeout = 10.seconds
  val writeTimeout = 10.seconds

  // state
  val emailContacts: Map[String,EmailContact] = Map.empty

  // configure the session and underlying transport
  val props = new java.util.Properties()
  props.put("mail.transport.protocol", "smtp")
  props.put("mail.smtp.host", notifierSettings.smtpServer)
  props.put("mail.smtp.port", notifierSettings.smtpPort.toString)
  props.put("mail.smtp.auth", if (notifierSettings.requireAuth) "true" else "false")
  props.put("mail.smtp.connectiontimeout", connectTimeout.toMillis.toString)
  props.put("mail.smtp.timeout", readTimeout.toMillis.toString)
  props.put("mail.smtp.writetimeout", writeTimeout.toMillis.toString)
  //props.put("mail.user", "username")
  //props.put("mail.password", "password")
  val session = Session.getDefaultInstance(props, null)
  val transport = session.getTransport
  transport.addConnectionListener(new EmailConnectionListener(self))
  transport.addTransportListener(new EmailTransportListener(self))

  // start in Connecting state
  startWith(Connecting, CommandBuffer(Vector.empty))

  // invoke initial connect to smtp server
  connect().pipeTo(self)

  when(Connecting) {

    // buffer notifications until we are connected
    case Event(command: NotifyContact, CommandBuffer(buffer)) =>
      if (emailContacts.contains(command.contact.id)) {
        stay() using CommandBuffer(buffer :+ command)
      } else stay()

    // connect succeeded and we have no queued notifications
    case Event(Success(_), CommandBuffer(buffer)) if buffer.isEmpty =>
      goto(Connected)

    // connect succeeded and we have queued notifications
    case Event(Success(_), CommandBuffer(buffer)) =>
      self ! ProcessCommand
      goto(Connected)

    // connect failed
    case Event(Failure(ex), _) =>
      log.debug("failed to connect: {}", ex.getMessage)
      setTimer("sleep-timeout", SleepExpires, sleepingTimeout)
      goto(Sleeping)

    // ignore ProcessCommand message in Connecting state
    case Event(ProcessCommand, _) =>
      stay()
  }

  when(Connected) {

    // if the notifier isn't configured to handle the contact, then ignore
    case Event(NotifyContact(contact, _), _) if !emailContacts.contains(contact.id) =>
      stay()

    // if transport isn't connected, then buffer the notification and move to Connecting state
    case Event(command: NotifyContact, CommandBuffer(buffer)) if !transport.isConnected =>
      connect().pipeTo(self)
      goto(Connecting) using CommandBuffer(buffer :+ command)

    // buffer the notification and immediately signal to send
    case Event(command: NotifyContact, CommandBuffer(buffer)) =>
      if (buffer.isEmpty)
        self ! ProcessCommand
      stay() using CommandBuffer(buffer :+ command)

    // send the email message
    case Event(ProcessCommand, CommandBuffer(buffer)) =>
      sendMessage(buffer.head).pipeTo(self)
      stay()

    // email message send succeeded, remove the command from the queue
    case Event(Success(command), CommandBuffer(buffer)) =>
      if (buffer.length > 1)
        self ! ProcessCommand
      stay() using CommandBuffer(buffer.tail)

    // email message send failed
    case Event(Failure(ex), CommandBuffer(buffer)) =>
      log.debug("failed to send message: {}", ex.getMessage)
      stay() using CommandBuffer(buffer.tail)
  }

  when(Sleeping) {

    // buffer notifications until we are connected
    case Event(command: NotifyContact, CommandBuffer(buffer)) =>
      if (emailContacts.contains(command.contact.id)) {
        stay() using CommandBuffer(buffer :+ command)
      } else stay()

    // we have slept long enough, retry connecting to smtp server
    case Event(SleepExpires, _) =>
      connect().pipeTo(self)
      goto(Connecting)

    // ignore ProcessCommand message in Sleeping state
    case Event(ProcessCommand, _) =>
      stay()
  }

  def connect(): Future[Try[Transport]] = Future {
    try {
      log.debug("connecting to smtp server {}:{}", notifierSettings.smtpServer, notifierSettings.smtpPort)
      transport.connect()
      Success(transport)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def sendMessage(message: NotifyContact): Future[Try[NotifyContact]] = Future {
    try {
      val msg = new MimeMessage(session)
      msg.setFrom(senderAddress)
      val recipientAddress = new InternetAddress(emailContacts(message.contact.id).address)
      msg.setRecipient(Message.RecipientType.TO, recipientAddress)
      msg.setSubject("Mandelbrot notification")
      msg.setText(message.notification.toString)
      log.debug("sending email to {} => {}", recipientAddress, msg.getContent.toString)
      transport.sendMessage(msg, Array(recipientAddress))
      Success(message)
    } catch {
      case ex: Throwable =>
        Failure(ex)
    }
  }
}

object EmailEmitter {
  def props(settings: EmailEmitterSettings) = Props(classOf[EmailEmitter], settings)

  sealed trait State
  case object Sleeping extends State
  case object Connecting extends State
  case object Connected extends State

  sealed trait Data
  case class CommandBuffer(queued: Vector[NotifyContact]) extends Data

  case object ProcessCommand
  case object SleepExpires
}

case class EmailContact(contact: Contact, address: String)

case class EmailEmitterSettings(smtpServer: String,
                                smtpPort: Int,
                                enableTls: Boolean,
                                requireAuth: Boolean,
                                senderAddress: String)


class EmailNotificationEmitter extends NotificationEmitterExtension {
  type Settings = EmailEmitterSettings
  def configure(config: Config): Settings = {
    val smtpServer = config.getString("smtp-server")
    val smtpPort = config.getInt("smtp-port")
    val enableTls = config.getBoolean("enable-tls")
    val requireAuth = config.getBoolean("require-auth")
    val senderAddress = config.getString("sender-address")
    EmailEmitterSettings(smtpServer, smtpPort, enableTls, requireAuth, senderAddress)
  }
  def props(settings: Settings): Props = EmailEmitter.props(settings)
}

/**
 *
 */
class EmailConnectionListener(notifier: ActorRef) extends ConnectionListener {
  val logger = LoggerFactory.getLogger(classOf[EmailConnectionListener])

  override def opened(e: ConnectionEvent): Unit = logger.debug("connected: " + e)

  override def disconnected(e: ConnectionEvent): Unit = logger.debug("disconnected: " + e)

  override def closed(e: ConnectionEvent): Unit = logger.debug("closed: " + e)
}

/**
 *
 */
class EmailTransportListener(notifier: ActorRef) extends TransportListener {
  val logger = LoggerFactory.getLogger(classOf[EmailConnectionListener])

  override def messageDelivered(e: TransportEvent): Unit = logger.debug("message delivered: " + e)

  override def messagePartiallyDelivered(e: TransportEvent): Unit = logger.debug("message partially delivered: " + e)

  override def messageNotDelivered(e: TransportEvent): Unit = logger.debug("message not delivered: " + e)
}
