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
import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import javax.mail.{Transport, MessagingException, Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.event.{TransportEvent, ConnectionEvent, TransportListener, ConnectionListener}
import scala.concurrent.Future
import org.slf4j.LoggerFactory

/**
 *
 */
class EmailNotifier(notifierSettings: EmailNotifier.NotifierSettings) extends Actor with Notifier with ActorLogging {
  import context.dispatcher

  // config
  val props = new java.util.Properties()
  props.put("mail.smtp.host", notifierSettings.smtpServer)
  props.put("mail.smtp.port", notifierSettings.smtpPort.toString)
  props.put("mail.transport.protocol", "smtp")
  props.put("mail.smtp.auth", if (notifierSettings.requireAuth) "true" else "false")
  //props.put("mail.user", "username")
  //props.put("mail.password", "password")
  val senderAddress = new InternetAddress(notifierSettings.senderAddress)

  val session = Session.getDefaultInstance(props, null)
  val transport = session.getTransport
  transport.addConnectionListener(new EmailConnectionListener(self))
  transport.addTransportListener(new EmailTransportListener(self))
  transport.connect()

  def receive = {

    case NotifyContact(contact, notification) =>
      notifierSettings.contacts.get(contact.id) match {
        case Some(emailContact) =>
          try {
            val msg = new MimeMessage(session)
            msg.setFrom(senderAddress)
            val recipientAddress = new InternetAddress(emailContact.address)
            msg.setRecipient(Message.RecipientType.TO, recipientAddress)
            msg.setSubject("Mandelbrot notification")
            msg.setText(notification.toString)
            log.debug("sending email to {} => {}", emailContact.address, msg.getContent.toString)
            transport.sendMessage(msg, Array(recipientAddress))
          } catch {
            case ex: Throwable =>
              log.debug("failed to send email to {}: {}", emailContact.address, ex.getMessage)
          }
        case None =>  // do nothing
      }

  }
}

object EmailNotifier {
  def props(notifierSettings: NotifierSettings) = Props(classOf[EmailNotifier], notifierSettings)

  case class EmailContact(contact: Contact, address: String)
  case class NotifierSettings(smtpServer: String,
                              smtpPort: Int,
                              enableTls: Boolean,
                              requireAuth: Boolean,
                              senderAddress: String,
                              contacts: Map[String,EmailContact])

  def settings(config: Config, contacts: Map[Contact,Config]): Option[NotifierSettings] = {
    val smtpServer = config.getString("smtp-server")
    val smtpPort = config.getInt("smtp-port")
    val enableTls = config.getBoolean("enable-tls")
    val requireAuth = config.getBoolean("require-auth")
    val senderAddress = config.getString("sender-address")
    val emailContacts = contacts.map { case (contact,contactConfig) =>
      contact.id -> EmailContact(contact, contactConfig.getString("email-address"))
    }.toMap
    Some(NotifierSettings(smtpServer, smtpPort, enableTls, requireAuth, senderAddress, emailContacts))
  }
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
