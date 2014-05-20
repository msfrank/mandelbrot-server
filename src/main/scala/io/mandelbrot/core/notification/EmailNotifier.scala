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

import akka.actor.{Props, ActorLogging, Actor}
import javax.mail.{Transport, MessagingException, Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import com.typesafe.config.Config
import scala.concurrent.Future

/**
 *
 */
class EmailNotifier(notifierSettings: EmailNotifier.NotifierSettings) extends Actor with Notifier with ActorLogging {
  import context.dispatcher

  // config
  val props = new java.util.Properties()
  props.put("mail.smtp.host", notifierSettings.smtpServer)
  props.put("mail.smtp.port", notifierSettings.smtpPort.toString)
  props.put("mail.transport.protocol", if (notifierSettings.enableTls) "smtps" else "smtp")
  props.put("mail.smtp.auth", if (notifierSettings.requireAuth) "true" else "false")
  //props.put("mail.user", "username")
  //props.put("mail.password", "password")
  val senderAddress = new InternetAddress(notifierSettings.senderAddress)

  // state
  val session = Session.getDefaultInstance(props, null)


  def receive = {

    case NotifyContact(contact, notification) =>
      notifierSettings.contacts.get(contact.id) match {
        case Some(emailContact) =>
          try {
            val msg = new MimeMessage(session)
            msg.setFrom(senderAddress)
            msg.setRecipient(Message.RecipientType.TO, new InternetAddress(emailContact.address))
            msg.setSubject("Mandelbrot notification")
            msg.setText(notification.toString)
            log.debug("sending email to {} => {}", emailContact.address, msg.getContent.toString)
            Future(Transport.send(msg))
          } catch {
            case ex: MessagingException =>
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
