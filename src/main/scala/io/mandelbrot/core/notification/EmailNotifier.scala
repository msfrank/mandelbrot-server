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

/**
 *
 */
class EmailNotifier extends Actor with ActorLogging {

  // config
  val props = new java.util.Properties()
  props.put("mail.smtp.host", "smtp.myisp.com")
  val senderAddress = new InternetAddress("server@mandelbrot.io")

  // state
  val session = Session.getDefaultInstance(props, null)


  def receive = {

    case NotifyContact(contact, notification) =>
      val msg = new MimeMessage(session)
      try {
        msg.setFrom(senderAddress)
        msg.setRecipient(Message.RecipientType.TO, new InternetAddress(contact.address))
        msg.setSubject("subject")
        msg.setText("Hi,\n\nHow are you?")
        Transport.send(msg)
      } catch {
        case ex: MessagingException =>
      }
  }
}

object EmailNotifier {
  def props() = Props(classOf[EmailNotifier])
}
