package io.mandelbrot.core.messagestream

import akka.actor.{Props, ActorLogging, Actor}
import akka.zeromq._
import spray.json._

/**
 *
 */
class MessageStreamService(messageStreamSettings: MessageStreamSettings) extends Actor with ActorLogging {
  import MessagePayload._

  val messageStream = MessageStream(context.system).messageStream
  val socket = ZeroMQExtension(context.system).newSocket(SocketType.Sub, Listener(self), Connect(messageStreamSettings.endpoint))

  messageStreamSettings.subscriptions match {
    case None =>
      socket ! SubscribeAll
      log.debug("subscribing to all topics")
    case Some(subscriptions) =>
      subscriptions.foreach { topic => socket ! Subscribe(topic) }
      log.debug("subscribing to {}", subscriptions.mkString(", "))
  }

  def receive = {

    case Connecting =>
      log.debug("connecting to {}", messageStreamSettings.endpoint)

    case m: ZMQMessage =>
      // the first frame is the topic, second is the message
      val topic = m.frames(0).utf8String.split('.')
      val payload = m.frames(1).utf8String.toJson
      messageStream.publish(MandelbrotMessage(topic, payload))

    case Closed =>
      log.debug("closed {}", messageStreamSettings.endpoint)
  }
}

object MessageStreamService {
  def props(messageStreamSettings: MessageStreamSettings) = Props(classOf[MessageStreamService], messageStreamSettings)
}
