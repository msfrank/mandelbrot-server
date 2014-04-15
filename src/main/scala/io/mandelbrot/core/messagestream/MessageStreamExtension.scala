package io.mandelbrot.core.messagestream

import akka.actor._
import akka.event.{EventBus, SubchannelClassification}
import akka.util.Subclassification

/**
 *
 */
class StartsWithSubclassification extends Subclassification[Array[String]] {
  override def isEqual(x: Array[String], y: Array[String]): Boolean = x == y
  override def isSubclass(x: Array[String], y: Array[String]): Boolean = x.startsWith(y)
}

/**
 * Publishes the payload of the MsgEnvelope when the topic of the
 * MsgEnvelope starts with the String specified when subscribing.
 */
class SubchannelBusImpl extends EventBus with SubchannelClassification {
  type Event = MandelbrotMessage
  type Classifier = Array[String]
  type Subscriber = ActorRef

  override protected val subclassification: Subclassification[Classifier] = new StartsWithSubclassification
  override protected def classify(event: Event): Classifier = event.topic
  override protected def publish(event: Event, subscriber: Subscriber): Unit = subscriber ! event.payload
}

/**
 *
 */
class MessageStreamExtensionImpl(system: ActorSystem) extends Extension {
  val messageStream = new SubchannelBusImpl()
  def subscribe(subscriber: ActorRef, to: Array[String]) = messageStream.subscribe(subscriber, to)
  def unsubscribe(subscriber: ActorRef, from: Array[String]) = messageStream.unsubscribe(subscriber, from)
  def publish(message: MandelbrotMessage) = messageStream.publish(message)
}

/**
 *
 */
object MessageStream extends ExtensionId[MessageStreamExtensionImpl] with ExtensionIdProvider {
  override def lookup() = MessageStream
  override def createExtension(system: ExtendedActorSystem) = new MessageStreamExtensionImpl(system)
  override def get(system: ActorSystem): MessageStreamExtensionImpl = super.get(system)
}
