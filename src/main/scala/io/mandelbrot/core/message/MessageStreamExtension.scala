package io.mandelbrot.core.message

import akka.actor._
import akka.event.{EventBus, SubchannelClassification}
import akka.util.Subclassification

/**
 *
 */
class TypeSubclassification extends Subclassification[Class[_]] {
  override def isEqual(x: Class[_], y: Class[_]): Boolean = x == y
  override def isSubclass(x: Class[_], y: Class[_]): Boolean = y.isAssignableFrom(x)
}

/**
 * Publishes the payload of the MsgEnvelope when the topic of the
 * MsgEnvelope starts with the String specified when subscribing.
 */
class MessageStreamBus extends EventBus with SubchannelClassification {
  type Event = Message
  type Classifier = Class[_]
  type Subscriber = ActorRef

  override protected val subclassification: Subclassification[Classifier] = new TypeSubclassification
  override protected def classify(event: Message): Class[_] = event.getClass
  override protected def publish(event: Message, subscriber: ActorRef): Unit = subscriber ! event
}

/**
 *
 */
class MessageStreamExtensionImpl(system: ActorSystem) extends Extension {
  val messageStream = new MessageStreamBus()
}

/**
 *
 */
object MessageStreamExtension extends ExtensionId[MessageStreamExtensionImpl] with ExtensionIdProvider {
  override def lookup() = MessageStreamExtension
  override def createExtension(system: ExtendedActorSystem) = new MessageStreamExtensionImpl(system)
  override def get(system: ActorSystem): MessageStreamExtensionImpl = super.get(system)
}

object MessageStream {
  def apply(system: ActorSystem) = MessageStreamExtension(system).messageStream
}
