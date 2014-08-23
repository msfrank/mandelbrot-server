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

package io.mandelbrot.core.system

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
