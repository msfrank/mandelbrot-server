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

package io.mandelbrot.core.cluster

import akka.actor._
import java.util

import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.cluster.EntityFunctions.{PropsCreator, KeyExtractor}

/**
 * ShardEntities manages entities for a single shard.  The ShardEntities actor
 * receives EntityEnvelope messages and sends the envelope contents to the appropriate
 * entity based on the entity key returned by the keyExtractor.  if the entity doesn't
 * exist, then the entity will be created if and only if a props instance is returned
 * from the propsCreator; otherwise, an EntityDeliveryFailed message will be returned
 * to the sender, with failure specified as ApiException(ResourceNotFound).
 */
class ShardEntities(services: ActorRef,
                    keyExtractor: KeyExtractor,
                    propsCreator: PropsCreator,
                    shardId: Int,
                    width: Int) extends Actor with ActorLogging {

  val refsByKey = new util.HashMap[String,ActorRef]()
  val keysByRef = new util.HashMap[ActorRef,String]()

  def receive = {

    case envelope: EntityEnvelope =>
      val entityKey = keyExtractor(envelope.message)
      refsByKey.get(entityKey) match {
        // entity doesn't exist in shard
        case null =>
          // if this message creates props, then create the actor
          if (propsCreator.isDefinedAt(envelope.message)) {
            val props = propsCreator(envelope.message)
            val entity = context.actorOf(props)
            context.watch(entity)
            refsByKey.put(entityKey, entity)
            keysByRef.put(entity, entityKey)
            entity.tell(envelope.message, envelope.sender)
          } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))
        // entity exists, forward the message to it
        case entity: ActorRef =>
          entity.tell(envelope.message, envelope.sender)
      }

    case Terminated(entityRef) =>
      val entityKey = keysByRef.remove(entityRef)
      refsByKey.remove(entityKey)
  }
}

object ShardEntities {
  def props(services: ActorRef, keyExtractor: KeyExtractor, propsCreator: PropsCreator, shardId: Int, width: Int) = {
    Props(classOf[ShardEntities], services, keyExtractor, propsCreator, shardId, width)
  }
}
