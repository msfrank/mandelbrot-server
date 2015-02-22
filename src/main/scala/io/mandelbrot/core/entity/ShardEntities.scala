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

package io.mandelbrot.core.entity

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import scala.concurrent.Future
import java.util

import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.entity.EntityFunctions.{ShardResolver, PropsCreator, KeyExtractor}

/**
 * ShardEntities manages entities for a single shard.  The ShardEntities actor
 * receives EntityEnvelope messages and sends the envelope contents to the appropriate
 * entity based on the entity key returned by the keyExtractor.  if the entity doesn't
 * exist, then the entity will be created if and only if a props instance is returned
 * from the propsCreator; otherwise, an EntityDeliveryFailed message will be returned
 * to the sender, with failure specified as ApiException(ResourceNotFound).
 */
class ShardEntities(services: ActorRef,
                    propsCreator: PropsCreator,
                    shardId: Int) extends Actor with ActorLogging with Stash {

  // config
  val limit = 100

  // state
  val pendingEntities = new util.HashMap[Entity,EntityEnvelope]()
  val refsByKey = new util.HashMap[String,ActorRef]()
  val entitiesByRef = new util.HashMap[ActorRef,Entity]()

  log.debug("managing entities for shard {}", shardId)

  override def preStart(): Unit = services ! ListEntities(shardId, limit, None)

  def initializing: Receive = {

    case envelope: EntityEnvelope =>
      stash()

    case result: ListEntitiesResult =>
      log.debug("received {} entities for shard {}", result.entities.length, result.op.shardId)
      result.entities.foreach { entity =>
        val props = propsCreator(entity)
        val actor = context.actorOf(props)
        context.watch(actor)
        refsByKey.put(entity.entityKey, actor)
        entitiesByRef.put(actor, entity)
        // send the envelope message to the entity
        actor ! entity
      }
      result.token match {
        case None =>
          log.debug("finished initializing entities for shard {}", shardId)
          unstashAll()
          context.become(running)
        case next => services ! ListEntities(shardId, limit, next)
      }

    // FIXME: use a backoff strategy
    case EntityServiceOperationFailed(op: ListEntitiesResult, failure) =>
      log.debug("operation {} failed: {}", op, failure)

    case Terminated(entityRef) =>
      val entity: Entity = entitiesByRef.remove(entityRef)
      refsByKey.remove(entity.entityKey)
      services ! DeleteEntity(entity.shardId, entity.entityKey)

    case result: DeleteEntityResult =>
      // do nothing

    // FIXME: use a backoff strategy
    case EntityServiceOperationFailed(op: DeleteEntityResult, failure) =>
      log.debug("operation {} failed: {}", op, failure)
  }

  def running: Receive = {

    case envelope: EntityEnvelope =>
      refsByKey.get(envelope.entityKey) match {
        // entity doesn't exist in shard
        case null =>
          // if this message creates props, then create the actor
          if (propsCreator.isDefinedAt(envelope.op)) {
            val props = propsCreator(envelope.op)
            val entity = Entity(shardId, envelope.entityKey)
            val actor = context.actorOf(props)
            context.watch(actor)
            refsByKey.put(envelope.entityKey, actor)
            entitiesByRef.put(actor, entity)
            // save the envelope message to the entity
            pendingEntities.put(entity, envelope)
            // create the entity entry
            services ! CreateEntity(entity.shardId, entity.entityKey)
          } else envelope.sender ! EntityDeliveryFailed(envelope.op, ApiException(ResourceNotFound))
        // entity exists, forward the message to it
        case entity: ActorRef =>
          entity.tell(envelope.op, envelope.sender)
      }

    case result: CreateEntityResult =>
      val entity = Entity(result.op.shardId, result.op.entityKey)
      val envelope = pendingEntities.remove(entity)
      refsByKey.get(entity.entityKey).tell(envelope.op, envelope.sender)

    case EntityServiceOperationFailed(op: CreateEntity, failure) =>
      log.debug("operation {} failed: {}", op, failure)

    case Terminated(entityRef) =>
      val entity: Entity = entitiesByRef.remove(entityRef)
      refsByKey.remove(entity.entityKey)
      services ! DeleteEntity(entity.shardId, entity.entityKey)

    case result: DeleteEntityResult =>
      // do nothing

    // FIXME: use a backoff strategy
    case EntityServiceOperationFailed(op: DeleteEntityResult, failure) =>
      log.debug("operation {} failed: {}", op, failure)
  }

  def receive = initializing
}

object ShardEntities {
  def props(services: ActorRef,
            propsCreator: PropsCreator,
            shardId: Int) = {
    Props(classOf[ShardEntities], services, propsCreator, shardId)
  }
}
