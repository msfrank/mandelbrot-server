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
import io.mandelbrot.core.cluster.EntityFunctions.{ShardResolver, PropsCreator, KeyExtractor}

/**
 * ShardEntities manages entities for a single shard.  The ShardEntities actor
 * receives EntityEnvelope messages and sends the envelope contents to the appropriate
 * entity based on the entity key returned by the keyExtractor.  if the entity doesn't
 * exist, then the entity will be created if and only if a props instance is returned
 * from the propsCreator; otherwise, an EntityDeliveryFailed message will be returned
 * to the sender, with failure specified as ApiException(ResourceNotFound).
 */
class ShardEntities(services: ActorRef,
                    shardResolver: ShardResolver,
                    keyExtractor: KeyExtractor,
                    propsCreator: PropsCreator,
                    shardId: Int,
                    width: Int) extends Actor with ActorLogging with Stash {

  // config
  val limit = 100

  // state
  val refsByKey = new util.HashMap[String,ActorRef]()
  val entitiesByRef = new util.HashMap[ActorRef,Entity]()
  val creatingEntities = new util.HashSet[Entity]()
  val deletingEntities = new util.HashSet[Entity]()

  override def preStart(): Unit = {
    services ! ListEntities(shardId, width, limit, None)
  }

  def initializing: Receive = {

    case envelope: EntityEnvelope =>
      stash()

    case result: ListEntitiesResult =>
      log.debug("received entities for shard {}:{}: {}", result.op.shardId, result.op.width, result.entities.mkString(","))
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
          log.debug("finished initializing entities for shard {}:{}", shardId, width)
          unstashAll()
          context.become(running)
        case next => services ! ListEntities(shardId, width, limit, next)
      }

    case ClusterServiceOperationFailed(op, failure) =>
      log.debug("operation {} failed: {}", op, failure)
      // FIXME: use a backoff strategy
      services ! op

    case Terminated(entityRef) =>
      val entity @ Entity(shardKey,entityKey) = entitiesByRef.remove(entityRef)
      refsByKey.remove(entityKey)
      services ! DeleteEntity(shardKey, entityKey)
      deletingEntities.add(entity)

    case result: DeleteEntityResult =>
      val DeleteEntity(shardKey, entityKey) = result.op
      deletingEntities.remove(Entity(shardKey, entityKey))
  }

  def running: Receive = {

    case envelope: EntityEnvelope =>
      val entityKey = keyExtractor(envelope.message)
      refsByKey.get(entityKey) match {
        // entity doesn't exist in shard
        case null =>
          // if this message creates props, then create the actor
          if (propsCreator.isDefinedAt(envelope.message)) {
            val props = propsCreator(envelope.message)
            val shardKey = shardResolver(envelope.message)
            val entity = Entity(shardKey, entityKey)
            val actor = context.actorOf(props)
            context.watch(actor)
            refsByKey.put(entityKey, actor)
            entitiesByRef.put(actor, entity)
            // send the envelope message to the entity
            actor.tell(envelope.message, envelope.sender)
            // create the entity entry
            services ! CreateEntity(shardKey, entityKey)
            creatingEntities.add(entity)
          } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))
        // entity exists, forward the message to it
        case entity: ActorRef =>
          entity.tell(envelope.message, envelope.sender)
      }

    case result: CreateEntityResult =>
      val CreateEntity(shardKey, entityKey) = result.op
      creatingEntities.remove(Entity(shardKey, entityKey)) 
      val entity = refsByKey.get(entityKey)
      entity ! StartEntity
      
    case Terminated(entityRef) =>
      val entity @ Entity(shardKey,entityKey) = entitiesByRef.remove(entityRef)
      refsByKey.remove(entityKey)
      services ! DeleteEntity(shardKey, entityKey)
      deletingEntities.add(entity)

    case result: DeleteEntityResult =>
      val DeleteEntity(shardKey, entityKey) = result.op
      deletingEntities.remove(Entity(shardKey, entityKey))

    case ClusterServiceOperationFailed(op, failure) =>
      // TODO: what do we do here?
      log.debug("operation {} failed: {}", op, failure)
  }

  def receive = initializing
}

object ShardEntities {
  def props(services: ActorRef,
            shardResolver: ShardResolver,
            keyExtractor: KeyExtractor,
            propsCreator: PropsCreator,
            shardId: Int,
            width: Int) = {
    Props(classOf[ShardEntities], services, shardResolver, keyExtractor, propsCreator, shardId, width)
  }
}

case object StartEntity
