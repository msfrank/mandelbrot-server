package io.mandelbrot.core.cluster

import akka.actor._
import java.util

import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.cluster.EntityFunctions.{PropsCreator, KeyExtractor}

/**
 *
 */
class ShardEntities(keyExtractor: KeyExtractor, propsCreator: PropsCreator) extends Actor with ActorLogging {

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
  def props(keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[ShardEntities], keyExtractor, propsCreator)
  }
}
