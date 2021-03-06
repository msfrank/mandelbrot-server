package io.mandelbrot.core.entity

import akka.actor.{Actor, ActorLogging, Props}
import io.mandelbrot.core.ServiceCommand

class TestEntity extends Actor with ActorLogging {
  log.debug("initialized TestEntity")
  def receive = {
    case m @ TestEntityCreate(key, shard, message) =>
      log.debug("received {}", m)
      sender() ! TestCreateReply(message)
    case m @ TestEntityMessage(key, shard, message) =>
      log.debug("received {}", m)
      sender() ! TestMessageReply(message)
    case unknown => log.error("received unknown message: {}", unknown)
  }
}

object TestEntity {
  def props() = Props(classOf[TestEntity])

  val shardResolver: EntityFunctions.ShardResolver = {
    case m: TestEntityCreate => m.shard
    case m: TestEntityMessage => m.shard
  }
  val keyExtractor: EntityFunctions.KeyExtractor = {
    case m: TestEntityCreate => m.key
    case m: TestEntityMessage => m.key
  }
  val propsCreator: EntityFunctions.PropsCreator = {
    case m: TestEntityCreate => TestEntity.props()
  }
  val entityReviver: EntityFunctions.EntityReviver = {
    case m: String => TestEntityRevive()
  }
}

case class TestEntityCreate(key: String, shard: Int, message: Any) extends ServiceCommand
case class TestCreateReply(message: Any)

case class TestEntityMessage(key: String, shard: Int, message: Any) extends ServiceCommand
case class TestMessageReply(message: Any)

case class TestEntityRevive()
