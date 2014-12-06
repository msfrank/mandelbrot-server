package io.mandelbrot.core.cluster

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config

class TestCoordinator extends Actor with ActorLogging with Coordinator {

  def receive = {

    case op: GetShard =>

    case op: UpdateMemberShards =>
  }
}

object TestCoordinator {
  def props() = Props(classOf[TestCoordinator])
  def settings(config: Config): Option[Any] = None
}

case class TestCoordinatorMessage(key: String, message: Any)
case class TestCoordinatorReply(message: Any)

class TestEntity extends Actor {
  def receive = {
    case TestCoordinatorMessage(key, message) => sender() ! TestCoordinatorReply(message)
  }
}

object TestEntity {
  def props() = Props(classOf[TestEntity])
}
