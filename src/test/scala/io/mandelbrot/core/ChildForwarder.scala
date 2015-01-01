package io.mandelbrot.core

import akka.actor.{Actor, ActorRef, Props}

class ChildForwarder(childProps: Props, target: ActorRef) extends Actor {
  val child = context.actorOf(childProps, "child")
  def receive = {
    case x if sender() == child => target forward x
    case x => child forward x
  }
}

object ChildForwarder {
  def props(childProps: Props, target: ActorRef) = Props(classOf[ChildForwarder], childProps, target)
}
