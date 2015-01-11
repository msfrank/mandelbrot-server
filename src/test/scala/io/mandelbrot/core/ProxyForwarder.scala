package io.mandelbrot.core

import akka.actor.{Actor, ActorRef, Props}

/**
 * forward messages from child to target and from target to child, unless the message
 * is from the child and is of type reflect.  in this case, send the message back to the
 * child.  this emulates the ServiceProxy actor.
 */
class ProxyForwarder(childProps: Props, target: ActorRef, reflect: Class[_]) extends Actor {
  val child = context.actorOf(childProps, "child")
  def receive = {
    case x if sender() == child =>
      if (x.getClass.isAssignableFrom(reflect))
        child ! x
      else
        target forward x
    case x => child forward x
  }
}

object ProxyForwarder {
  def props(childProps: Props, target: ActorRef, reflect: Class[_]) = Props(classOf[ProxyForwarder], childProps, target, reflect)
}
