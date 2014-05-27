package io.mandelbrot.core

import akka.actor.{Actor, Props}

class Blackhole extends Actor {
  def receive = {
    case _ =>
  }
}

object Blackhole {
  def props() = Props(classOf[Blackhole])
}