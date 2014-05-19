package io.mandelbrot.core

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.{Props, Actor}

class ServiceExtensionSpec extends WordSpec with MustMatchers {

  "A class extending ServiceExtension" must {

    "return a Props instance from applying the props() method of a compatible implementation" in {
      val props = ServiceExtension.makeServiceProps("io.mandelbrot.core.TestExtension", None)
      props.clazz must be === classOf[TestExtension]
    }
  }
}

class TestExtension extends Actor {
  def receive = {
    case _ => sender() ! None
  }
}

object TestExtension {
  def props() = Props(classOf[TestExtension])
}
