package io.mandelbrot.core

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import akka.actor.{Props, Actor}

class ServiceExtensionSpec extends WordSpec with ShouldMatchers {

  "A ServiceExtension class" should {

    "return true if the class implements a specified interface" in {
      ServiceExtension.pluginImplements("io.mandelbrot.core.TestExtension", classOf[TestInterface]) should be(true)
    }

    "return a Props instance from applying the props() method of a compatible implementation" in {
      val props = ServiceExtension.makePluginProps("io.mandelbrot.core.TestExtension", None)
      props.clazz shouldEqual classOf[TestExtension]
    }
  }
}

trait TestInterface

class TestExtension extends Actor with TestInterface {
  def receive = {
    case _ => sender() ! None
  }
}

object TestExtension {
  def props() = Props(classOf[TestExtension])
}
