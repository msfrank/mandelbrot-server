package io.mandelbrot.core

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.{Props, Actor}

class ServiceExtensionSpec extends WordSpec with MustMatchers {

  "A class extending ServiceExtension" must {

    "return a Props instance from applying the props() method of a compatible implementation" in {
      val extension = new TestExtension()
      val props = extension.makeServiceProps("io.mandelbrot.core.TestExtension", None)
      props.clazz must be === classOf[TestImplementation]
    }
  }
}

class TestImplementation extends Actor {
  def receive = {
    case _ => sender() ! None
  }
}
class TestExtension extends ServiceExtension

object TestExtension {
  def props() = Props(classOf[TestImplementation])
}
