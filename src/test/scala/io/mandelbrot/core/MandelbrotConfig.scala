package io.mandelbrot.core

import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.ActorRef

trait ComposableConfig {
  def config: Config
  def ++(other: MergedConfig): MergedConfig = new MergedConfig(other.config.withFallback(config))
  def ++(other: ComposableConfig): MergedConfig = new MergedConfig(other.config.withFallback(config))
  def +(other: MergedConfig): Config = other.config.withFallback(config)
  def +(other: ComposableConfig): Config = other.config.withFallback(config)
  def +(other: Config): Config = other.withFallback(config)
}

class MergedConfig(val config: Config) extends ComposableConfig

//class ServiceMock(testActor: ActorRef) extends ComposableConfig {
//  val config = ConfigFactory.parseString(
//    """
//      |service-mock {
//      |  plugin = "io.mandelbrot.core.TestService"
//      |  plugin-settings {
//      |    forward-to = "%s"
//      |  }
//      |}
//    """.format(testActor.path).stripMargin)
//}

object MandelbrotConfig extends ComposableConfig {
  val config = ConfigFactory.parseString("mandelbrot {}")
}

object AkkaConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = DEBUG
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  actor {
      |    debug {
      |      receive = on
      |      autoreceive = on
      |      lifecycle = on
      |      fsm = on
      |      event-stream = on
      |      unhandled = on
      |      router-misconfiguration = on
      |    }
      |  }
      |}
    """.stripMargin)
}

object SprayConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |spray {
      |  can {
      |    server {
      |      idle-timeout = 60 s
      |    }
      |  }
      |}
    """.stripMargin)
}

object ConfigConversions {
  import scala.language.implicitConversions
  implicit def composableConfig2Config(composable: ComposableConfig): Config = composable.config
  implicit def mergedConfig2Config(merged: MergedConfig): Config = merged.config
}
