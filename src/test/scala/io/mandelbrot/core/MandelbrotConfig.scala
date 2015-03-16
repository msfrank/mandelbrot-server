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

object MandelbrotConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |mandelbrot {
      |  config {
      |    file = []
      |  }
      |  http {
      |    interface = localhost
      |    port = 8080
      |    backlog = 10
      |    page-limit = 100
      |    request-timeout = 10 seconds
      |    debug-exceptions = false
      |  }
      |  cluster {
      |    enabled = false
      |    seed-nodes = []
      |    min-nr-members = 0
      |    total-shards = 4
      |    delivery-attempts = 3
      |    balancer-handover-retries = 10
      |    balancer-takeover-retries = 5
      |    balancer-retry-interval = 1 second
      |    plugin = "io.mandelbrot.core.entity.TestEntityCoordinatorExtension"
      |    plugin-settings { }
      |  }
      |  registry {
      |    min-joining-timeout = 1 minute
      |    min-probe-timeout = 1 minute
      |    min-alert-timeout = 1 minute
      |    min-leaving-timeout = 1 minute
      |    plugin = "io.mandelbrot.core.registry.TestRegistryPersisterExtension"
      |    plugin-settings { }
      |  }
      |  state {
      |    max-summary-size = 1k
      |    max-detail-size = 64k
      |    status-history-age = 30 days
      |    default-search-limit = 100
      |    snapshot-initial-delay = 1 minute
      |    snapshot-interval = 1 hour
      |    plugin = "io.mandelbrot.core.state.TestStatePersisterExtension"
      |    plugin-settings { }
      |  }
      |  notification {
      |    cleaner-initial-delay = 2 minutes
      |    cleaner-interval = 5 minutes
      |    stale-window-overlap = 10 minutes
      |    snapshot-initial-delay = 1 minute
      |    snapshot-interval = 1 hour
      |    contacts {}
      |    groups {}
      |    notifiers {}
      |    notification-rules-file = conf/notification.rules
      |  }
      |  shutdown-timeout = 30 seconds
      |}
    """.stripMargin)
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
