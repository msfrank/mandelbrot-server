package io.mandelbrot.core.registry

import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue, Config}
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.io.File
import java.net.URI

import io.mandelbrot.core.notification.{NotificationPolicyTypeSquelch, NotificationPolicyTypeEscalate, NotificationPolicyTypeEmit}
import java.util.concurrent.TimeUnit

class RegistrySettings(staticRegistry: Option[File])

object RegistrySettings {
  def parse(config: Config): RegistrySettings = {
    val staticRegistry = if (config.hasPath("static-registry")) Some(new File(config.getString("static-registry"))) else None
    new RegistrySettings(staticRegistry)
  }
}

class StaticRegistry(config: Config, registrySettings: RegistrySettings) {

  // static registry defaults
  val staticJoiningTimeout = 5.minutes
  val staticProbeTimeout = 5.minutes
  val staticLeavingTimeout = 5.minutes
  val staticFlapWindow = 10.minutes
  val staticFlapDeviations = 10
  val staticNotificationPolicyType = NotificationPolicyTypeEmit

  val systems: Map[URI,ProbeSpec] = if (config.hasPath("registry.systems")) {
    config.getConfig("registry.systems").entrySet.map { case entry =>
      val system = new URI(entry.getKey)
      entry.getValue match {
        case o: ConfigObject =>
          system -> parseSpec(o.toConfig)
        case unknown =>
          throw new IllegalArgumentException()
      }
    }.toMap
  } else Map.empty

  def parseSpec(config: Config): ProbeSpec = {
    val objectType = config.getString("object-type")
    val policy = parsePolicy(config.getConfig("policy"))
    val metadata = Map.empty[String,String]
    val children = if (config.hasPath("children")) config.getConfig("children").entrySet().map { case entry =>
        val name = entry.getKey
        entry.getValue match {
          case o: ConfigObject =>
            name -> parseSpec(o.toConfig)
          case unknown =>
            throw new IllegalArgumentException()
        }
    }.toMap else Map.empty[String,ProbeSpec]
    //ProbeSpec(objectType, policy, metadata, children, static = true)
    ProbeSpec(objectType, metadata, children)
  }

  def parsePolicy(config: Config): ProbePolicy = {
    val joiningTimeout: Duration = if (config.hasPath("joining-timeout")) {
      FiniteDuration(config.getDuration("joining-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    } else staticJoiningTimeout
    val probeTimeout: Duration = if (config.hasPath("probe-timeout")) {
      FiniteDuration(config.getDuration("probe-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    } else staticProbeTimeout
    val leavingTimeout: Duration = if (config.hasPath("leaving-timeout")) {
      FiniteDuration(config.getDuration("leaving-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    } else staticLeavingTimeout
    val flapWindow: Duration = if (config.hasPath("flap-window")) {
      FiniteDuration(config.getDuration("flap-window", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    } else staticFlapWindow
    val flapDeviations = if (config.hasPath("flap-deviations")) config.getInt("flap-deviations") else staticFlapDeviations
    val notificationPolicyType = if (config.hasPath("notification-policy")) { config.getString("notification-policy") match {
      case "emit" => NotificationPolicyTypeEmit
      case "escalate" => NotificationPolicyTypeEscalate
      case "squelch" => NotificationPolicyTypeSquelch
      case "unknown" => throw new IllegalArgumentException()
    }} else staticNotificationPolicyType
    ProbePolicy(joiningTimeout, probeTimeout, leavingTimeout, flapWindow, flapDeviations, notificationPolicyType, inherits = false)
  }
}

object StaticRegistry {
  def apply(staticRegistry: File, registrySettings: RegistrySettings): StaticRegistry = {
    val config = ConfigFactory.load(staticRegistry.getAbsolutePath)
    new StaticRegistry(config, registrySettings)
  }
}
