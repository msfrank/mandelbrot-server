/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core

import akka.actor._
import com.typesafe.config._
import com.typesafe.config.ConfigException.WrongType
import io.mandelbrot.core.entity.ClusterSettings
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import java.io.File

import io.mandelbrot.core.registry.RegistrySettings
import io.mandelbrot.core.state.StateSettings
import io.mandelbrot.core.notification.NotificationSettings
import io.mandelbrot.core.history.HistorySettings
import io.mandelbrot.core.http.HttpSettings
import java.util.concurrent.TimeUnit
import io.mandelbrot.core.tracking.TrackingSettings

/**
 *
 */
case class ServerConfigSettings(registry: RegistrySettings,
                                state: StateSettings,
                                notification: NotificationSettings,
                                history: HistorySettings,
                                tracking: TrackingSettings,
                                cluster: ClusterSettings,
                                http: HttpSettings,
                                shutdownTimeout: FiniteDuration)

/**
 *
 */
class ServerConfigExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[ServerConfigExtension])

  /* build the settings tree */
  val settings = try {
    val mandelbrotConfig = system.settings.config.getConfig("mandelbrot")

    /* parse registry settings */
    val registrySettings = RegistrySettings.parse(mandelbrotConfig.getConfig("registry"))

    /* parse state settings */
    val stateSettings = StateSettings.parse(mandelbrotConfig.getConfig("state"))

    /* parse notification settings */
    val notificationSettings = NotificationSettings.parse(mandelbrotConfig.getConfig("notification"))

    /* parse history settings */
    val historySettings = HistorySettings.parse(mandelbrotConfig.getConfig("history"))

    /* parse tracking settings */
    val trackingSettings = TrackingSettings.parse(mandelbrotConfig.getConfig("tracking"))

    /* parse tracking settings */
    val clusterSettings = ClusterSettings.parse(mandelbrotConfig.getConfig("cluster"))

    /* parse http settings */
    val httpSettings = HttpSettings.parse(mandelbrotConfig.getConfig("http"))

    /* */
    val shutdownTimeout = FiniteDuration(mandelbrotConfig.getDuration("shutdown-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    ServerConfigSettings(registrySettings, stateSettings, notificationSettings, historySettings,
      trackingSettings, clusterSettings, httpSettings, shutdownTimeout)

  } catch {
    case ex: ServerConfigException =>
      throw ex
    case ex: ConfigException =>
      throw new ServerConfigException(ex)
    case ex: Throwable =>
      throw new ServerConfigException("unexpected exception while parsing configuration", ex)
  }
}

object ServerConfig extends ExtensionId[ServerConfigExtension] with ExtensionIdProvider {

  private val log = LoggerFactory.getLogger("io.mandelbrot.core.ServerConfig")

  override def lookup() = ServerConfig
  override def createExtension(system: ExtendedActorSystem) = new ServerConfigExtension(system)

  /* retrieve the ServerConfigSettings from the actor system */
  def settings(implicit system: ActorSystem): ServerConfigSettings = super.get(system).settings

  /**
   * load the mandelbrot config file from the location specified by the mandelbrot.config.file
   * config value in baseConfig.  this config value can be a string (e.g. from system property
   * -Dmandelbrot.config.file) or a string list.
   */
  def loadConfigFile(baseConfig: Config): Config = {
    val possibleConfigFiles = try {
      if (!baseConfig.hasPath("mandelbrot.config.file"))
        throw new ServerConfigException("mandelbrot.config.file is not specified")
      baseConfig.getStringList("mandelbrot.config.file").map(new File(_))
    } catch {
      case ex: WrongType =>
        Seq(new File(baseConfig.getString("mandelbrot.config.file")))
    }
    for (file <- possibleConfigFiles) {
      if (file.canRead) {
        log.debug("found config file {}", file.getAbsolutePath)
        return ConfigFactory.parseFile(file)
      }
    }
    throw new ServerConfigException("failed to find a readable config file")
  }

  /* build the runtime configuration from mandelbrot.conf, with fallbacks to the base config */
  def load() = try {
    val baseConfig = ConfigFactory.load()
    val mandelbrotConfig = loadConfigFile(baseConfig)
    ConfigFactory.defaultOverrides.withFallback(mandelbrotConfig.withFallback(baseConfig))
  } catch {
    case ex: ServerConfigException =>
      throw ex
    case ex: ConfigException =>
      throw new ServerConfigException(ex)
    case ex: Throwable =>
      throw new ServerConfigException("unexpected exception while parsing configuration", ex)
  }
}

/**
 *
 */
class ServerConfigException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: ConfigException) = this("failed to parse config: " + cause.getMessage, cause)
  def this(cause: Throwable) = this(null, cause)
}
