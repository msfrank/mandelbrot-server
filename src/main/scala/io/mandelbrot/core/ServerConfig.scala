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
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.io.File

import com.typesafe.config.ConfigException.WrongType
import io.mandelbrot.core.http.HttpSettings
import io.mandelbrot.core.messagestream.MessageStreamSettings

/**
 *
 */
case class ServerConfigSettings(messageStream: Option[MessageStreamSettings], http: Option[HttpSettings])

/**
 *
 */
class ServerConfigExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[ServerConfigExtension])

  val config = system.settings.config.getConfig("mandelbrot")

  /* build the settings tree */
  val settings = try {

    /* parse message-stream settings */
    val messageStreamSettings = if (!config.hasPath("message-stream")) None else {
      Some(MessageStreamSettings.parse(config.getConfig("message-stream")))
    }

    /* parse http settings */
    val httpSettings = if (!config.hasPath("http")) None else {
      Some(HttpSettings.parse(config.getConfig("http")))
    }

    ServerConfigSettings(messageStreamSettings, httpSettings)

  } catch {
    case ex: ServerConfigException =>
      throw ex
    case ex: ConfigException =>
      throw ServerConfigException(ex)
    case ex: Throwable =>
      throw ServerConfigException("unexpected exception while parsing configuration", ex)
  }
}

object ServerConfig extends ExtensionId[ServerConfigExtension] with ExtensionIdProvider {
  override def lookup() = ServerConfig
  override def createExtension(system: ExtendedActorSystem) = new ServerConfigExtension(system)

  /* retrieve the ServerConfigSettings from the actor system */
  def settings(implicit system: ActorSystem): ServerConfigSettings = super.get(system).settings

  /* build the runtime configuration */
  val config = try {
    val baseConfig = ConfigFactory.load()
    val mandelbrotConfig = loadConfigFile(baseConfig)
    ConfigFactory.defaultOverrides.withFallback(mandelbrotConfig.withFallback(baseConfig))
  } catch {
    case ex: ServerConfigException =>
      throw ex
    case ex: ConfigException =>
      throw ServerConfigException(ex)
    case ex: Throwable =>
      throw ServerConfigException("unexpected exception while parsing configuration", ex)
  }

  /**
   * load the mandelbrot config file from the location specified by the mandelbrot.config.file config
   * value in baseConfig.  this config value can be a string (e.g. from system property -Dmandelbrot.config.file)
   * or a string list.
   */
  def loadConfigFile(baseConfig: Config): Config = {
    val possibleConfigFiles = try {
      if (!baseConfig.hasPath("mandelbrot.config.file"))
        throw ServerConfigException("mandelbrot.config.file is not specified")
      baseConfig.getStringList("mandelbrot.config.file").map(new File(_))
    } catch {
      case ex: WrongType =>
        Seq(new File(baseConfig.getString("mandelbrot.config.file")))
    }
    var config: Option[Config] = None
    for (file <- possibleConfigFiles if config.isEmpty) {
      if (file.canRead)
        config = Some(ConfigFactory.parseFile(file))
    }
    config.getOrElse(throw ServerConfigException("failed to find a readable config file"))
  }
}

/**
 *
 */
case class ServerConfigException(message: String, cause: Throwable) extends Exception(message, cause)

object ServerConfigException {
  def apply(message: String): ServerConfigException = new ServerConfigException(message, null)
  def apply(cause: ConfigException): ServerConfigException = ServerConfigException("failed to parse config: " + cause.getMessage, cause)
}
