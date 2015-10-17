package io.mandelbrot.core.ingest

import akka.actor.Props
import com.typesafe.config.{ConfigFactory, Config}

import io.mandelbrot.core.ServerConfigException

case class IngestSettings(numPartitions: Int, props: Props)

object IngestSettings {
  def parse(config: Config): IngestSettings = {
    val numPartitions = config.getInt("num-partitions")
    val plugin = config.getString("plugin")
    val pluginSettings = if (config.hasPath("plugin-settings")) config.getConfig("plugin-settings") else ConfigFactory.empty()
    val props = StatePersister.extensions.get(plugin) match {
      case None =>
        throw new ServerConfigException("%s is not recognized as an IngestExtension".format(plugin))
      case Some(extension) =>
        extension.props(extension.configure(pluginSettings))
    }
    IngestSettings(numPartitions, props)
  }
}
