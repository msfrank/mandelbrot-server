package io.mandelbrot.core.cluster

import com.typesafe.config.Config
import io.mandelbrot.core.{ServerConfigException, ServiceExtension}
import scala.collection.JavaConversions._

case class CoordinatorSettings(plugin: String, settings: Option[Any])

class ClusterSettings(val enabled: Boolean,
                      val seedNodes: Vector[String],
                      val minNrMembers: Int,
                      val shardCount: Int,
                      val coordinator: CoordinatorSettings)

object ClusterSettings {
  def parse(config: Config): ClusterSettings = {
    val enabled = config.getBoolean("enabled")
    val seedNodes = config.getStringList("seed-nodes")
    val minNrMembers = config.getInt("min-nr-members")
    val shardCount = config.getInt("shard-count")
    val plugin = config.getString("plugin")
    if (!ServiceExtension.pluginImplements(plugin, classOf[Coordinator]))
      throw new ServerConfigException("%s is not recognized as an Coordinator plugin".format(plugin))
    val service = if (config.hasPath("plugin-settings")) {
      ServiceExtension.makePluginSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new ClusterSettings(enabled, seedNodes.toVector, minNrMembers, shardCount, CoordinatorSettings(plugin, service))
  }
}
