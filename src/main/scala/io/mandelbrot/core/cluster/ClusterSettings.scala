package io.mandelbrot.core.cluster

import com.typesafe.config.Config
import scala.collection.JavaConversions._

class ClusterSettings(val enabled: Boolean, val seedNodes: Vector[String])

object ClusterSettings {
  def parse(config: Config): ClusterSettings = {
    val enabled = config.getBoolean("enabled")
    val seedNodes = config.getStringList("seed-nodes")
    new ClusterSettings(enabled, seedNodes.toVector)
  }
}
