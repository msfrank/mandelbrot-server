package io.mandelbrot.core.cluster

import com.typesafe.config.Config
import scala.collection.JavaConversions._

class ClusterSettings(val enabled: Boolean,
                      val seedNodes: Vector[String],
                      val minNrMembers: Int,
                      val initialShardCount: Int)

object ClusterSettings {
  def parse(config: Config): ClusterSettings = {
    val enabled = config.getBoolean("enabled")
    val seedNodes = config.getStringList("seed-nodes")
    val minNrMembers = config.getInt("min-nr-members")
    val initialShardCount = config.getInt("initial-shard-count")
    new ClusterSettings(enabled, seedNodes.toVector, minNrMembers, initialShardCount)
  }
}
