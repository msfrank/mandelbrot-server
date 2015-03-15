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

package io.mandelbrot.core.entity

import java.util.concurrent.TimeUnit

import akka.actor.Props
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration

import io.mandelbrot.core.ServerConfigException

case class CoordinatorSettings(plugin: String, settings: Option[Any])

class ClusterSettings(val enabled: Boolean,
                      val seedNodes: Vector[String],
                      val minNrMembers: Int,
                      val totalShards: Int,
                      val deliveryAttempts: Int,
                      val clusterRole: Option[String],
                      val maxHandOverRetries: Int,
                      val maxTakeOverRetries: Int,
                      val retryInterval: FiniteDuration,
                      val props: Props)

object ClusterSettings {
  def parse(config: Config): ClusterSettings = {
    val enabled = config.getBoolean("enabled")
    val seedNodes = config.getStringList("seed-nodes")
    val minNrMembers = config.getInt("min-nr-members")
    val totalShards = config.getInt("total-shards")
    val deliveryAttempts = config.getInt("delivery-attempts")
    val clusterRole: Option[String] = if (config.hasPath("cluster-role")) Some(config.getString("cluster-role")) else None
    val maxHandOverRetries = config.getInt("balancer-handover-retries")
    val maxTakeOverRetries = config.getInt("balancer-takeover-retries")
    val units = TimeUnit.MILLISECONDS
    val retryInterval = FiniteDuration(config.getDuration("balancer-retry-interval", units), units)
    val plugin = config.getString("plugin")
    val pluginSettings = if (config.hasPath("plugin-settings")) config.getConfig("plugin-settings") else ConfigFactory.empty()
    val props = EntityCoordinator.extensions.get(plugin) match {
      case None =>
        throw new ServerConfigException("%s is not recognized as an EntityCoordinatorExtension".format(plugin))
      case Some(extension) =>
        extension.props(extension.configure(pluginSettings))
    }
    new ClusterSettings(enabled,
                        seedNodes.toVector,
                        minNrMembers,
                        totalShards,
                        deliveryAttempts,
                        clusterRole,
                        maxHandOverRetries,
                        maxTakeOverRetries,
                        retryInterval,
                        props)
  }
}
