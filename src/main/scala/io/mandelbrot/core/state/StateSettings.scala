package io.mandelbrot.core.state

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit
import java.io.File
import io.mandelbrot.core.ServiceSettings

case class StateSettings(plugin: String,
                         service: Option[Any],
                         maxSummarySize: Long,
                         maxDetailSize: Long,
                         statusHistoryAge: Duration,
                         defaultSearchLimit: Int)

object StateSettings extends ServiceSettings {
  def parse(config: Config): StateSettings = {
    val maxSummarySize = config.getBytes("max-summary-size")
    val maxDetailSize = config.getBytes("max-detail-size")
    val statusHistoryAge = FiniteDuration(config.getDuration("status-history-age", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val defaultSearchLimit = config.getInt("default-search-limit")
    val plugin = config.getString("plugin")
    val service = if (config.hasPath("plugin-settings")) {
      makeServiceSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new StateSettings(plugin, service, maxSummarySize, maxDetailSize, statusHistoryAge, defaultSearchLimit)
  }
}