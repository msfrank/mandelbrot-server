package io.mandelbrot.core.history

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.io.File
import java.util.concurrent.TimeUnit
import io.mandelbrot.core.ServiceSettings

case class HistorySettings(plugin: String,
                           service: Option[Any],
                           historyRetention: Duration
                           )

object HistorySettings extends ServiceSettings {
  def parse(config: Config): HistorySettings = {
    val historyRetention = FiniteDuration(config.getDuration("history-retention", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val plugin = config.getString("plugin")
    val service = if (config.hasPath("plugin-settings")) {
      makeServiceSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new HistorySettings(plugin, service, historyRetention)
  }
}
