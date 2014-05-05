package io.mandelbrot.core.notification

import com.typesafe.config.Config

import io.mandelbrot.core.ServiceSettings

class NotificationSettings(val plugin: String, val service: Option[Any])

object NotificationSettings extends ServiceSettings {
  def parse(config: Config): NotificationSettings = {
    val plugin = config.getString("plugin")
    val service = if (config.hasPath("plugin-settings")) {
      makeServiceSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new NotificationSettings(plugin, service)
  }
}
