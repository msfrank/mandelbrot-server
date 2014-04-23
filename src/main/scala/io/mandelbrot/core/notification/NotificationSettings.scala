package io.mandelbrot.core.notification

import com.typesafe.config.Config

class NotificationSettings()

object NotificationSettings {
  def parse(config: Config): NotificationSettings = {
    new NotificationSettings()
  }
}
