package io.mandelbrot.core.history

import com.typesafe.config.Config

class HistorySettings()

object HistorySettings {
  def parse(config: Config): HistorySettings = {
    new HistorySettings()
  }
}
