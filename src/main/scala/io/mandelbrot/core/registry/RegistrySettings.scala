package io.mandelbrot.core.registry

import com.typesafe.config.Config

class RegistrySettings()

object RegistrySettings {
  def parse(config: Config): RegistrySettings = {
    new RegistrySettings()
  }
}
