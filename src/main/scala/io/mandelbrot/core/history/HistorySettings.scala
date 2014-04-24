package io.mandelbrot.core.history

import com.typesafe.config.Config
import java.io.File

class HistorySettings(val databasePath: File,
                      val inMemory: Boolean,
                      val h2databaseToUpper: Boolean
                      )

object HistorySettings {
  def parse(config: Config): HistorySettings = {
    val databasePath = new File(config.getString("database-path"))
    val inMemory = config.getBoolean("in-memory")
    val h2databaseToUpper = config.getBoolean("h2-database-to-upper")
    new HistorySettings(databasePath, inMemory, h2databaseToUpper)
  }
}
