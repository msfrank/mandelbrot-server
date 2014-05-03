package io.mandelbrot.core.history

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.io.File
import java.util.concurrent.TimeUnit

class HistorySettings(val databasePath: File,
                      val inMemory: Boolean,
                      val h2databaseToUpper: Boolean,
                      val historyRetention: Duration
                      )

object HistorySettings {
  def parse(config: Config): HistorySettings = {
    val databasePath = new File(config.getString("database-path"))
    val inMemory = config.getBoolean("in-memory")
    val h2databaseToUpper = config.getBoolean("h2-database-to-upper")
    val historyRetention = FiniteDuration(config.getDuration("history-retention", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    new HistorySettings(databasePath, inMemory, h2databaseToUpper, historyRetention)
  }
}
