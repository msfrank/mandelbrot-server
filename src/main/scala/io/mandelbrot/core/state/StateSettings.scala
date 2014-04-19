package io.mandelbrot.core.state

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit

class StateSettings(val maxSummarySize: Long,
                    val maxDetailSize: Long,
                    val statusHistoryAge: Duration,
                    val defaultSearchLimit: Int)

object StateSettings {
  def parse(config: Config): StateSettings = {
    val maxSummarySize = config.getBytes("max-summary-size")
    val maxDetailSize = config.getBytes("max-detail-size")
    val statusHistoryAge = FiniteDuration(config.getDuration("status-history-age", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val defaultSearchLimit = config.getInt("default-search-limit")
    new StateSettings(maxSummarySize, maxDetailSize, statusHistoryAge, defaultSearchLimit)
  }
}