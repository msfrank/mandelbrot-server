package io.mandelbrot.persistence.cassandra

import com.typesafe.config.Config
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.ConsistencyLevel

case class TableSettings(consistencyLevel: ConsistencyLevel,
                         serialConsistencyLevel: ConsistencyLevel,
                         fetchSize: Int,
                         tracePercentage: Double)

object TableSettings {

  def parse(config: Config): TableSettings = {
    val consistencyLevel = ConsistencyLevel.valueOf(config.getString("consistency-level"))
    val serialConsistencyLevel = ConsistencyLevel.valueOf(config.getString("serial-consistency-level"))
    val fetchSize = config.getInt("fetch-size")
    val tracePercentage = config.getDouble("trace-percentage")
    TableSettings(consistencyLevel, serialConsistencyLevel, fetchSize, tracePercentage)
  }
}
