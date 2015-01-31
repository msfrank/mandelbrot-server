package io.mandelbrot.persistence.cassandra

import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import io.mandelbrot.core.ComposableConfig

object CassandraConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    s"""
       |mandelbrot {
       |  persistence {
       |    cassandra {
       |      seed-nodes = [ localhost ]
       |      replication-factor = 1
       |      keyspace-name = scalatest_${DateTime.now().toString("YMD_Hms_S")}
       |    }
       |  }
       |}
    """.stripMargin)
}
