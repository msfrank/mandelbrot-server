package io.mandelbrot.persistence.cassandra

import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import io.mandelbrot.core.ComposableConfig

import scala.util.Random

object CassandraConfig extends ComposableConfig {
  def config = ConfigFactory.parseString(
    s"""
       |mandelbrot {
       |  persistence {
       |    cassandra {
       |      seed-nodes = [ localhost ]
       |      replication-factor = 1
       |      keyspace-name = scalatest_${DateTime.now().toString("YMDHms")}_${Math.abs(Random.nextLong().toShort)}
       |    }
       |  }
       |}
    """.stripMargin)
}
