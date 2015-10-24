package io.mandelbrot.persistence.cassandra

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

import io.mandelbrot.core.registry.RegistryManagerSpec
import io.mandelbrot.core.{AkkaConfig, MandelbrotConfig}

class CassandraRegistryManagerSpec(_system: ActorSystem) extends RegistryManagerSpec(_system) with BeforeAndAfterEach {
  def this() = this(ActorSystem("CassandraRegistryManagerSpec", CassandraRegistryManagerSpec.config))
  override def afterEach(): Unit = {
    Cassandra(system).dropKeyspace()
    super.afterEach()
  }
}

object CassandraRegistryManagerSpec {
  val config = AkkaConfig ++ MandelbrotConfig ++ CassandraConfig + ConfigFactory.parseString(
    """
      |mandelbrot {
      |  registry {
      |    plugin = "io.mandelbrot.persistence.cassandra.CassandraRegistryExtension"
      |    plugin-settings { }
      |  }
      |}
    """.stripMargin)
}
