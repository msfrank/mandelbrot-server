package io.mandelbrot.persistence.cassandra

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

import io.mandelbrot.core.state.StateManagerSpec
import io.mandelbrot.core.{AkkaConfig, MandelbrotConfig}

class CassandraStateManagerSpec(_system: ActorSystem) extends StateManagerSpec(_system) with BeforeAndAfterEach {
  def this() = this(ActorSystem("CassandraStateManagerSpec", CassandraStateManagerSpec.config))
  override def afterEach(): Unit = {
    Cassandra(system).dropKeyspace()
    super.afterEach()
  }
}

object CassandraStateManagerSpec {
  val config = AkkaConfig ++ MandelbrotConfig ++ CassandraConfig + ConfigFactory.parseString(
    """
      |mandelbrot {
      |  state {
      |    plugin = "io.mandelbrot.persistence.cassandra.CassandraStateExtension"
      |    plugin-settings { }
      |  }
      |}
    """.stripMargin)
}
