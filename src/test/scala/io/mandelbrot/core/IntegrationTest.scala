package io.mandelbrot.core

import org.scalatest.{BeforeAndAfterAll, Suite}
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.slf4j.LoggerFactory

abstract class IntegrationTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with Suite with BeforeAndAfterAll {

  //LoggerFactory.getLogger("IntegrationTest").debug("%s config: %s".format(getClass.getName, _system.settings.config.root().toString))

  def this(actorSystemName: String, config: Config) = this(ActorSystem(actorSystemName, config))
  def this(actorSystemName: String, composableConfig: ComposableConfig) = this(ActorSystem(actorSystemName, composableConfig.config))
  def this(actorSystemName: String, mergedConfig: MergedConfig) = this(ActorSystem(actorSystemName, mergedConfig.config))
  def this(actorSystemName: String) = this(ActorSystem(actorSystemName, (MandelbrotConfig ++ AkkaConfig ++ SprayConfig).config))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }
}
