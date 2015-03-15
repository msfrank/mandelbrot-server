package io.mandelbrot.core.registry

import akka.actor._
import com.typesafe.config.Config

class TestRegistryPersister(settings: TestRegistryPersisterSettings) extends Actor with ActorLogging {

  def receive = {
    case message => log.info("received {}", message)
  }
}

object TestRegistryPersister {
  def props(settings: TestRegistryPersisterSettings) = Props(classOf[TestRegistryPersister], settings)
}

case class TestRegistryPersisterSettings()

class TestRegistryPersisterExtension extends RegistryPersisterExtension {
  type Settings = TestRegistryPersisterSettings
  def configure(config: Config): Settings = TestRegistryPersisterSettings()
  def props(settings: Settings): Props = TestRegistryPersister.props(settings)
}

