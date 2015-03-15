package io.mandelbrot.core.state

import akka.actor._
import com.typesafe.config.Config

class TestStatePersister(settings: TestStatePersisterSettings) extends Actor with ActorLogging {

  def receive = {
    case message => log.info("received {}", message)
  }
}

object TestStatePersister {
  def props(settings: TestStatePersisterSettings) = Props(classOf[TestStatePersister], settings)
}

case class TestStatePersisterSettings()

class TestStatePersisterExtension extends StatePersisterExtension {
  type Settings = TestStatePersisterSettings
  def configure(config: Config): Settings = TestStatePersisterSettings()
  def props(settings: Settings): Props = TestStatePersister.props(settings)
}

