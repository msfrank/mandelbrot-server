package io.mandelbrot.core.state

import akka.actor.Props
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.ServiceLoader

abstract class StateExtension {
  type Settings
  def configure(config: Config): Settings
  def props(settings: Settings): Props
}

object StateExtension {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.state.StateExtension")
  val extensions = ServiceLoader.load(classOf[StateExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded StateExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
