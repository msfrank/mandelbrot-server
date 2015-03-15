package io.mandelbrot.core.state

import akka.actor.Props
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.ServiceLoader

abstract class StatePersisterExtension {
  type Settings
  def configure(config: Config): Settings
  def props(settings: Settings): Props
}

object StatePersister {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.state.StatePersister")
  val extensions = ServiceLoader.load(classOf[StatePersisterExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded StatePersisterExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
