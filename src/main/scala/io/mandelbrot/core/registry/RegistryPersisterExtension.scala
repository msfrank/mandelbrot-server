package io.mandelbrot.core.registry

import akka.actor.Props
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.ServiceLoader

abstract class RegistryPersisterExtension {
  type Settings
  def configure(config: Config): Settings
  def props(settings: Settings): Props
}

object RegistryPersister {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.registry.RegistryPersister")
  val extensions = ServiceLoader.load(classOf[RegistryPersisterExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded RegistryPersisterExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
