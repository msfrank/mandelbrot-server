package io.mandelbrot.core.registry

import akka.actor.Props
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.ServiceLoader

abstract class RegistryExtension {
  type Settings
  def configure(config: Config): Settings
  def props(settings: Settings): Props
}

object RegistryExtension {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.registry.RegistryExtension")
  val extensions = ServiceLoader.load(classOf[RegistryExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded RegistryExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
