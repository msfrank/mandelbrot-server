package io.mandelbrot.core.entity

import akka.actor.Props
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.ServiceLoader

abstract class EntityCoordinatorExtension {
  type Settings
  def configure(config: Config): Settings
  def props(settings: Settings): Props
}

object EntityCoordinator {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.entity.EntityCoordinator")
  val extensions = ServiceLoader.load(classOf[EntityCoordinatorExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded EntityCoordinatorExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
