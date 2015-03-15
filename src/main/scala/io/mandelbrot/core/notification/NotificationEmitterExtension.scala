package io.mandelbrot.core.notification

import akka.actor.Props
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.ServiceLoader

abstract class NotificationEmitterExtension {
  type Settings
  def configure(config: Config): Settings
  def props(settings: Settings): Props
}

object NotificationEmitter {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.notification.NotificationEmitter")
  val extensions = ServiceLoader.load(classOf[NotificationEmitterExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded NotificationEmitterExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
