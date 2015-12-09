package io.mandelbrot.core.check

import java.util.ServiceLoader
import akka.actor.{ActorRef, Props}
import org.slf4j.LoggerFactory
import spray.json.JsObject
import scala.collection.JavaConversions._

import io.mandelbrot.core.model.{Timestamp, CheckHealth, CheckRef}

/**
 *
 */
abstract class ProcessorExtension {
  type Settings
  def configure(json: Option[JsObject]): Settings
  def props(settings: Settings): Props
}

/**
  *
  */
object ProcessorExtension {
  val logger = LoggerFactory.getLogger("io.mandelbrot.core.check.ProcessorExtension")
  val extensions = ServiceLoader.load(classOf[ProcessorExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded ProcessorExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}

case class ChangeProcessor(lsn: Long, services: ActorRef, children: Set[CheckRef])
case class ProcessorStatus(lsn: Long, timestamp: Timestamp, health: CheckHealth, summary: Option[String])
