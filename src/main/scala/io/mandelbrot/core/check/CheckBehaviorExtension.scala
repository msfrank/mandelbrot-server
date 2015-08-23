package io.mandelbrot.core.check

import java.util.ServiceLoader
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

import io.mandelbrot.core.model.{CheckId, ProbeId}

/**
 *
 */
trait ProcessorFactory {
  def implement(): BehaviorProcessor
  def observes(): Set[ProbeId]
}

/**
 *
 */
abstract class CheckBehaviorExtension {
  type Settings
  trait DependentProcessorFactory extends ProcessorFactory {
    val settings: Settings
    def implement(): BehaviorProcessor
    def observes(): Set[ProbeId]
  }
  def configure(properties: Map[String,String]): DependentProcessorFactory
}

object CheckBehavior {
  val logger = LoggerFactory.getLogger(CheckBehavior.getClass)
  val extensions = ServiceLoader.load(classOf[CheckBehaviorExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded CheckBehaviorExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
