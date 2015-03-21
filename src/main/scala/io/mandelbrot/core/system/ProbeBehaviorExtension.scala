package io.mandelbrot.core.system

import java.util.ServiceLoader
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

trait ProcessorFactory {
  def implement(): BehaviorProcessor
}

abstract class ProbeBehaviorExtension {
  type Settings
  trait DependentProcessorFactory extends ProcessorFactory {
    val settings: Settings
    def implement(): BehaviorProcessor
  }
  def configure(properties: Map[String,String]): DependentProcessorFactory
}

object ProbeBehavior {
  val logger = LoggerFactory.getLogger(ProbeBehavior.getClass)
  val extensions = ServiceLoader.load(classOf[ProbeBehaviorExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded ProbeBehaviorExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
