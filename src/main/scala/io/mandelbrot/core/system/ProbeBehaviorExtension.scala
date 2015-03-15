package io.mandelbrot.core.system

import java.util.ServiceLoader
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

abstract class ProbeBehaviorExtension {
  def implement(properties: Map[String,String]): BehaviorProcessor
}

object ProbeBehavior {
  val logger = LoggerFactory.getLogger(ProbeBehavior.getClass)
  val extensions = ServiceLoader.load(classOf[ProbeBehaviorExtension]).map { p =>
    val clazz = p.getClass
    logger.info("loaded ProbeBehaviorExtension %s".format(clazz.getCanonicalName))
    (clazz.getCanonicalName, p)
  }.toMap
}
