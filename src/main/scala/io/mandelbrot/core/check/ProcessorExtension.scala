package io.mandelbrot.core.check

import java.util.ServiceLoader
import akka.actor.{ActorRef, Props}
import org.slf4j.LoggerFactory
import spray.json.JsObject
import scala.collection.JavaConversions._

import io.mandelbrot.core.model.CheckRef
import io.mandelbrot.core.timeseries.Tick

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
case class LsnTickAlarmState(lsn: Long, tick: Tick, alarmState: Option[Boolean])

//
///**
// *
// */
//trait ProcessorFactory {
//  def implement(): BehaviorProcessor
//  def observes(): Set[ProbeId]
//}
//
///**
// *
// */
//abstract class CheckBehaviorExtension {
//  type Settings
//  trait DependentProcessorFactory extends ProcessorFactory {
//    val settings: Settings
//    def implement(): BehaviorProcessor
//    def observes(): Set[ProbeId]
//  }
//  def configure(properties: Map[String,String]): DependentProcessorFactory
//}
//
//object CheckBehavior {
//  val logger = LoggerFactory.getLogger(CheckBehavior.getClass)
//  val extensions = ServiceLoader.load(classOf[CheckBehaviorExtension]).map { p =>
//    val clazz = p.getClass
//    logger.info("loaded CheckBehaviorExtension %s".format(clazz.getCanonicalName))
//    (clazz.getCanonicalName, p)
//  }.toMap
//}
