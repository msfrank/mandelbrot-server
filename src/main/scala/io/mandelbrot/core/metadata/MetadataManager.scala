package io.mandelbrot.core.metadata

import akka.actor.{Props, ActorLogging, Actor}
import io.mandelbrot.core.registry.ProbeRef

class MetadataManager extends Actor with ActorLogging {

  def receive = {

    case ProbeMetadata(probeRef, metaData) =>
      log.debug("received metadata for {}: {}", probeRef, metaData)

  }
}

object MetadataManager {
  def props() = Props(classOf[MetadataManager])
}

case class ProbeMetadata(probeRef: ProbeRef, metaData: Map[String,String])
