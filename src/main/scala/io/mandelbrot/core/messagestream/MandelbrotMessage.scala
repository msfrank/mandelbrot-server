package io.mandelbrot.core.messagestream

import spray.json._
import org.joda.time.DateTime

import io.mandelbrot.core.registry.{ProbeRef, ProbeState}
import io.mandelbrot.core.http.JsonProtocol._

/**
 *
 */
sealed trait MessagePayload
case class StateMessagePayload(probeRef: ProbeRef, state: ProbeState, timestamp: DateTime) extends MessagePayload

object MessagePayload extends DefaultJsonProtocol {
  //implicit val StateMessagePayloadFormat = jsonFormat3(StateMessagePayload)
}

/**
 *
 */
case class MandelbrotMessage(topic: Array[String], payload: JsValue)
