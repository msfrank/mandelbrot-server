package io.mandelbrot.core.messagestream

import spray.json._
import org.joda.time.DateTime

import io.mandelbrot.core.registry.{ProbeRef, ProbeHealth}

/**
 *
 */
sealed trait Message
sealed trait MandelbrotMessage extends Message { val source: ProbeRef }
case class StateMessage(source: ProbeRef, state: ProbeHealth, summary: String, detail: Option[String], timestamp: DateTime) extends MandelbrotMessage
case class GenericMessage(messageType: String, value: JsValue) extends Message

