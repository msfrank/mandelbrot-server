package io.mandelbrot.core.message

import spray.json._
import org.joda.time.DateTime

import io.mandelbrot.core.registry.{ProbeRef, ProbeHealth}

/**
 *
 */
sealed trait Message
case class GenericMessage(messageType: String, value: JsValue) extends Message

sealed trait MandelbrotMessage extends Message { val source: ProbeRef }
case class StatusMessage(source: ProbeRef, health: ProbeHealth, summary: String, detail: Option[String], timestamp: DateTime) extends MandelbrotMessage
