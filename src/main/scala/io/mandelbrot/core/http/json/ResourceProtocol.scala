package io.mandelbrot.core.http.json

import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait ResourceProtocol extends DefaultJsonProtocol with StandardProtocol {

  /* convert AgentId class */
  implicit object AgentIdFormat extends JsonFormat[AgentId] {
    def write(agentId: AgentId) = JsString(agentId.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => AgentId(string)
      case _ => throw new DeserializationException("expected AgentId")
    }
  }

  /* convert ProbeId class */
  implicit object ProbeIdFormat extends JsonFormat[ProbeId] {
    def write(probeId: ProbeId) = JsString(probeId.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => ProbeId(string)
      case _ => throw new DeserializationException("expected ProbeId")
    }
  }

  /* convert CheckId class */
  implicit object CheckIdFormat extends JsonFormat[CheckId] {
    def write(checkId: CheckId) = JsString(checkId.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => CheckId(string)
      case _ => throw new DeserializationException("expected CheckId")
    }
  }

  /* convert ProbeRef class */
  implicit object ProbeRefFormat extends JsonFormat[ProbeRef] {
    def write(ref: ProbeRef) = JsString(ref.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => ProbeRef(string)
      case _ => throw new DeserializationException("expected ProbeRef")
    }
  }

  /* convert CheckRef class */
  implicit object CheckRefFormat extends JsonFormat[CheckRef] {
    def write(ref: CheckRef) = JsString(ref.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => CheckRef(string)
      case _ => throw new DeserializationException("expected CheckRef")
    }
  }

  /* convert MetricSource class */
  implicit object MetricSourceFormat extends JsonFormat[MetricSource] {
    def write(source: MetricSource) = JsString(source.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => MetricSource(string)
      case _ => throw new DeserializationException("expected MetricSource")
    }
  }
}

