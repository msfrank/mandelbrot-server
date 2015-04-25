package io.mandelbrot.core.http.json

import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait ResourceProtocol extends DefaultJsonProtocol with StandardProtocol {

  /* convert Resource class */
  implicit object ResourceFormat extends RootJsonFormat[Resource] {
    def write(resource: Resource) = JsString(resource.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => Resource(string)
      case _ => throw new DeserializationException("expected Resource")
    }
  }

  /* convert ProbeRef class */
  implicit object ProbeRefFormat extends RootJsonFormat[ProbeRef] {
    def write(ref: ProbeRef) = JsString(ref.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => ProbeRef(string)
      case _ => throw new DeserializationException("expected ProbeRef")
    }
  }

  /* convert MetricSource class */
  implicit object MetricSourceFormat extends RootJsonFormat[MetricSource] {
    def write(source: MetricSource) = JsString(source.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => MetricSource(string)
      case _ => throw new DeserializationException("expected ProbeRef")
    }
  }
}

