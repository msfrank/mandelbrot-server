package io.mandelbrot.core.http.json

import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait ConstantsProtocol extends DefaultJsonProtocol with StandardProtocol {

  /* convert CheckHealth class */
  implicit object CheckHealthFormat extends RootJsonFormat[CheckHealth] {
    def write(health: CheckHealth) = health match {
      case CheckHealthy => JsString("healthy")
      case CheckDegraded => JsString("degraded")
      case CheckFailed => JsString("failed")
      case CheckUnknown => JsString("unknown")
      case unknown => throw new SerializationException("unknown CheckHealth state " + unknown.getClass)
    }
    def read(value: JsValue) = value match {
      case JsString("healthy") => CheckHealthy
      case JsString("degraded") => CheckDegraded
      case JsString("failed") => CheckFailed
      case JsString("unknown") => CheckUnknown
      case unknown => throw new DeserializationException("unknown CheckHealth state " + unknown)
    }
  }

  /* convert CheckLifecycle class */
  implicit object CheckLifecycleFormat extends RootJsonFormat[CheckLifecycle] {
    def write(lifecycle: CheckLifecycle) = lifecycle match {
      case CheckInitializing => JsString("initializing")
      case CheckJoining => JsString("joining")
      case CheckKnown => JsString("known")
      case CheckSynthetic => JsString("synthetic")
      case CheckRetired => JsString("retired")
      case unknown => throw new SerializationException("unknown CheckLifecycle state " + unknown.getClass)
    }
    def read(value: JsValue) = value match {
      case JsString("initializing") => CheckInitializing
      case JsString("joining") => CheckJoining
      case JsString("known") => CheckKnown
      case JsString("synthetic") => CheckSynthetic
      case JsString("retired") => CheckRetired
      case unknown => throw new DeserializationException("unknown CheckLifecycle state " + unknown)
    }
  }

  /* convert SourceType class */
  implicit object SourceTypeFormat extends RootJsonFormat[SourceType] {
    def write(sourceType: SourceType) = JsString(sourceType.toString)
    def read(value: JsValue) = value match {
      case JsString("gauge") => GaugeSource
      case JsString("counter") => CounterSource
      case _ => throw new DeserializationException("expected SourceType")
    }
  }

  /* convert MetricUnit class */
  implicit object MetricUnitFormat extends RootJsonFormat[MetricUnit] {
    def write(unit: MetricUnit) = JsString(unit.name)
    def read(value: JsValue) = value match {
      case JsString("units") => Units
      case JsString("operations") => Ops
      case JsString("percent") => Percent
      case JsString("years") => Years
      case JsString("months") => Months
      case JsString("weeks") => Weeks
      case JsString("days") => Days
      case JsString("hours") => Hours
      case JsString("minutes") => Minutes
      case JsString("seconds") => Seconds
      case JsString("milliseconds") => Millis
      case JsString("microseconds") => Micros
      case JsString("bytes") => Bytes
      case JsString("kilobytes") => KiloBytes
      case JsString("megabytes") => MegaBytes
      case JsString("gigabytes") => GigaBytes
      case JsString("terabytes") => TeraBytes
      case JsString("petabytes") => PetaBytes
      case _ => throw new DeserializationException("expected MetricUnit")
    }
  }
}
