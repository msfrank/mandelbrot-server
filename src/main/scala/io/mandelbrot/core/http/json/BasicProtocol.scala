package io.mandelbrot.core.http.json

import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait BasicProtocol extends DefaultJsonProtocol with StandardProtocol {

  /* convert ProbeRef class */
  implicit object ProbeRefFormat extends RootJsonFormat[ProbeRef] {
    def write(ref: ProbeRef) = JsString(ref.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => ProbeRef(string)
      case _ => throw new DeserializationException("expected ProbeRef")
    }
  }

  /* convert ProbeHealth class */
  implicit object ProbeHealthFormat extends RootJsonFormat[ProbeHealth] {
    def write(health: ProbeHealth) = health match {
      case ProbeHealthy => JsString("healthy")
      case ProbeDegraded => JsString("degraded")
      case ProbeFailed => JsString("failed")
      case ProbeUnknown => JsString("unknown")
      case unknown => throw new SerializationException("unknown ProbeHealth state " + unknown.getClass)
    }
    def read(value: JsValue) = value match {
      case JsString("healthy") => ProbeHealthy
      case JsString("degraded") => ProbeDegraded
      case JsString("failed") => ProbeFailed
      case JsString("unknown") => ProbeUnknown
      case unknown => throw new DeserializationException("unknown ProbeHealth state " + unknown)
    }
  }

  /* convert ProbeLifecycle class */
  implicit object ProbeLifecycleFormat extends RootJsonFormat[ProbeLifecycle] {
    def write(lifecycle: ProbeLifecycle) = lifecycle match {
      case ProbeInitializing => JsString("initializing")
      case ProbeJoining => JsString("joining")
      case ProbeKnown => JsString("known")
      case ProbeSynthetic => JsString("synthetic")
      case ProbeRetired => JsString("retired")
      case unknown => throw new SerializationException("unknown ProbeLifecycle state " + unknown.getClass)
    }
    def read(value: JsValue) = value match {
      case JsString("initializing") => ProbeInitializing
      case JsString("joining") => ProbeJoining
      case JsString("known") => ProbeKnown
      case JsString("synthetic") => ProbeSynthetic
      case JsString("retired") => ProbeRetired
      case unknown => throw new DeserializationException("unknown ProbeLifecycle state " + unknown)
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
