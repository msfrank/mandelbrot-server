package io.mandelbrot.core.http.json

import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait ConstantsProtocol extends DefaultJsonProtocol with StandardProtocol {

  /* convert CheckHealth class */
  implicit object CheckHealthFormat extends JsonFormat[CheckHealth] {
    def write(health: CheckHealth) = JsString(health.name)
    def read(value: JsValue) = value match {
      case JsString(CheckHealthy.name) => CheckHealthy
      case JsString(CheckDegraded.name) => CheckDegraded
      case JsString(CheckFailed.name) => CheckFailed
      case JsString(CheckUnknown.name) => CheckUnknown
      case unknown => throw new DeserializationException("expected CheckHealth")
    }
  }

  /* convert CheckLifecycle class */
  implicit object CheckLifecycleFormat extends JsonFormat[CheckLifecycle] {
    def write(lifecycle: CheckLifecycle) = JsString(lifecycle.name)
    def read(value: JsValue) = value match {
      case JsString(CheckInitializing.name) => CheckInitializing
      case JsString(CheckJoining.name) => CheckJoining
      case JsString(CheckKnown.name) => CheckKnown
      case JsString(CheckSynthetic.name) => CheckSynthetic
      case JsString(CheckRetired.name) => CheckRetired
      case unknown => throw new DeserializationException("expected CheckLifecycle")
    }
  }

  /* convert SourceType class */
  implicit object SourceTypeFormat extends JsonFormat[SourceType] {
    def write(sourceType: SourceType) = JsString(sourceType.name)
    def read(value: JsValue) = value match {
      case JsString(GaugeSource.name) => GaugeSource
      case JsString(CounterSource.name) => CounterSource
      case _ => throw new DeserializationException("expected SourceType")
    }
  }

  /* convert MetricUnit class */
  implicit object MetricUnitFormat extends JsonFormat[MetricUnit] {
    def write(unit: MetricUnit) = JsString(unit.name)
    def read(value: JsValue) = value match {
      case JsString(Units.name) => Units
      case JsString(Ops.name) => Ops
      case JsString(Percent.name) => Percent
      case JsString(Years.name) => Years
      case JsString(Months.name) => Months
      case JsString(Weeks.name) => Weeks
      case JsString(Days.name) => Days
      case JsString(Hours.name) => Hours
      case JsString(Minutes.name) => Minutes
      case JsString(Seconds.name) => Seconds
      case JsString(Millis.name) => Millis
      case JsString(Micros.name) => Micros
      case JsString(Bytes.name) => Bytes
      case JsString(KiloBytes.name) => KiloBytes
      case JsString(MegaBytes.name) => MegaBytes
      case JsString(GigaBytes.name) => GigaBytes
      case JsString(TeraBytes.name) => TeraBytes
      case JsString(PetaBytes.name) => PetaBytes
      case _ => throw new DeserializationException("expected MetricUnit")
    }
  }

  /* convert MetricUnit class */
  implicit object SamplingRateFormat extends JsonFormat[SamplingRate] {
    def write(samplingRate: SamplingRate) = JsString(samplingRate.name)
    def read(value: JsValue) = value match {
      case JsString(PerSecond.name) => PerSecond
      case JsString(PerMinute.name) => PerMinute
      case JsString(PerFiveMinutes.name) => PerFiveMinutes
      case JsString(PerHour.name) => PerHour
      case JsString(PerDay.name) => PerDay
      case _ => throw new DeserializationException("expected SamplingRate")
    }
  }
}
