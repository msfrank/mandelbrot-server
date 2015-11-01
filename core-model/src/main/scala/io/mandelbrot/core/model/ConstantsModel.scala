package io.mandelbrot.core.model

sealed trait ConstantsModel {
  val name: String
  override def toString = name
}

/* check lifecycle */
sealed trait CheckLifecycle extends ConstantsModel
case object CheckInitializing extends CheckLifecycle  { val name = "initializing" }
case object CheckJoining extends CheckLifecycle       { val name = "joining" }
case object CheckKnown extends CheckLifecycle         { val name = "known" }
case object CheckSynthetic extends CheckLifecycle     { val name = "synthetic" }
case object CheckRetired extends CheckLifecycle       { val name = "retired" }

/* check health */
sealed trait CheckHealth extends ConstantsModel
case object CheckHealthy extends CheckHealth  { val name = "healthy" }
case object CheckDegraded extends CheckHealth { val name = "degraded" }
case object CheckFailed extends CheckHealth   { val name = "failed" }
case object CheckUnknown extends CheckHealth  { val name = "unknown" }


/* metric source type */
sealed trait SourceType extends ConstantsModel
case object GaugeSource extends SourceType    { val name = "gauge" }
case object CounterSource extends SourceType  { val name = "counter" }

/* */
sealed trait MetricUnit extends ConstantsModel

/* no unit specified */
case object Units extends MetricUnit   { val name = "units" }
case object Operations extends MetricUnit     { val name = "operations" }
case object Percent extends MetricUnit { val name = "percent" }

/* time units */
case object Years extends MetricUnit    { val name = "years" }
case object Months extends MetricUnit   { val name = "months" }
case object Weeks extends MetricUnit    { val name = "weeks" }
case object Days extends MetricUnit     { val name = "days" }
case object Hours extends MetricUnit    { val name = "hours" }
case object Minutes extends MetricUnit  { val name = "minutes" }
case object Seconds extends MetricUnit  { val name = "seconds" }
case object Millis extends MetricUnit   { val name = "milliseconds" }
case object Micros extends MetricUnit   { val name = "microseconds" }

/* size units */
case object Bytes extends MetricUnit      { val name = "bytes" }
case object KiloBytes extends MetricUnit  { val name = "kilobytes" }
case object MegaBytes extends MetricUnit  { val name = "megabytes" }
case object GigaBytes extends MetricUnit  { val name = "gigabytes" }
case object TeraBytes extends MetricUnit  { val name = "terabytes" }
case object PetaBytes extends MetricUnit  { val name = "petabytes" }

/* sampling rates */
sealed trait SamplingRate extends ConstantsModel

case object PerSecond extends SamplingRate      { val name = "1second" }
case object PerMinute extends SamplingRate      { val name = "1minute" }
case object PerFiveMinutes extends SamplingRate { val name = "5minutes" }
case object PerHour extends SamplingRate        { val name = "1hour" }
case object PerDay extends SamplingRate         { val name = "1day" }
