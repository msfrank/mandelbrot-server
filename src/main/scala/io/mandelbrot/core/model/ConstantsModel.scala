package io.mandelbrot.core.model

sealed trait ConstantsModel

/* check lifecycle */
sealed trait CheckLifecycle extends ConstantsModel
case object CheckInitializing extends CheckLifecycle { override def toString = "initializing" }
case object CheckJoining extends CheckLifecycle { override def toString = "joining" }
case object CheckKnown extends CheckLifecycle   { override def toString = "known" }
case object CheckSynthetic extends CheckLifecycle { override def toString = "synthetic" }
case object CheckRetired extends CheckLifecycle { override def toString = "retired" }

/* check health */
sealed trait CheckHealth extends ConstantsModel
case object CheckHealthy extends CheckHealth  { override def toString = "healthy" }
case object CheckDegraded extends CheckHealth { override def toString = "degraded" }
case object CheckFailed extends CheckHealth   { override def toString = "failed" }
case object CheckUnknown extends CheckHealth  { override def toString = "unknown" }


/* metric source type */
sealed trait SourceType extends ConstantsModel
case object GaugeSource extends SourceType    { override def toString = "gauge" }
case object CounterSource extends SourceType  { override def toString = "counter" }

/* */
sealed trait MetricUnit extends ConstantsModel {
  def name: String
}

/* no unit specified */
case object Units extends MetricUnit   { val name = "units" }
case object Ops extends MetricUnit     { val name = "operations" }
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
