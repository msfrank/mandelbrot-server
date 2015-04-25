package io.mandelbrot.core.model

sealed trait ConstantsModel

/* probe lifecycle */
sealed trait ProbeLifecycle extends ConstantsModel
case object ProbeInitializing extends ProbeLifecycle { override def toString = "initializing" }
case object ProbeJoining extends ProbeLifecycle { override def toString = "joining" }
case object ProbeKnown extends ProbeLifecycle   { override def toString = "known" }
case object ProbeSynthetic extends ProbeLifecycle { override def toString = "synthetic" }
case object ProbeRetired extends ProbeLifecycle { override def toString = "retired" }

/* probe health */
sealed trait ProbeHealth extends ConstantsModel
case object ProbeHealthy extends ProbeHealth  { override def toString = "healthy" }
case object ProbeDegraded extends ProbeHealth { override def toString = "degraded" }
case object ProbeFailed extends ProbeHealth   { override def toString = "failed" }
case object ProbeUnknown extends ProbeHealth  { override def toString = "unknown" }


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