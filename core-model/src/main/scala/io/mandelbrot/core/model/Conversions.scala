package io.mandelbrot.core.model

object Conversions {
  import language.implicitConversions

  /* */
  implicit class Long2MetricUnits(val long: Long) extends AnyVal {
    def metricUnits = DataPoint(long, Units)
  }
  implicit def long2MetricUnits(long: Long): Long2MetricUnits = new Long2MetricUnits(long)
  implicit def intMetricUnits(int: Int): Long2MetricUnits = new Long2MetricUnits(int.toLong)

  /* */
  implicit class Long2MetricOperations(val long: Long) extends AnyVal {
    def metricOperations = DataPoint(long, Operations)
  }
  implicit def long2MetricOperations(long: Long): Long2MetricOperations = new Long2MetricOperations(long)
  implicit def int2MetricOperations(int: Int): Long2MetricOperations = new Long2MetricOperations(int.toLong)

  /* */
  implicit class Long2MetricPercent(val long: Long) extends AnyVal {
    def metricPercent = DataPoint(long, Percent)
  }
  implicit def long2MetricPercent(long: Long): Long2MetricPercent = new Long2MetricPercent(long)
  implicit def int2MetricPercent(int: Int): Long2MetricPercent = new Long2MetricPercent(int.toLong)

  /* */
  implicit class Long2MetricBytes(val long: Long) extends AnyVal {
    def metricBytes = DataPoint(long, Bytes)
  }
  implicit def long2MetricBytes(long: Long): Long2MetricBytes = new Long2MetricBytes(long)
  implicit def int2MetricBytes(int: Int): Long2MetricBytes = new Long2MetricBytes(int.toLong)
}
