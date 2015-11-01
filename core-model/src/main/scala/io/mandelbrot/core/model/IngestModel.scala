package io.mandelbrot.core.model

import org.joda.time.DateTime

object IngestModel

sealed trait Observation {
  val probeId: ProbeId
  val timestamp: DateTime
  val dimensions: Map[String,String]
}

/* */
final case class DataPoint(value: Long, unit: MetricUnit, samplingRate: Option[SamplingRate] = None)

/* */
final case class ScalarMapObservation(probeId: ProbeId,
                                      timestamp: DateTime,
                                      dimensions: Map[String,String],
                                      scalarMap: Map[String,DataPoint]) extends Observation

/* */
final case class VectorObservation(probeId: ProbeId,
                                   timestamp: DateTime,
                                   dimensions: Map[String,String],
                                   metricName: String,
                                   vector: Vector[DataPoint]) extends Observation

/* the set of metrics emitted by a check */
final case class ProbeObservation(generation: Long, observation: Observation)

/* a page of check metrics entries */
final case class ProbeObservationPage(history: Vector[ProbeObservation], last: Option[String], exhausted: Boolean)

