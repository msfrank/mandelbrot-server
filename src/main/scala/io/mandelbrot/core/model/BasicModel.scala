package io.mandelbrot.core.model

import java.net.URI

sealed trait BasicModel

/**
 *
 */
class ProbeRef(val uri: URI, val path: Vector[String]) extends Ordered[ProbeRef] with BasicModel {

  def parentOption: Option[ProbeRef] = if (path.isEmpty) None else Some(new ProbeRef(uri, path.init))

  def hasParent: Boolean = path.nonEmpty

  def isRoot: Boolean = path.isEmpty

  def isParentOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.equals(that.path.take(path.length))

  def isDirectParentOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.equals(that.path.init)

  def isChildOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.take(that.path.length).equals(that.path)

  def isDirectChildOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.init.equals(that.path)

  def compare(that: ProbeRef): Int = toString.compareTo(that.toString)

  override def equals(other: Any): Boolean = other match {
    case otherRef: ProbeRef => uri.equals(otherRef.uri) && path.equals(otherRef.path)
    case _ => false
  }

  override def hashCode() = toString.hashCode

  override def toString = uri.toString + "/" + path.mkString("/")
}

object ProbeRef {

  def apply(uri: URI, path: Vector[String]): ProbeRef = new ProbeRef(uri, path)

  def apply(string: String): ProbeRef = {
    val index = string.indexOf('/')
    if (index == -1) new ProbeRef(new URI(string), Vector.empty) else {
      val (uri,path) = string.splitAt(index)
      if (path.length == 1) new ProbeRef(new URI(uri), Vector.empty) else new ProbeRef(new URI(uri), path.tail.split('/').toVector)
    }
  }

  def apply(uri: URI): ProbeRef = apply(uri, Vector.empty)

  def apply(uri: URI, path: String): ProbeRef = {
    val segments = path.split('/').filter(_ != "").toVector
    new ProbeRef(uri, segments)
  }

  def apply(uri: String, path: String): ProbeRef = apply(new URI(uri), path)

  def unapply(probeRef: ProbeRef): Option[(URI,Vector[String])] = Some((probeRef.uri, probeRef.path))
}

/* object lifecycle */
sealed trait ProbeLifecycle extends BasicModel
case object ProbeInitializing extends ProbeLifecycle { override def toString = "initializing" }
case object ProbeJoining extends ProbeLifecycle { override def toString = "joining" }
case object ProbeKnown extends ProbeLifecycle   { override def toString = "known" }
case object ProbeSynthetic extends ProbeLifecycle { override def toString = "synthetic" }
case object ProbeRetired extends ProbeLifecycle { override def toString = "retired" }

/* object state */
sealed trait ProbeHealth extends BasicModel
case object ProbeHealthy extends ProbeHealth  { override def toString = "healthy" }
case object ProbeDegraded extends ProbeHealth { override def toString = "degraded" }
case object ProbeFailed extends ProbeHealth   { override def toString = "failed" }
case object ProbeUnknown extends ProbeHealth  { override def toString = "unknown" }

/**
 * A MetricSource uniquely identifies a metric within a ProbeSystem.
 */
class MetricSource(val probePath: Vector[String], val metricName: String) extends Ordered[MetricSource] with BasicModel {

  def compare(that: MetricSource): Int = toString.compareTo(that.toString)

  override def equals(other: Any): Boolean = other match {
    case other: MetricSource => probePath.equals(other.probePath) && metricName.equals(other.metricName)
    case _ => false
  }

  override def hashCode() = toString.hashCode

  override def toString = if (probePath.isEmpty) metricName else probePath.mkString("/", "/", ":") + metricName
}

object MetricSource {
  def apply(probePath: Vector[String], metricName: String): MetricSource = new MetricSource(probePath, metricName)

  def apply(string: String): MetricSource = {
    val index = string.indexOf(':')
    if (index == -1) new MetricSource(Vector.empty, string) else {
      if (string.head != '/')
        throw new IllegalArgumentException()
      val (path,name) = string.splitAt(index)
      new MetricSource(path.tail.split('/').toVector, name.tail)
    }
  }

  def unapply(source: MetricSource): Option[(Vector[String], String)] = Some((source.probePath, source.metricName))
}

/* */
sealed trait SourceType extends BasicModel
case object GaugeSource extends SourceType    { override def toString = "gauge" }
case object CounterSource extends SourceType  { override def toString = "counter" }

/* */
sealed trait MetricUnit extends BasicModel {
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