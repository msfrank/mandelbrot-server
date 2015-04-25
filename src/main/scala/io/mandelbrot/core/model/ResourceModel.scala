package io.mandelbrot.core.model

import java.net.{URI, IDN}

sealed trait ResourceModel

/**
 * A resource locator
 */
class Resource(val segments: Vector[String]) extends Ordered[Resource] with ResourceModel {

  def parentOption: Option[Resource] = if (segments.isEmpty) None else Some(new Resource(segments.init))
  def hasParent: Boolean = segments.nonEmpty
  def isRoot: Boolean = segments.isEmpty
  def isParentOf(that: Resource): Boolean = segments.equals(that.segments.take(segments.length))
  def isDirectParentOf(that: Resource): Boolean = segments.equals(that.segments.init)
  def isChildOf(that: Resource): Boolean = segments.take(that.segments.length).equals(that.segments)
  def isDirectChildOf(that: Resource): Boolean = segments.init.equals(that.segments)
  def compare(that: Resource): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = segments.map(IDN.toASCII(_, IDN.USE_STD3_ASCII_RULES)).mkString(".")
  override def equals(other: Any): Boolean = other match {
    case otherRef: Resource => segments.equals(otherRef.segments)
    case _ => false
  }
}

object Resource {
  def apply(string: String): Resource = {
    val segments = IDN.toUnicode(string, IDN.USE_STD3_ASCII_RULES).split('.').toVector
    new Resource(segments)
  }
  def unapply(resource: Resource): Option[Vector[String]] = Some(resource.segments)
}

/**
 *
 */
class ProbeRef(val uri: URI, val path: Vector[String]) extends Ordered[ProbeRef] with ResourceModel {

  def parentOption: Option[ProbeRef] = if (path.isEmpty) None else Some(new ProbeRef(uri, path.init))
  def hasParent: Boolean = path.nonEmpty
  def isRoot: Boolean = path.isEmpty
  def isParentOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.equals(that.path.take(path.length))
  def isDirectParentOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.equals(that.path.init)
  def isChildOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.take(that.path.length).equals(that.path)
  def isDirectChildOf(that: ProbeRef): Boolean = uri.equals(that.uri) && path.init.equals(that.path)
  def compare(that: ProbeRef): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = uri.toString + "/" + path.mkString("/")
  override def equals(other: Any): Boolean = other match {
    case otherRef: ProbeRef => uri.equals(otherRef.uri) && path.equals(otherRef.path)
    case _ => false
  }
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

/**
 * A MetricSource uniquely identifies a metric within a ProbeSystem.
 */
class MetricSource(val probePath: Vector[String], val metricName: String) extends Ordered[MetricSource] with ResourceModel {

  def compare(that: MetricSource): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = if (probePath.isEmpty) metricName else probePath.mkString("/", "/", ":") + metricName
  override def equals(other: Any): Boolean = other match {
    case other: MetricSource => probePath.equals(other.probePath) && metricName.equals(other.metricName)
    case _ => false
  }
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

