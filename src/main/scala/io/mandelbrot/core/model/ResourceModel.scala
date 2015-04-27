package io.mandelbrot.core.model

import java.net.{URI, IDN}

sealed trait ResourceModel

trait Hierarchical[T <: Resource] {

  val segments: Vector[String]
  def parentOption: Option[T]

  def hasParent: Boolean = segments.length > 1
  def isParentOf(that: T): Boolean = segments.equals(that.segments.take(segments.length))
  def isDirectParentOf(that: T): Boolean = segments.equals(that.segments.init)
  def isChildOf(that: T): Boolean = segments.take(that.segments.length).equals(that.segments)
  def isDirectChildOf(that: T): Boolean = segments.init.equals(that.segments)
  def compare(that: T): Int = toString.compareTo(that.toString)

  override def toString = segments.map(IDN.toASCII(_, IDN.USE_STD3_ASCII_RULES)).mkString(".")

  override def hashCode() = toString.hashCode
}
/**
 * A resource locator
 */
class Resource(val segments: Vector[String]) extends ResourceModel {
  if (segments.length == 0) throw new IllegalArgumentException("empty resource is not allowed")
  segments.foreach {
    case segment =>
      if (segment.length == 0) throw new IllegalArgumentException("empty segment is not allowed")
  }
}

object Resource {
  def stringToSegments(string: String): Vector[String] = {
    if (string.length > 255) throw new IllegalArgumentException("resource length exceeds 255 characters")
    string.split('.').map { segment =>
      if (segment.length > 63) throw new IllegalArgumentException("resource segment exceeds 63 characters")
      IDN.toUnicode(segment, IDN.USE_STD3_ASCII_RULES)
    }.toVector
  }
}

/**
 *
 */
class AgentId(segments: Vector[String]) extends Resource(segments) with Hierarchical[AgentId] with Ordered[AgentId] {
  def parentOption: Option[AgentId] = if (hasParent) Some(new AgentId(segments.init)) else None
  override def equals(other: Any): Boolean = other match {
    case otherAgent: AgentId => segments.equals(otherAgent.segments)
    case _ => false
  }
}
object AgentId {
  def apply(string: String): AgentId = new AgentId(Resource.stringToSegments(string))
  def unapply(agentId: AgentId): Option[Vector[String]] = Some(agentId.segments)
}

/**
 *
 */
class CheckId(segments: Vector[String]) extends Resource(segments) with Hierarchical[CheckId] with Ordered[CheckId] {
  def parentOption: Option[CheckId] = if (hasParent) Some(new CheckId(segments.init)) else None
  override def equals(other: Any): Boolean = other match {
    case otherCheck: CheckId => segments.equals(otherCheck.segments)
    case _ => false
  }
}
object CheckId {
  def apply(string: String): CheckId = new CheckId(Resource.stringToSegments(string))
  def unapply(checkId: CheckId): Option[Vector[String]] = Some(checkId.segments)
}

/**
 *
 */
class ProbeRef(val agentId: AgentId, val checkId: CheckId) extends Ordered[ProbeRef] with ResourceModel {

  def parentOption: Option[ProbeRef] = checkId.parentOption.map(parent => new ProbeRef(agentId, parent))
  def hasParent: Boolean = checkId.hasParent
  def isParentOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && checkId.isParentOf(that.checkId)
  def isDirectParentOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && checkId.isDirectParentOf(that.checkId)
  def isChildOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && checkId.isChildOf(that.checkId)
  def isDirectChildOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && checkId.isDirectChildOf(that.checkId)
  def compare(that: ProbeRef): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = agentId.toString + ":" + checkId.toString
  override def equals(other: Any): Boolean = other match {
    case otherRef: ProbeRef => agentId.equals(otherRef.agentId) && checkId.equals(otherRef.checkId)
    case _ => false
  }
}

object ProbeRef {
  def apply(agentId: AgentId, checkId: CheckId): ProbeRef = new ProbeRef(agentId, checkId)
  def apply(agentId: String, checkId: String): ProbeRef = new ProbeRef(AgentId(agentId), CheckId(checkId))
  def apply(string: String): ProbeRef = {
    val index = string.indexOf(':')
    if (index == -1) throw new IllegalArgumentException()
    val (agentId,checkId) = string.splitAt(index)
    ProbeRef(AgentId(agentId), CheckId(checkId.tail))
  }
  //def apply(uri: URI): ProbeRef = apply(uri, Vector.empty)
  def unapply(probeRef: ProbeRef): Option[(AgentId,CheckId)] = Some((probeRef.agentId, probeRef.checkId))
}

/**
 * A MetricSource uniquely identifies a metric within a ProbeSystem.
 */
class MetricSource(val checkId: CheckId, val metricName: String) extends Ordered[MetricSource] with ResourceModel {

  def compare(that: MetricSource): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = checkId.toString + ":" + metricName
  override def equals(other: Any): Boolean = other match {
    case other: MetricSource => checkId.equals(other.checkId) && metricName.equals(other.metricName)
    case _ => false
  }
}

object MetricSource {
  def apply(checkId: CheckId, metricName: String): MetricSource = new MetricSource(checkId, metricName)

  def apply(string: String): MetricSource = {
    val index = string.indexOf(':')
    if (index == -1) throw new IllegalArgumentException()
    val (checkId,metricName) = string.splitAt(index)
    new MetricSource(CheckId(checkId), metricName.tail)
  }

  def unapply(source: MetricSource): Option[(CheckId, String)] = Some((source.checkId, source.metricName))
}

