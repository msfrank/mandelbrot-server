package io.mandelbrot.core.model

import java.net.IDN

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
  if (segments.isEmpty) throw new IllegalArgumentException("empty resource is not allowed")
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
 * Agent identifier
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
 * Check Identifier
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
 * Probe Identifier
 */
class ProbeId(segments: Vector[String]) extends Resource(segments) with Hierarchical[ProbeId] with Ordered[ProbeId] {
  def parentOption: Option[ProbeId] = if (hasParent) Some(new ProbeId(segments.init)) else None
  override def equals(other: Any): Boolean = other match {
    case otherProbe: ProbeId => segments.equals(otherProbe.segments)
    case _ => false
  }
}

object ProbeId {
  def apply(string: String): ProbeId = new ProbeId(Resource.stringToSegments(string))
  def unapply(checkId: ProbeId): Option[Vector[String]] = Some(checkId.segments)
}

/**
 *
 */
class CheckRef(val agentId: AgentId, val checkId: CheckId) extends Ordered[CheckRef] with ResourceModel {

  def parentOption: Option[CheckRef] = checkId.parentOption.map(parent => new CheckRef(agentId, parent))
  def hasParent: Boolean = checkId.hasParent
  def isParentOf(that: CheckRef): Boolean = agentId.equals(that.agentId) && checkId.isParentOf(that.checkId)
  def isDirectParentOf(that: CheckRef): Boolean = agentId.equals(that.agentId) && checkId.isDirectParentOf(that.checkId)
  def isChildOf(that: CheckRef): Boolean = agentId.equals(that.agentId) && checkId.isChildOf(that.checkId)
  def isDirectChildOf(that: CheckRef): Boolean = agentId.equals(that.agentId) && checkId.isDirectChildOf(that.checkId)
  def compare(that: CheckRef): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = agentId.toString + ":" + checkId.toString
  override def equals(other: Any): Boolean = other match {
    case otherRef: CheckRef => agentId.equals(otherRef.agentId) && checkId.equals(otherRef.checkId)
    case _ => false
  }
}

object CheckRef {
  def apply(agentId: AgentId, checkId: CheckId): CheckRef = new CheckRef(agentId, checkId)
  def apply(agentId: String, checkId: String): CheckRef = new CheckRef(AgentId(agentId), CheckId(checkId))
  def apply(string: String): CheckRef = {
    val index = string.indexOf(':')
    if (index == -1) throw new IllegalArgumentException()
    val (agentId,checkId) = string.splitAt(index)
    CheckRef(AgentId(agentId), CheckId(checkId.tail))
  }
  //def apply(uri: URI): CheckRef = apply(uri, Vector.empty)
  def unapply(checkRef: CheckRef): Option[(AgentId,CheckId)] = Some((checkRef.agentId, checkRef.checkId))
}

/**
 *
 */
class ProbeRef(val agentId: AgentId, val probeId: ProbeId) extends Ordered[ProbeRef] with ResourceModel {

  def parentOption: Option[ProbeRef] = probeId.parentOption.map(parent => new ProbeRef(agentId, parent))
  def hasParent: Boolean = probeId.hasParent
  def isParentOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && probeId.isParentOf(that.probeId)
  def isDirectParentOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && probeId.isDirectParentOf(that.probeId)
  def isChildOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && probeId.isChildOf(that.probeId)
  def isDirectChildOf(that: ProbeRef): Boolean = agentId.equals(that.agentId) && probeId.isDirectChildOf(that.probeId)
  def compare(that: ProbeRef): Int = toString.compareTo(that.toString)

  override def hashCode() = toString.hashCode
  override def toString = agentId.toString + ":" + probeId.toString
  override def equals(other: Any): Boolean = other match {
    case otherRef: ProbeRef => agentId.equals(otherRef.agentId) && probeId.equals(otherRef.probeId)
    case _ => false
  }
}

object ProbeRef {
  def apply(agentId: AgentId, probeId: ProbeId): ProbeRef = new ProbeRef(agentId, probeId)
  def apply(agentId: String, probeId: String): ProbeRef = new ProbeRef(AgentId(agentId), ProbeId(probeId))
  def apply(string: String): ProbeRef = {
    val index = string.indexOf(':')
    if (index == -1) throw new IllegalArgumentException()
    val (agentId,probeId) = string.splitAt(index)
    ProbeRef(AgentId(agentId), ProbeId(probeId.tail))
  }
  //def apply(uri: URI): ProbeRef = apply(uri, Vector.empty)
  def unapply(probeRef: ProbeRef): Option[(AgentId,ProbeId)] = Some((probeRef.agentId, probeRef.probeId))
}

