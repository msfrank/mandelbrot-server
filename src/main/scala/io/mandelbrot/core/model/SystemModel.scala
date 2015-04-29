package io.mandelbrot.core.model

import java.util.regex.Pattern

sealed trait SystemModel

/* key-value pairs describing a probe */
case class ProbeMetadata(probeRef: ProbeRef, metadata: Map[String,String]) extends SystemModel

sealed trait CheckMatcher {
  def matches(checkId: CheckId): Boolean
}

/**
 * Special case object for unconditional matching
 */
object MatchesAll extends CheckMatcher {
  def matches(checkId: CheckId): Boolean = true
  override def toString = "*"
}

sealed trait SegmentMatcher {
  def matches(candidate: String): Boolean
}

case object MatchAny extends SegmentMatcher {
  def matches(candidate: String) = true
  override def toString = "*"
}

case class MatchExact(string: String) extends SegmentMatcher {
  def matches(candidate: String) = candidate == string
  override def toString = string
}

case class MatchPrefix(prefix: String) extends SegmentMatcher {
  def matches(candidate: String) = candidate.startsWith(prefix)
  override def toString = prefix + "*"
}

case class MatchSuffix(suffix: String) extends SegmentMatcher {
  def matches(candidate: String) = candidate.endsWith(suffix)
  override def toString = "*" + suffix
}

case class MatchGlob(tokens: Vector[String]) extends SegmentMatcher {
  def escape(literal: String): String = """\Q""" + literal + """\E"""
  val regex = Pattern.compile(tokens.map {
    case "*" => ".*"
    case "?" => ".?"
    case token => escape(token)
  }.mkString)
  def matches(candidate: String) = regex.matcher(candidate).matches()
  override def toString = tokens.mkString
}

case class PathMatcher(segments: Vector[SegmentMatcher]) extends CheckMatcher {
  def matches(checkId: CheckId): Boolean = {
    for (i <- 0.until(segments.length)) {
      if (!checkId.segments.isDefinedAt(i))
        return false
      val segment = segments(i)
      if (!segment.matches(checkId.segments(i)))
        return false
    }
    true
  }
  override def toString = segments.mkString(".")
}