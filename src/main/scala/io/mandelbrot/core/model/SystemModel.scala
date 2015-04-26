package io.mandelbrot.core.model

import java.util.regex.Pattern

sealed trait SystemModel

/* key-value pairs describing a probe */
case class ProbeMetadata(probeRef: ProbeRef, metadata: Map[String,String]) extends SystemModel

/**
 * Matches a ProbeRef by individually comparing the uri scheme,  uri location,
 * and path components according to the specified matchers.
 */
case class ProbeMatcher(scheme: Option[SegmentMatcher],
                        location: Option[SegmentMatcher],
                        path: Option[PathMatcher]) extends SystemModel {
  def matches(probeRef: ProbeRef): Boolean = {
    // FIXME!
//    for (matcher <- scheme if !matcher.matches(probeRef.uri.getScheme))
//      return false
//    for (matcher <- location if !matcher.matches(probeRef.uri.getRawSchemeSpecificPart))
//      return false
//    for (matcher <- path if !matcher.matches(probeRef.path))
//      return false
    true
  }
  override def toString = if (scheme.isEmpty && location.isEmpty && path.isEmpty) "*" else {
    val schemestring = if (scheme.isDefined) scheme.get.toString else "*"
    val locationstring = if (location.isDefined) location.get.toString else "*"
    if (path.isEmpty) schemestring + ":" + locationstring else {
      schemestring + ":" + locationstring + "/" + path.get.toString
    }
  }
}

/**
 * Special case object for unconditional matching
 */
object MatchesAll extends ProbeMatcher(None, None, None) {
  override def matches(probeRef: ProbeRef): Boolean = true
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

case class PathMatcher(segments: Vector[SegmentMatcher]) {
  def matches(candidate: Vector[String]): Boolean = {
    for (i <- 0.until(segments.length)) {
      if (!candidate.isDefinedAt(i))
        return false
      val segment = segments(i)
      if (!segment.matches(candidate(i)))
        return false
    }
    true
  }
  override def toString = segments.mkString("/")
}