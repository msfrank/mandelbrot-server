package io.mandelbrot.core.registry

import java.util.regex.Pattern
import scala.util.parsing.combinator.RegexParsers
import org.slf4j.LoggerFactory

/**
 * Matches a ProbeRef by individually comparing the uri scheme,  uri location,
 * and path components according to the specified matchers.
 */
case class ProbeMatcher(scheme: Option[SegmentMatcher], location: Option[SegmentMatcher], path: Option[PathMatcher]) {
  def matches(probeRef: ProbeRef): Boolean = {
    for (matcher <- scheme if !matcher.matches(probeRef.uri.getScheme))
      return false
    for (matcher <- location if !matcher.matches(probeRef.uri.getRawSchemeSpecificPart))
      return false
    for (matcher <- path if !matcher.matches(probeRef.path))
      return false
    true
  }
}

/**
 * Special case object for unconditional matching
 */
object MatchesAll extends ProbeMatcher(None, None, None) {
  override def matches(probeRef: ProbeRef): Boolean = true
}

sealed trait SegmentMatcher {
  def matches(candidate: String): Boolean
}

case object MatchAny extends SegmentMatcher {
  def matches(candidate: String) = true
}

case class MatchExact(string: String) extends SegmentMatcher {
  def matches(candidate: String) = candidate == string
}

case class MatchPrefix(prefix: String) extends SegmentMatcher {
  def matches(candidate: String) = candidate.startsWith(prefix)
}

case class MatchSuffix(suffix: String) extends SegmentMatcher {
  def matches(candidate: String) = candidate.endsWith(suffix)
}

case class MatchRegex(regex: Pattern) extends SegmentMatcher {
  def matches(candidate: String) = regex.matcher(candidate).matches()
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
}

/**
 *
 */
class ProbeMatcherParser extends RegexParsers {

  val logger = LoggerFactory.getLogger(classOf[ProbeMatcherParser])

  /* shamelessly copied from Parsers.scala */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    logger.debug(name + " --> " + r)
    r
  }

  override val skipWhitespace = false

  def escape(literal: String): String = """\Q""" + literal + """\E"""

  def parseGlob(glob: String): SegmentMatcher = {
    val tokens: Vector[String] = glob.foldLeft(Vector.empty[String]) {
      case (t, ch) if t.isEmpty => Vector(ch.toString)
      case (t, ch) =>
        val curr = t.last
        ch match {
          case '?' if curr == "*" =>
            t
          case '?' =>
            t :+ "?"
          case '*' if curr == "*" =>
            t
          case '*' if curr == "?" =>
            var prefix: Vector[String] = t
            while (prefix.last == "?")
              prefix = prefix.init
            prefix :+ "*"
          case _ if curr == "*" || curr == "?" =>
            t :+ ch.toString
          case _ =>
            t.init :+ (curr + ch)
        }
    }
    tokens match {
      case Vector("*") => MatchAny
      case vector if vector.length == 1 => MatchExact(vector.head)
      case _ =>
        val regex = tokens.map {
          case "*" => ".*"
          case "?" => ".?"
          case token => escape(token)
        }.mkString
        MatchRegex(Pattern.compile(regex))
    }
  }

  def schemeMatcher: Parser[SegmentMatcher] = regex("""[a-zA-Z?*][a-zA-Z0-9+.\-*?]*""".r) ^^ parseGlob

  def locationMatcher: Parser[SegmentMatcher] = regex("""[^/]+""".r) ^^ parseGlob

  def pathMatcher: Parser[PathMatcher] = rep1(regex("""/[^/]*""".r)) ^^ {
    case segments: List[String] => PathMatcher(segments.map(segment => parseGlob(segment.tail)).toVector)
  }

  def schemeLocation: Parser[ProbeMatcher] = (schemeMatcher ~ literal(":") ~ locationMatcher) ^^ {
    case scheme ~ ":" ~ location => new ProbeMatcher(Some(scheme), Some(location), None)
  }

  def schemeLocationPath: Parser[ProbeMatcher] = (schemeMatcher ~ literal(":") ~ locationMatcher ~ pathMatcher) ^^ {
    case scheme ~ ":" ~ location ~ path => new ProbeMatcher(Some(scheme), Some(location), Some(path))
  }

  val probeMatcher: Parser[ProbeMatcher] = (schemeLocationPath | schemeLocation | literal("*")) ^^ {
    case "*" => MatchesAll
    case matcher: ProbeMatcher => matcher
  }

  def parseProbeMatcher(input: String): ProbeMatcher = parseAll(probeMatcher, input) match {
    case Success(matcher: ProbeMatcher, _) => matcher
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception(failure.msg)
  }
}
