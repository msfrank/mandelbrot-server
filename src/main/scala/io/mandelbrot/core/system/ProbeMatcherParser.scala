/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.system

import org.slf4j.LoggerFactory
import scala.util.parsing.combinator.RegexParsers

import io.mandelbrot.core.model._

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

  def parseGlob(glob: String): SegmentMatcher = {
    val tokens: Vector[String] = glob.foldLeft(Vector.empty[String]) {
      /* first time around we just store the char */
      case (t, ch) if t.isEmpty => Vector(ch.toString)
      /* if ch is '?' */
      case (t, '?') =>
        val curr = t.last
        /* if last char was *, then don't store anything, otherwise append the ? */
        if (curr == "*") t else t :+ "?"
      /* if ch is '*' */
      case (t, '*') =>
        val curr = t.last
        /*
         * if last char was *, then don't store anything, otherwise if last char
         * was ? then we back out any ? chars and replace with a single *, otherwise
         * just append the *
         */
        if (curr == "*") t else if (curr == "?") {
          var prefix: Vector[String] = t
          while (prefix.last == "?")
            prefix = prefix.init
          prefix :+ "*"
        } else t :+ "*"
      /* if ch is not a matcher ('?' or '*') */
      case (t, ch) =>
        val curr = t.last
        /* if last char was a matcher, then append to tokens list, otherwise append to last token */
        if (curr == "*" || curr == "?") t :+ ch.toString else t.init :+ (curr + ch)
    }
    tokens match {
      case Vector("*") => MatchAny
      case Vector("?") => MatchGlob(tokens)
      case vector if vector.length == 1 => MatchExact(vector.head)
      case _ => MatchGlob(tokens)
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
