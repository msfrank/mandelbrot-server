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

package io.mandelbrot.core.registry

import java.net.URI

/**
 *
 */
class ProbeRef(val uri: URI, val path: Vector[String]) extends Ordered[ProbeRef] {

  def parentOption: Option[ProbeRef] = if (path.isEmpty) None else Some(new ProbeRef(uri, path.init))

  def hasParent: Boolean = !path.isEmpty

  def compare(that: ProbeRef): Int = toString.compareTo(that.toString)

  override def toString = uri.toString + "/" + path.mkString("/")
}

object ProbeRef {

  def apply(uri: URI, path: Vector[String]): ProbeRef = new ProbeRef(uri, path)

  def unapply(probeRef: ProbeRef): Option[(URI,Vector[String])] = Some((probeRef.uri, probeRef.path))
}
