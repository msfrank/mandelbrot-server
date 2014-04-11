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

import akka.actor._
import java.net.URI

import io.mandelbrot.core.notification.Notification

/**
 *
 */
class ProbeSystem(uri: URI, notificationManager: ActorRef) extends Actor with ActorLogging {
  import ProbeSystem._

  // state
  var probes: Map[ProbeRef,ProbeActor] = Map.empty

  def receive = {

    /* configure the probe system using the spec */
    case spec: ProbeSpec =>
      val specSet = probeSpec2Set(spec)
      val probeSet = probes.keySet
      // add new probes
      val probesAdded = specSet -- probeSet
      probesAdded.toVector.sorted.foreach { case ref: ProbeRef =>
        val actor = ref.parentOption match {
          case Some(parent) =>
            context.actorOf(Probe.props(ref, probes(parent).actor, notificationManager))
          case None =>
            context.actorOf(Probe.props(ref, self, notificationManager))
        }
        log.debug("added probe {}", ref)
        probes = probes + (ref -> ProbeActor(findProbeSpec(spec, ref.path), actor))
      }
      // remove stale probes
      val probesRemoved = probeSet -- specSet
      probesRemoved.toVector.sorted.reverse.foreach { case ref: ProbeRef =>
        probes(ref).actor ! PoisonPill
        log.debug("removed probe {}", ref)
        probes = probes - ref
      }

    /* handle notifications which have been passed up from Probe */
    case notification: Notification =>
      notificationManager.forward(notification)

    case Terminated(ref) =>
      log.debug("actor {} has been terminated", ref.path)

  }

  /**
   * flatten ProbeSpec into a Set of ProbeRefs
   */
  def probeSpec2Set(path: Vector[String], spec: ProbeSpec): Set[ProbeRef] = {
    val iterChildren = spec.children.toSet
    val childRefs = iterChildren.map { case (name: String, childSpec: ProbeSpec) =>
      probeSpec2Set(path :+ name, childSpec)
    }.flatten
    childRefs + ProbeRef(uri, path)
  }
  def probeSpec2Set(spec: ProbeSpec): Set[ProbeRef] = probeSpec2Set(Vector.empty, spec)

  /**
   * find the ProbeSpec referenced by path
   */
  def findProbeSpec(spec: ProbeSpec, path: Vector[String]): ProbeSpec = {
    if (path.isEmpty) spec else findProbeSpec(spec.children(path.head), path.tail)
  }
}

object ProbeSystem {
  def props(uri: URI, notificationManager: ActorRef) = Props(classOf[ProbeSystem], uri, notificationManager)

  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)
}

/**
 *
 */
sealed trait ProbeSystemOperation
sealed trait ProbeSystemQuery
sealed trait ProbeSystemCommand

case class DescribeProbe(probeRef: ProbeRef) extends ProbeSystemQuery

case class DescribeProbes(probeRef: ProbeRef, recursionLevel: Int) extends ProbeSystemQuery

