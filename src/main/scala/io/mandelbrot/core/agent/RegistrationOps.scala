/**
 * Copyright 2015 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.agent

import akka.actor.{Props, Actor, ActorLogging, ActorRef}

import scala.concurrent.duration._
import scala.collection.mutable

import io.mandelbrot.core.check._
import io.mandelbrot.core.model._
import io.mandelbrot.core.{BadRequest, ApiException}

/**
 *
 */
trait RegistrationOps extends Actor with ActorLogging {

  // state
  val services: ActorRef
  var checks: Map[CheckId,CheckActor]
  val retiredChecks: mutable.HashMap[ActorRef,(CheckId,Long)]
  val zombieChecks: mutable.HashSet[CheckId]
  val pendingDeletes: mutable.HashMap[ActorRef,Long]
  val observationBus: ObservationBus
  var generation: Long
  var lsn: Long

  /**
   * given a registration, return the set of all check resources, including
   * the implicit containers.
   */
  def makeRegistrationSet(registration: AgentSpec): Set[CheckId] = {
    var registrationSet = registration.checks.keySet
    for (resource <- registrationSet) {
      var parent = resource.parentOption
      while (parent.isDefined) {
        registrationSet = registrationSet + parent.get
        parent = parent.get.parentOption
      }
    }
    registrationSet
  }

  /**
   * returns Unit if the specified registration is valid according to system
   * policy, otherwise throws an Exception.
   */
  def validateAgentRegistration(registration: AgentSpec): Either[ApiException,AgentSpec] = {
    registration.checks.values.foreach {
      case checkSpec =>
        if (!ProcessorExtension.extensions.contains(checkSpec.checkType))
          return Left(ApiException(BadRequest, new NoSuchElementException(s"invalid checkType ${checkSpec.checkType}")))
    }
    Right(registration)
  }

  /**
   * apply the spec to the agent, adding and removing checks as necessary
   */
  def applyAgentRegistration(agentId: AgentId, registration: AgentSpec, lsn: Long): Unit = {

    val registrationSet = makeRegistrationSet(registration)
    val checkSet = checks.keySet

    // create props for each check which isn't in checkSet
    val checksAdded = new mutable.HashMap[CheckId,(CheckSpec,Props)]
    (registrationSet -- checkSet).toVector.sorted.foreach { case checkId: CheckId =>
      registration.checks.get(checkId) match {
        case Some(checkSpec) =>
          val extension = ProcessorExtension.extensions(checkSpec.checkType)
          val settings = extension.configure(checkSpec.properties)
          val props = extension.props(settings)
          checksAdded.put(checkId, (checkSpec,props))
        case None =>  // ignore placeholders
      }
    }

    // create props for each check which has been updated
    val checksUpdated = new mutable.HashMap[CheckId,(CheckSpec,Props)]
    checkSet.intersect(registrationSet).foreach { case checkId: CheckId =>
      registration.checks.get(checkId) match {
        case Some(checkSpec) =>
          val extension = ProcessorExtension.extensions(checkSpec.checkType)
          val settings = extension.configure(checkSpec.properties)
          val props = extension.props(settings)
          checksUpdated.put(checkId, (checkSpec,props))
        case None =>  // ignore placeholders
      }
    }

    // remove stale checks
    val checksRemoved = checkSet -- registrationSet
    checksRemoved.toVector.sorted.reverse.foreach { case checkId: CheckId =>
      log.debug("check {} retires", checkId)
      val CheckActor(_, _, actor) = checks(checkId)
      actor ! RetireCheck(lsn)
      retiredChecks.put(actor, (checkId,lsn))
    }

    // create check actors for each added check
    checksAdded.keys.toVector.sorted.foreach { checkId =>
      val (checkSpec, props) = checksAdded(checkId)
      val checkRef = CheckRef(agentId, checkId)
      val actor = checkId.parentOption match {
        case Some(parent) =>
          context.actorOf(Check.props(checkRef, generation, checks(parent).actor, services, observationBus))
        case _ =>
          context.actorOf(Check.props(checkRef, generation, self, services, observationBus))
      }
      log.debug("check {} joins {}", checkId, agentId)
      context.watch(actor)
      checks = checks + (checkId -> CheckActor(checkSpec, props, actor))
    }

    // update existing checks and mark zombie checks
    checksUpdated.keys.toVector.foreach { checkId =>
      val (checkSpec, props) = checksUpdated(checkId)
      if (retiredChecks.contains(checks(checkId).actor)) {
        zombieChecks.add(checkId)
      } else {
        val CheckActor(_, _, actor) = checks(checkId)
        checks = checks + (checkId -> CheckActor(checkSpec, props, actor))
      }
    }

    // signal added and updated checks to gather initializer data and update check state
    (checksAdded.keySet ++ checksUpdated.keySet).foreach { case checkId =>
      val CheckActor(checkSpec, props, actor) = checks(checkId)
      val directChildren = registrationSet.filter { _.parentOption match {
        case Some(parent) => parent == checkId
        case None => false
      }}.map(childId => CheckRef(agentId, childId))
      actor ! ChangeCheck(checkSpec.checkType, checkSpec.policy, props, directChildren, lsn)
    }
  }
}

case class CheckActor(spec: CheckSpec, props: Props, actor: ActorRef)
