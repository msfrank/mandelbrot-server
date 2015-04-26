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
import com.typesafe.config.Config
import java.net.URI

import io.mandelbrot.core._
import io.mandelbrot.core.model._

/**
 * the registry manager holds a map of all probe systems in memory, and is the parent actor
 * of all probe systems (which in turn are the parents of each probe in a system).  The registry
 * manager is responsible for accepting registration, update, and unregistration requests and
 * applying them to the appropriate probe system.
 */
class RegistryManager(settings: RegistrySettings) extends Actor with ActorLogging {

  // state
  val registrar: ActorRef = context.actorOf(settings.props, "registrar")

  def receive = {
    case op: CreateRegistration =>
      if (!registrationValid(op.registration))
        sender() ! RegistryServiceOperationFailed(op, ApiException(BadRequest))
      else registrar forward op

    case op: UpdateRegistration =>
      if (!registrationValid(op.registration))
        sender() ! RegistryServiceOperationFailed(op, ApiException(BadRequest))
      else registrar forward op

    case op: RegistryServiceOperation =>
      registrar forward op
  }

  /**
   * Returns true if the specified registration parameters adhere to server
   * policy, otherwise returns false.
   */
  def registrationValid(registration: AgentRegistration): Boolean = {
    // FIXME: implement validation logic
    true
  }
}

object RegistryManager {
  def props(settings: RegistrySettings) = Props(classOf[RegistryManager], settings)
  def settings(config: Config): Option[Any] = None
}

/* registry operations */
sealed trait RegistryServiceOperation extends ServiceOperation
sealed trait RegistryServiceCommand extends ServiceCommand with RegistryServiceOperation
sealed trait RegistryServiceQuery extends ServiceQuery with RegistryServiceOperation
case class RegistryServiceOperationFailed(op: RegistryServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class CreateRegistration(agentId: AgentId, registration: AgentRegistration) extends RegistryServiceCommand
case class CreateRegistrationResult(op: CreateRegistration, metadata: AgentMetadata)

case class UpdateRegistration(agentId: AgentId, registration: AgentRegistration, lsn: Long) extends RegistryServiceCommand
case class UpdateRegistrationResult(op: UpdateRegistration, metadata: AgentMetadata)

case class DeleteRegistration(agentId: AgentId, lsn: Long) extends RegistryServiceCommand
case class DeleteRegistrationResult(op: DeleteRegistration, lsn: Long)

case class ListRegistrations(limit: Int, last: Option[String]) extends RegistryServiceQuery
case class ListRegistrationsResult(op: ListRegistrations, page: AgentsPage)

case class GetRegistration(agentId: AgentId) extends RegistryServiceQuery
case class GetRegistrationResult(op: GetRegistration, registration: AgentRegistration, lsn: Long)
