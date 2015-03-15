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
    case op: CreateProbeSystemEntry =>
      if (!registrationValid(op.registration))
        sender() ! RegistryServiceOperationFailed(op, ApiException(BadRequest))
      else registrar forward op

    case op: UpdateProbeSystemEntry =>
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
  def registrationValid(registration: ProbeRegistration): Boolean = {
    // FIXME: implement validation logic
    true
  }
}

object RegistryManager {
  def props(settings: RegistrySettings) = Props(classOf[RegistryManager], settings)
  def settings(config: Config): Option[Any] = None
}

/* object registry operations */
sealed trait RegistryServiceOperation extends ServiceOperation
sealed trait RegistryServiceCommand extends ServiceCommand with RegistryServiceOperation
sealed trait RegistryServiceQuery extends ServiceQuery with RegistryServiceOperation
case class RegistryServiceOperationFailed(op: RegistryServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class CreateProbeSystemEntry(uri: URI, registration: ProbeRegistration) extends RegistryServiceCommand
case class CreateProbeSystemEntryResult(op: CreateProbeSystemEntry, lsn: Long)

case class UpdateProbeSystemEntry(uri: URI, registration: ProbeRegistration, lsn: Long) extends RegistryServiceCommand
case class UpdateProbeSystemEntryResult(op: UpdateProbeSystemEntry, lsn: Long)

case class DeleteProbeSystemEntry(uri: URI, lsn: Long) extends RegistryServiceCommand
case class DeleteProbeSystemEntryResult(op: DeleteProbeSystemEntry, lsn: Long)

case class ListProbeSystems(limit: Int, last: Option[String]) extends RegistryServiceQuery
case class ListProbeSystemsResult(op: ListProbeSystems, page: ProbeSystemsPage)

case class GetProbeSystemEntry(uri: URI) extends RegistryServiceQuery
case class GetProbeSystemEntryResult(op: GetProbeSystemEntry, registration: ProbeRegistration, lsn: Long)
