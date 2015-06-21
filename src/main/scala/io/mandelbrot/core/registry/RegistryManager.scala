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
import org.joda.time.DateTime

import io.mandelbrot.core._
import io.mandelbrot.core.model._

/**
 *
 */
class RegistryManager(settings: RegistrySettings) extends Actor with ActorLogging {

  // state
  val registrar: ActorRef = context.actorOf(settings.props, "registrar")

  def receive = {

    case op: PutRegistration =>
      if (!registrationValid(op.registration))
        sender() ! RegistryServiceOperationFailed(op, ApiException(BadRequest))
      else registrar forward op

    case op: CommitRegistration =>
      if (!registrationValid(op.registration))
        sender() ! RegistryServiceOperationFailed(op, ApiException(BadRequest))
      else registrar forward op

    case op: RegistryServiceQuery =>
      registrar forward op

    case op: RegistryServiceCommand =>
      registrar forward op
  }

  /**
   * Returns true if the specified registration parameters adhere to server
   * policy, otherwise returns false.
   */
  def registrationValid(registration: AgentSpec): Boolean = {
    // FIXME: implement validation logic
    true
  }
}

object RegistryManager {
  def props(settings: RegistrySettings) = Props(classOf[RegistryManager], settings)
  def settings(config: Config): Option[Any] = None

  val MinGeneration = 0L
  val MaxGeneration = Long.MaxValue
  val MinLsn = 0L
  val MaxLsn = Long.MaxValue
  val MinGenerationLsn = GenerationLsn(MinGeneration, MinLsn)
  val MaxGenerationLsn = GenerationLsn(MaxGeneration, MaxLsn)
}

/* registry operations */
sealed trait RegistryServiceOperation extends ServiceOperation
sealed trait RegistryServiceCommand extends ServiceCommand with RegistryServiceOperation
sealed trait RegistryServiceQuery extends ServiceQuery with RegistryServiceOperation
case class RegistryServiceOperationFailed(op: RegistryServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class GetRegistration(agentId: AgentId) extends RegistryServiceQuery
case class GetRegistrationResult(op: GetRegistration,
                                 registration: AgentSpec,
                                 metadata: AgentMetadata,
                                 lsn: Long,
                                 committed: Boolean)

case class GetRegistrationHistory(agentId: AgentId,
                                  from: Option[GenerationLsn],
                                  to: Option[GenerationLsn],
                                  limit: Int,
                                  fromInclusive: Boolean = false,
                                  toExclusive: Boolean = false,
                                  descending: Boolean = false,
                                  last: Option[String] = None) extends RegistryServiceQuery
case class GetRegistrationHistoryResult(op: GetRegistrationHistory, page: RegistrationsPage)

case class PutRegistration(agentId: AgentId,
                           registration: AgentSpec,
                           metadata: AgentMetadata,
                           lsn: Long) extends RegistryServiceCommand
case class PutRegistrationResult(op: PutRegistration, metadata: AgentMetadata)

case class CommitRegistration(agentId: AgentId,
                              registration: AgentSpec,
                              metadata: AgentMetadata,
                              lsn: Long) extends RegistryServiceCommand
case class CommitRegistrationResult(op: CommitRegistration)

case class DeleteRegistration(agentId: AgentId, generation: Long) extends RegistryServiceCommand
case class DeleteRegistrationResult(op: DeleteRegistration)

case class ListTombstones(olderThan: DateTime, limit: Int) extends RegistryServiceQuery
case class ListTombstonesResult(op: ListTombstones, tombstones: Vector[AgentTombstone])

case class PutTombstone(agentId: AgentId, generation: Long, expires: DateTime) extends RegistryServiceCommand
case class PutTombstoneResult(op: PutTombstone)

case class DeleteTombstone(agentId: AgentId, generation: Long, expires: DateTime) extends RegistryServiceCommand
case class DeleteTombstoneResult(op: DeleteTombstone)

case class AddAgentToGroup(metadata: AgentMetadata, groupName: String) extends RegistryServiceCommand
case class AddAgentToGroupResult(op: AddAgentToGroup)

case class RemoveAgentFromGroup(agentId: AgentId, groupName: String) extends RegistryServiceCommand
case class RemoveAgentFromGroupResult(op: RemoveAgentFromGroup)

case class DescribeGroup(groupName: String, limit: Int, last: Option[String]) extends RegistryServiceQuery
case class DescribeGroupResult(op: DescribeGroup, page: MetadataPage)

case class ListGroups(limit: Int, last: Option[String]) extends RegistryServiceQuery
case class ListGroupsResult(op: ListGroups, page: GroupsPage)

