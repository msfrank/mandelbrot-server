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

package io.mandelbrot.core

import java.net.URI

import akka.actor._
import io.mandelbrot.core.agent._
import io.mandelbrot.core.entity._
import io.mandelbrot.core.model.{AgentId, NotificationEvent}
import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.check._

import scala.util.hashing.MurmurHash3

/**
 * ServiceProxy is a router for all service operation messages, responsible for sending
 * operations to the correct service.
 */
class ServiceProxy extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings
  val deliveryAttempts = settings.cluster.deliveryAttempts

  //
  val keyExtractor: EntityFunctions.KeyExtractor = {
    case op: CheckOperation => op.checkRef.agentId.toString
    case op: ProbeOperation => op.probeRef.agentId.toString
    case op: AgentOperation => op.agentId.toString
  }
  val shardResolver: EntityFunctions.ShardResolver = {
    case message => MurmurHash3.stringHash(keyExtractor(message))
  }
  val propsCreator: EntityFunctions.PropsCreator = {
    case op: RegisterAgent => Agent.props(self)
    case entity: Entity => Agent.props(self)
  }
  val entityReviver: EntityFunctions.EntityReviver = {
    case key => ReviveAgent(AgentId(key))
  }

  val registryService = context.actorOf(RegistryManager.props(settings.registry,
    settings.cluster.enabled), "registry-service")
  val notificationService = context.actorOf(NotificationManager.props(settings.notification,
    settings.cluster.enabled), "notification-service")
  val stateService = context.actorOf(StateManager.props(settings.state,
    settings.cluster.enabled), "state-service")
  val entityService = context.actorOf(EntityManager.props(settings.cluster,
    propsCreator, entityReviver), "entity-service")

  def receive = {

    case op: EntityServiceOperation =>
      entityService forward op

    case op: RegistryServiceOperation =>
      registryService forward op

    case op: StateServiceOperation =>
      stateService forward op

    case op: NotificationServiceOperation =>
      notificationService forward op

    case event: NotificationEvent =>
      notificationService forward event

    case op: ServiceOperation if keyExtractor.isDefinedAt(op) =>
      try {
        val shardKey = shardResolver(op)
        val entityKey = keyExtractor(op)
        entityService ! EntityEnvelope(sender(), op, shardKey, entityKey, deliveryAttempts, deliveryAttempts)
      } catch {
        case ex: Throwable => sender() ! EntityDeliveryFailed(op, ApiException(BadRequest))
      }
  }

  override def unhandled(message: Any): Unit = {
    message match {
      case op: ServiceOperation => log.warning("dropping unhandled ServiceOperation {}", op)
      case other => log.error("dropping unhandled message {}", other)
    }
  }
}

object ServiceProxy {
  def props() =  Props(classOf[ServiceProxy])

}

trait ServiceOperation
trait ServiceCommand extends ServiceOperation
trait ServiceQuery extends ServiceOperation
trait ServiceEvent extends ServiceOperation

trait ServiceOperationFailed {
  val op: ServiceOperation
  val failure: Throwable
}
