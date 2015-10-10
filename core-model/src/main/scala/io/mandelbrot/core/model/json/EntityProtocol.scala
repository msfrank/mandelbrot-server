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

package io.mandelbrot.core.model.json

import akka.actor.{Address, AddressFromURIString}
import akka.cluster.MemberStatus
import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait EntityProtocol extends DefaultJsonProtocol with ConstantsProtocol {

  /* convert Address class */
  implicit object ClusterAddressFormat extends RootJsonFormat[Address] {
    def write(address: Address) = JsString(address.toString)
    def read(value: JsValue) = AddressFromURIString(value.toString())
  }

  /* convert MemberStatus class */
  implicit object MemberStatusFormat extends RootJsonFormat[MemberStatus] {
    import akka.cluster.MemberStatus._
    def write(status: MemberStatus) = status match {
      case Up => JsString("up")
      case Down => JsString("down")
      case Exiting => JsString("exiting")
      case Joining => JsString("joining")
      case Leaving => JsString("leaving")
      case Removed => JsString("removed")
      case unknown => throw new DeserializationException("unknown MemberStatus type " + status.toString)
    }
    def read(value: JsValue) = value match {
      case JsString("up") => Up
      case JsString("down") => Down
      case JsString("exiting") => Exiting
      case JsString("joining") => Joining
      case JsString("leaving") => Leaving
      case JsString("removed") => Removed
      case unknown => throw new DeserializationException("unknown MemberStatus type " + value.toString())
    }
  }

  /* convert ShardEntry class */
  implicit object ShardEntryFormat extends RootJsonFormat[ShardEntry] {
    def write(entry: ShardEntry) = entry match {
      case AssignedShardEntry(shardId, address) =>
        JsObject(Map(
          "status" -> JsString("assigned"),
          "shardId" -> JsNumber(shardId),
          "address" -> JsString(address.toString)
        ))
      case PreparingShardEntry(shardId, address) =>
        JsObject(Map(
          "status" -> JsString("preparing"),
          "shardId" -> JsNumber(shardId),
          "address" -> JsString(address.toString)
        ))
      case MigratingShardEntry(shardId, address) =>
        JsObject(Map(
          "status" -> JsString("migrating"),
          "shardId" -> JsNumber(shardId),
          "address" -> JsString(address.toString)
        ))
      case MissingShardEntry(shardId) =>
        JsObject(Map(
          "status" -> JsString("assigned"),
          "shardId" -> JsNumber(shardId)
        ))
    }
    def read(value: JsValue): ShardEntry = value match {
      case JsObject(fields) =>
        val shardId = fields.get("shardId") match {
          case Some(JsNumber(number)) => number.toInt
          case None => throw new DeserializationException("ShardEntry missing field 'shardId'")
          case unknown => throw new DeserializationException("failed to parse ShardEntry field 'shardId'")
        }
        val address = fields.get("address") map {
          case JsString(string) => AddressFromURIString(string)
          case unknown => throw new DeserializationException("failed to parse ShardEntry field 'status'")
        }
        fields.get("status") match {
          case Some(JsString("missing")) if address.isEmpty =>
            MissingShardEntry(shardId)
          case Some(JsString("assigned")) if address.isDefined =>
            AssignedShardEntry(shardId, address.get)
          case Some(JsString("preparing")) if address.isDefined =>
            PreparingShardEntry(shardId, address.get)
          case Some(JsString("migrating")) if address.isDefined =>
            MigratingShardEntry(shardId, address.get)
          case None =>
            throw new DeserializationException("ShardEntry missing field 'status'")
          case unknown =>
            throw new DeserializationException("failed to parse ShardEntry format")
        }
      case unknown =>
        throw new DeserializationException("unknown ShardEntry format")
    }
  }

  /* convert ShardMapStatus class */
  implicit val ShardMapStatusFormat = jsonFormat2(ShardMapStatus)

  /* convert ClusterStatus class */
  implicit val NodeStatusFormat = jsonFormat4(NodeStatus)

  /* convert ClusterStatus class */
  implicit val ClusterStatusFormat = jsonFormat3(ClusterStatus)
}
