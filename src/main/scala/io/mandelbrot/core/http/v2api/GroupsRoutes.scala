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

package io.mandelbrot.core.http.v2api

import akka.pattern.ask
import spray.httpx.SprayJsonSupport._

import io.mandelbrot.core._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.http.json._

/**
 * GroupsRoutes contains all HTTP routes for interacting with agent groups.
 */
trait GroupsRoutes extends ApiService {
  import RoutingDirectives._
  import JsonProtocol._

  val groupsRoutes = {
    pathPrefix("groups" / Segment) { case groupName =>
      /* page through agents in the specified group */
      get {
        pagingParams { paging =>
        complete {
          val limit = paging.limit.getOrElse(settings.pageLimit)
          serviceProxy.ask(DescribeGroup(groupName, limit, paging.last)).map {
            case result: DescribeGroupResult =>
              result.page
            case failure: ServiceOperationFailed =>
              throw failure.failure
          }
        }}
      }
    }
  }
}
