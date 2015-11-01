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
import spray.http.{StatusCodes, HttpResponse}
import spray.httpx.SprayJsonSupport._

import io.mandelbrot.core._
import io.mandelbrot.core.ingest.{AppendObservationResult, AppendObservation}
import io.mandelbrot.core.model.json._

/**
 * GroupsRoutes contains all HTTP routes for interacting with agent groups.
 */
//trait IngestRoutes extends ApiService {
//  import RoutingDirectives._
//  import JsonProtocol._
//
//  val ingestRoutes = {
//    pathPrefix("ingest") {
//      pathEndOrSingleSlash {
//        post {
//          complete {
//            serviceProxy.ask(AppendObservation()).map {
//              case result: AppendObservationResult =>
//                HttpResponse(StatusCodes.OK)
//              case failure: ServiceOperationFailed =>
//                throw failure.failure
//            }
//        }}
//      }
//    }
//  }
//}
