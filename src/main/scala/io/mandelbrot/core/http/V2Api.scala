package io.mandelbrot.core.http

/**
 *
 */
trait V2Api extends AgentsRoutes with NodesRoutes with ShardsRoutes {

  val version2 = pathPrefix("v2") {
    agentsRoutes ~ shardsRoutes ~ nodesRoutes
  }

  val routes = version2
}
