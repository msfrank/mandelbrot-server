package io.mandelbrot.core.registry

object ProbeConversions {

  implicit def registration2spec(registration: ProbeRegistration): ProbeSpec = {
    val children = registration.children.map { case ((name,child)) => name -> registration2spec(child) }
    ProbeSpec(registration.objectType, registration.policy, registration.metadata, children, static = false)
  }

}
