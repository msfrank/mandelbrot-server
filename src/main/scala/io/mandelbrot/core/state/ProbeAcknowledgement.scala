package io.mandelbrot.core.state

import java.util.UUID
import org.joda.time.DateTime
import io.mandelbrot.core.registry.ProbeRef

/**
 *
 */
case class ProbeAcknowledgement(probeRef: ProbeRef, id: UUID, correlation: UUID, timestamp: DateTime)

/**
 *
 */
case class Worknote(acknowledgement: UUID, timestamp: DateTime, description: String, internal: Boolean)
