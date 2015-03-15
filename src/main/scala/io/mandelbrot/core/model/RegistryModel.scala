package io.mandelbrot.core.model

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration
import java.net.URI

sealed trait RegistryModel

/* a dynamic probe system registration */
case class ProbeRegistration(systemType: String,
                             metadata: Map[String,String],
                             probes: Map[String,ProbeSpec],
                             metrics: Map[MetricSource,MetricSpec]) extends RegistryModel

/* probe tunable parameters which apply to all probe types */
case class ProbePolicy(joiningTimeout: FiniteDuration,
                       probeTimeout: FiniteDuration,
                       alertTimeout: FiniteDuration,
                       leavingTimeout: FiniteDuration,
                       notifications: Option[Set[String]]) extends RegistryModel

/* probe specification */
case class ProbeSpec(probeType: String,
                     policy: ProbePolicy,
                     properties: Map[String,String],
                     metadata: Map[String,String],
                     children: Map[String,ProbeSpec]) extends RegistryModel

/* metric specification */
case class MetricSpec(sourceType: SourceType,
                      metricUnit: MetricUnit,
                      step: Option[FiniteDuration],
                      heartbeat: Option[FiniteDuration],
                      cf: Option[ConsolidationFunction]) extends RegistryModel

/* */
case class ProbeSystemMetadata(uri: URI, joinedOn: DateTime, lastUpdate: DateTime) extends RegistryModel

/* */
case class ProbeSystemsPage(systems: Vector[ProbeSystemMetadata], last: Option[String]) extends RegistryModel
