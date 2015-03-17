package io.mandelbrot.core.model

import org.joda.time.DateTime
import java.util.UUID

sealed trait StateModel

/* probe status submitted by the agent */
case class ProbeEvaluation(timestamp: DateTime,
                           summary: Option[String],
                           health: Option[ProbeHealth],
                           metrics: Option[Map[String,BigDecimal]]) extends StateModel

/* the complete status of a probe */
case class ProbeStatus(timestamp: DateTime,
                       lifecycle: ProbeLifecycle,
                       summary: Option[String],
                       health: ProbeHealth,
                       metrics: Map[String,BigDecimal],
                       lastUpdate: Option[DateTime],
                       lastChange: Option[DateTime],
                       correlation: Option[UUID],
                       acknowledged: Option[UUID],
                       squelched: Boolean) extends StateModel

case class ProbeCondition(timestamp: DateTime,
                          lifecycle: ProbeLifecycle,
                          summary: Option[String],
                          health: ProbeHealth,
                          correlation: Option[UUID],
                          acknowledged: Option[UUID],
                          squelched: Boolean) extends StateModel

case class ProbeConditionPage(history: Vector[ProbeCondition], last: Option[DateTime], exhausted: Boolean) extends StateModel

case class ProbeNotifications(timestamp: DateTime, notifications: Vector[ProbeNotification]) extends StateModel

case class ProbeNotificationsPage(history: Vector[ProbeNotifications], last: Option[DateTime], exhausted: Boolean) extends StateModel

case class ProbeMetrics(timestamp: DateTime, metrics: Map[String,BigDecimal]) extends StateModel

case class ProbeMetricsPage(history: Vector[ProbeMetrics], last: Option[DateTime], exhausted: Boolean) extends StateModel
