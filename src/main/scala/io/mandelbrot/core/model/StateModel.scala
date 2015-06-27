package io.mandelbrot.core.model

import org.joda.time.DateTime
import java.util.UUID

sealed trait StateModel

/* check status submitted by the agent */
case class CheckEvaluation(timestamp: DateTime,
                           summary: Option[String],
                           health: Option[CheckHealth],
                           metrics: Option[Map[String,BigDecimal]]) extends StateModel

/* the complete status of a check */
case class CheckStatus(generation: Long,
                       timestamp: DateTime,
                       lifecycle: CheckLifecycle,
                       summary: Option[String],
                       health: CheckHealth,
                       metrics: Map[String,BigDecimal],
                       lastUpdate: Option[DateTime],
                       lastChange: Option[DateTime],
                       correlation: Option[UUID],
                       acknowledged: Option[UUID],
                       squelched: Boolean) extends StateModel

case class CheckCondition(generation: Long,
                          timestamp: DateTime,
                          lifecycle: CheckLifecycle,
                          summary: Option[String],
                          health: CheckHealth,
                          correlation: Option[UUID],
                          acknowledged: Option[UUID],
                          squelched: Boolean) extends StateModel

case class CheckConditionPage(history: Vector[CheckCondition], last: Option[String], exhausted: Boolean) extends StateModel

case class CheckNotifications(generation: Long, timestamp: DateTime, notifications: Vector[CheckNotification]) extends StateModel

case class CheckNotificationsPage(history: Vector[CheckNotifications], last: Option[String], exhausted: Boolean) extends StateModel

case class CheckMetrics(generation: Long, timestamp: DateTime, metrics: Map[String,BigDecimal]) extends StateModel

case class CheckMetricsPage(history: Vector[CheckMetrics], last: Option[String], exhausted: Boolean) extends StateModel
