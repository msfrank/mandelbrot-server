package io.mandelbrot.core.model

import org.joda.time.DateTime
import java.util.UUID

sealed trait StateModel

/* check status submitted by the agent */
case class CheckEvaluation(timestamp: DateTime,
                           metrics: Map[String,BigDecimal],
                           summary: Option[String] = None) extends StateModel

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

/* the condition of a check */
case class CheckCondition(generation: Long,
                          timestamp: DateTime,
                          lifecycle: CheckLifecycle,
                          summary: Option[String],
                          health: CheckHealth,
                          correlation: Option[UUID],
                          acknowledged: Option[UUID],
                          squelched: Boolean) extends StateModel

/* the set of notifications emitted by a check */
case class CheckNotifications(generation: Long, timestamp: DateTime, notifications: Vector[CheckNotification]) extends StateModel

/* the set of metrics emitted by a check */
case class CheckMetrics(generation: Long, timestamp: DateTime, metrics: Map[String,BigDecimal]) extends StateModel

/* a page of check status entries */
case class CheckStatusPage(history: Vector[CheckStatus], last: Option[String], exhausted: Boolean) extends StateModel

/* a page of check condition entries */
case class CheckConditionPage(history: Vector[CheckCondition], last: Option[String], exhausted: Boolean) extends StateModel

/* a page of check notifications entries */
case class CheckNotificationsPage(history: Vector[CheckNotifications], last: Option[String], exhausted: Boolean) extends StateModel

/* a page of check metrics entries */
case class CheckMetricsPage(history: Vector[CheckMetrics], last: Option[String], exhausted: Boolean) extends StateModel
