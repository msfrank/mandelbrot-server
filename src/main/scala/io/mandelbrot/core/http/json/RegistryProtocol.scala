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

package io.mandelbrot.core.http.json

import spray.json._

import io.mandelbrot.core.metrics._
import io.mandelbrot.core.model._
import io.mandelbrot.core.system._

/**
 *
 */
trait RegistryProtocol extends DefaultJsonProtocol with ConstantsProtocol with ResourceProtocol with MetricsProtocol {

  /* convert CheckPolicy class */
  implicit val CheckPolicyFormat = jsonFormat5(CheckPolicy)

  /* convert MetricSpec class */
  implicit val MetricSpecFormat = jsonFormat5(MetricSpec)

  /* a little extra magic here- we use lazyFormat because CheckSpec has a recursive definition */
  implicit val CheckSpecFormat = jsonFormat4(CheckSpec)

  /* convert AgentRegistration class */
  implicit val AgentRegistrationFormat = jsonFormat5(AgentRegistration)

  /* convert AggregateEvaluation class */
  implicit object AggregateEvaluationFormat extends RootJsonFormat[AggregateEvaluation] {
    def write(evaluation: AggregateEvaluation) = JsString(evaluation.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => EvaluateWorst
      case _ => throw new DeserializationException("expected AggregateEvaluation")
    }
  }

  /* convert MetricsEvaluation class */
  implicit object MetricsEvaluationFormat extends RootJsonFormat[MetricsEvaluation] {
    val metricsEvaluationParser = new MetricsEvaluationParser()
    def write(evaluation: MetricsEvaluation) = JsString(evaluation.toString)
    def read(value: JsValue) = value match {
      case JsString(string) => metricsEvaluationParser.parseMetricsEvaluation(string)
      case _ => throw new DeserializationException("expected MetricsEvaluation")
    }
  }

  /* convert AgentMetadata class */
  implicit val AgentMetadataFormat = jsonFormat4(AgentMetadata)

  /* convert AgentsPage class */
  implicit val AgentsPageFormat = jsonFormat2(AgentsPage)

  /* registry operations */
  implicit val RegisterProbeSystemFormat = jsonFormat2(RegisterProbeSystem)

  /* probe system operations */
  implicit val UpdateProbeSystemFormat = jsonFormat2(UpdateProbeSystem)
}
