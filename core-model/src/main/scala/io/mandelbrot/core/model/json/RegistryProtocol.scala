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

package io.mandelbrot.core.model.json

import spray.json._

import io.mandelbrot.core.model._

/**
 *
 */
trait RegistryProtocol extends DefaultJsonProtocol with ConstantsProtocol with ResourceProtocol with MetricsProtocol {

  /* convert AgentPolicy class */
  implicit val AgentPolicyFormat = jsonFormat1(AgentPolicy)

  /* convert CheckPolicy class */
  implicit val ProbePolicyFormat = jsonFormat1(ProbePolicy)

  /* convert CheckPolicy class */
  implicit val CheckPolicyFormat = jsonFormat5(CheckPolicy)

  /* convert MetricSpec class */
  implicit val MetricSpecFormat = jsonFormat2(MetricSpec)

  /* convert CheckSpec class */
  implicit val CheckSpecFormat = jsonFormat4(CheckSpec)

  /* convert CheckSpec class */
  implicit val ProbeSpecFormat = jsonFormat4(ProbeSpec)

  /* convert AgentSpec class */
  implicit val AgentSpecFormat = jsonFormat7(AgentSpec)

  /* convert AgentMetadata class */
  implicit val AgentMetadataFormat = jsonFormat5(AgentMetadata)

  /* convert MetricsEvaluation class */
//  implicit object TimeseriesEvaluationFormat extends RootJsonFormat[TimeseriesEvaluation] {
//    val parser = TimeseriesEvaluationParser.parser
//    def write(evaluation: TimeseriesEvaluation) = JsString(evaluation.toString)
//    def read(value: JsValue) = value match {
//      case JsString(string) => parser.parseTimeseriesEvaluation(string)
//      case _ => throw new DeserializationException("expected MetricsEvaluation")
//    }
//  }

  /* convert MetadataPage class */
  implicit val MetadataPageFormat = jsonFormat3(MetadataPage)

  /* convert RegistrationsPage class */
  implicit val RegistrationsPageFormat = jsonFormat3(RegistrationsPage)

  /* convert GroupsPage class */
  implicit val GroupsPageFormat = jsonFormat3(GroupsPage)
}
