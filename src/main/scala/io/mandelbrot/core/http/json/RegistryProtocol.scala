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
trait RegistryProtocol extends DefaultJsonProtocol with BasicProtocol with MetricsProtocol {

  /* convert MetricSpec class */
  implicit val MetricSpecFormat = jsonFormat5(MetricSpec)

  /* a little extra magic here- we use lazyFormat because ProbeSpec has a recursive definition */
  implicit val _ProbeSpecFormat: JsonFormat[ProbeSpec] = lazyFormat(jsonFormat(ProbeSpec, "probeType", "metadata", "policy", "behavior", "children"))
  implicit val ProbeSpecFormat = rootFormat(_ProbeSpecFormat)

  /* convert ProbeRegistration class */
  implicit val ProbeRegistrationFormat = jsonFormat4(ProbeRegistration)

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

  /* convert ProbeBehavior implementations */
  implicit val ScalarBehaviorFormat = jsonFormat2(ScalarProbeBehavior)
  implicit val AggregateBehaviorFormat = jsonFormat3(AggregateProbeBehavior)
  implicit val MetricsBehaviorFormat = jsonFormat3(MetricsProbeBehavior)

  /* convert ProbeBehavior class */
  implicit object ProbeBehaviorFormat extends RootJsonFormat[ProbeBehavior] {
    def write(behaviorPolicy: ProbeBehavior) = behaviorPolicy match {
      case behavior: AggregateProbeBehavior =>
        JsObject(Map("behaviorType" -> JsString("aggregate"), "behaviorPolicy" -> behavior.toJson))
      case behavior: ScalarProbeBehavior =>
        JsObject(Map("behaviorType" -> JsString("scalar"), "behaviorPolicy" -> behavior.toJson))
      case behavior: MetricsProbeBehavior =>
        JsObject(Map("behaviorType" -> JsString("metrics"), "behaviorPolicy" -> behavior.toJson))
      case unknown => throw new SerializationException("unknown BehaviorPolicy " + unknown.getClass)
    }

    def read(value: JsValue) = value match {
      case JsObject(fields) =>
        if (!fields.contains("behaviorPolicy"))
          throw new DeserializationException("missing behaviorPolicy")
        fields.get("behaviorType") match {
          case Some(JsString("aggregate")) =>
            AggregateBehaviorFormat.read(fields("behaviorPolicy"))
          case Some(JsString("scalar")) =>
            ScalarBehaviorFormat.read(fields("behaviorPolicy"))
          case Some(JsString("metrics")) =>
            MetricsBehaviorFormat.read(fields("behaviorPolicy"))
          case Some(JsString(unknown)) =>
            throw new DeserializationException("unknown behaviorType " + unknown)
          case None =>
            throw new DeserializationException("missing behaviorType")
          case unknownValue =>
            throw new DeserializationException("behaviorType is not a string")
        }
      case unknown => throw new DeserializationException("unknown BehaviorPolicy " + unknown)
    }
  }

  /* convert ProbePolicy class */
  implicit val ProbePolicyFormat = jsonFormat5(ProbePolicy)

  /* convert ProbeSystemMetadata class */
  implicit val ProbeSystemMetadataFormat = jsonFormat3(ProbeSystemMetadata)

  /* convert ProbeSystemsPage class */
  implicit val ProbeSystemsPageFormat = jsonFormat2(ProbeSystemsPage)

  /* registry operations */
  implicit val RegisterProbeSystemFormat = jsonFormat2(RegisterProbeSystem)

  /* probe system operations */
  implicit val UpdateProbeSystemFormat = jsonFormat2(UpdateProbeSystem)
}
