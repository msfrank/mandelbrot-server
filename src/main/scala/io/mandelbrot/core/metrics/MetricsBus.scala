package io.mandelbrot.core.metrics

import akka.actor.ActorRef
import akka.event.{LookupClassification, EventBus}

import io.mandelbrot.core.system.{ProbeRef, MetricsMessage}

/**
 *
 */
class MetricsBus extends EventBus with LookupClassification {
  type Event = MetricsMessage
  type Classifier = ProbeRef
  type Subscriber = ActorRef

  override protected def classify(event: Event): Classifier = event.source
  override protected def publish(event: Event, subscriber: Subscriber): Unit = subscriber ! event
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int = a.compareTo(b)

  // determines the initial size of the index data structure
  // used internally (i.e. the expected number of different classifiers)
  override protected def mapSize(): Int = 128
}
