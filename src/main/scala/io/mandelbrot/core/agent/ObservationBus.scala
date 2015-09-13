package io.mandelbrot.core.agent

import akka.actor.ActorRef
import akka.event.{EventBus, LookupClassification}

import io.mandelbrot.core.check.ProcessObservation
import io.mandelbrot.core.model._

/**
 *
 */
class ObservationBus extends EventBus with LookupClassification {
  type Subscriber = ActorRef
  type Event = ProcessObservation
  type Classifier = ProbeId

  override protected def classify(event: Event): Classifier = event.probeId
  override protected def publish(event: Event, subscriber: Subscriber): Unit = subscriber ! event
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int = a.compareTo(b)

  // determines the initial size of the index data structure
  // used internally (i.e. the expected number of different classifiers)
  override protected def mapSize(): Int = 32
}
