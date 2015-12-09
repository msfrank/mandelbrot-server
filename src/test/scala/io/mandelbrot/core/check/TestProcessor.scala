package io.mandelbrot.core.check

import akka.actor._
import spray.json.JsObject
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core.model.json.JsonProtocol._

case class TestProcessorSettings(statusList: Vector[(CheckHealth,Option[String])], interval: FiniteDuration)

class TestProcessor(settings: TestProcessorSettings) extends Actor with ActorLogging {
  import context.dispatcher

  var left: Vector[(CheckHealth,Option[String])] = settings.statusList
  var lsn: Long = 0
  var parent: ActorRef = ActorRef.noSender
  var cancellable: Option[Cancellable] = None

  def receive = {

    case change: ChangeProcessor =>
      lsn = change.lsn
      parent = sender()
      cancellable = Some(context.system.scheduler.schedule(settings.interval, settings.interval, self, "next"))
      log.debug("test processor changes: {}", change)

    case "next" =>
      left.headOption match {
        case Some((health, summary)) =>
          val timestamp = Timestamp()
          val status = ProcessorStatus(lsn, timestamp, health, summary)
          parent ! status
          log.debug("test processor emits status {}", status)
          left = left.tail
        case None =>  // do nothing
      }
  }

  override def postStop(): Unit = cancellable.foreach(_.cancel())
}

object TestProcessor {
  def props(settings: TestProcessorSettings) = Props(classOf[TestProcessor], settings)
}

class TestCheck extends ProcessorExtension {
  type Settings = TestProcessorSettings
  implicit val TestProcessorSettingsFormat = jsonFormat2(TestProcessorSettings)
  def configure(json: Option[JsObject]) = json.map(_.convertTo[TestProcessorSettings])
    .getOrElse(throw new IllegalArgumentException())
  def props(settings: TestProcessorSettings) = TestProcessor.props(settings)
}
