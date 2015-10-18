package io.mandelbrot.core.ingest

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.{BadRequest, ResourceNotFound, ApiException, ServerConfig}

class TestObservationIngester(settings: TestObservationIngesterSettings) extends Actor with ActorLogging {

  // config
  val numPartitions = settings.numPartitions

  // state
  val checkpoints = new java.util.HashMap[String,Int]
  val partitions = new java.util.HashMap[String,util.ArrayList[AppendObservation]]
  var currentPartition = 0

  override def preStart(): Unit = {
    0.until(numPartitions).map(n => s"partition-$n").foreach { partitionName =>
      partitions.put(partitionName, new java.util.ArrayList[AppendObservation]())
    }
    partitions.keys.foreach { partitionName => checkpoints.put(partitionName, 0) }
  }

  def receive = {

    case op: AppendObservation =>
      val partitionName = s"partition-$currentPartition"
      currentPartition = if (currentPartition + 1 == numPartitions) 0 else currentPartition + 1
      val partition = partitions.get(partitionName)
      partition.add(op)
      sender() ! AppendObservationResult(op)

    case op: GetObservations =>
      Option(partitions.get(op.partition)) match {
        case None =>
          sender() ! IngestServiceOperationFailed(op, ApiException(ResourceNotFound, s"no such partition ${op.partition}"))
        case Some(partition) =>
          val index = op.token.map(token2index).getOrElse(0)
          try {
            val observations = partition.listIterator(index)
              .take(op.count)
              .map(_.observation)
              .toVector
            val token = index2token(index + observations.length)
            sender() ! GetObservationsResult(op, observations, token)
          } catch {
            case ex: IndexOutOfBoundsException =>
              sender() ! GetObservationsResult(op, Vector.empty, index2token(index))
          }
      }

    case op: ListPartitions =>
      sender() ! ListPartitionsResult(op, partitions.keys.toVector)

    case op: GetCheckpoint =>
      Option(checkpoints.get(op.partition)) match {
        case None =>
          sender() ! IngestServiceOperationFailed(op, ApiException(ResourceNotFound, s"no such partition ${op.partition}"))
        case Some(index) =>
          sender() ! GetCheckpointResult(op, index2token(index))
      }

    case op: PutCheckpoint =>
      Option(partitions.get(op.partition)) match {
        case None =>
          sender() ! IngestServiceOperationFailed(op, ApiException(ResourceNotFound, s"no such partition ${op.partition}"))
        case Some(partition) =>
          val index = token2index(op.token)
          if (index < 0 || index >= partition.length)
            sender() ! IngestServiceOperationFailed(op, ApiException(BadRequest, s"invalid token ${op.token}"))
          checkpoints.put(op.partition, index)
          sender() ! PutCheckpointResult(op)
      }
  }

  def token2index(token: String): Int = token.toInt

  def index2token(index: Int): String = index.toString
}

object TestObservationIngester {
  def props(settings: TestObservationIngesterSettings) = Props(classOf[TestObservationIngester], settings)
}

case class TestObservationIngesterSettings(numPartitions: Int)

class TestObservationIngesterExtension extends IngestExtension {
  type Settings = TestObservationIngesterSettings
  def configure(config: Config): Settings = {
    val numPartitions = config.getInt("num-partitions")
    TestObservationIngesterSettings(numPartitions)
  }
  def props(settings: Settings): Props = TestObservationIngester.props(settings)
}

