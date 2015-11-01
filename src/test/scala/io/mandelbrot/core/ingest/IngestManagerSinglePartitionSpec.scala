package io.mandelbrot.core.ingest

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}

import io.mandelbrot.core._
import io.mandelbrot.core.model.{Timestamp, ScalarMapObservation, ProbeRef}
import io.mandelbrot.core.model.Conversions._
import io.mandelbrot.core.ConfigConversions._

class IngestManagerSinglePartitionSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("IngestManagerSinglePartitionSpec", AkkaConfig ++ MandelbrotConfig + ConfigFactory.parseString(
    """
      |mandelbrot.ingest.plugin-settings.num-partitions = 1
    """.stripMargin)))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val settings = ServerConfig(system).settings

  def withIngestService(testCode: (ActorRef) => Any) {
    val ingestService = system.actorOf(IngestManager.props(settings.ingest, settings.cluster.enabled))
    testCode(ingestService)
    ingestService ! PoisonPill
  }

  val probeRef = ProbeRef("foo.agent:system.load")

  "An IngestManager backed with a single partition" when {

    "servicing an AppendObservation request" should {

      "append an observation" in withIngestService { ingestService =>
        val observation = ScalarMapObservation(probeRef.probeId, DateTime.now(DateTimeZone.UTC),
          Map("agentId" -> probeRef.agentId.toString), Map("load1" -> 1.metricUnits))
        val timestamp = Timestamp()
        ingestService ! AppendObservation(probeRef, timestamp, observation)
        expectMsgClass(classOf[AppendObservationResult])
      }
    }

    "servicing a GetObservations request" should {

      val timestamp = Timestamp()
      val observation1 = ScalarMapObservation(timestamp.toDateTime.minusMinutes(5), Map("load1" -> 5.0))
      val observation2 = ScalarMapObservation(timestamp.toDateTime.minusMinutes(4), Map("load1" -> 4.0))
      val observation3 = ScalarMapObservation(timestamp.toDateTime.minusMinutes(3), Map("load1" -> 3.0))
      val observation4 = ScalarMapObservation(timestamp.toDateTime.minusMinutes(2), Map("load1" -> 2.0))
      val observation5 = ScalarMapObservation(timestamp.toDateTime.minusMinutes(1), Map("load1" -> 1.0))

      "retrieve observations from the beginning when no token is specified" in withIngestService { ingestService =>
        // append observations to a single partition
        ingestService ! AppendObservation(probeRef, timestamp, observation1)
        expectMsgClass(classOf[AppendObservationResult])
        ingestService ! AppendObservation(probeRef, timestamp, observation2)
        expectMsgClass(classOf[AppendObservationResult])
        ingestService ! AppendObservation(probeRef, timestamp, observation3)
        expectMsgClass(classOf[AppendObservationResult])
        ingestService ! AppendObservation(probeRef, timestamp, observation4)
        expectMsgClass(classOf[AppendObservationResult])
        ingestService ! AppendObservation(probeRef, timestamp, observation5)
        expectMsgClass(classOf[AppendObservationResult])

        ingestService ! ListPartitions()
        val listPartitionsResult = expectMsgClass(classOf[ListPartitionsResult])
        listPartitionsResult.partitions.length shouldEqual 1

        // get observations
        ingestService ! GetObservations(listPartitionsResult.partitions.head, 100, None)
        val getObservationsResult = expectMsgClass(classOf[GetObservationsResult])
        getObservationsResult.observations shouldEqual Vector(observation1, observation2,
          observation3, observation4, observation5)
      }
    }

    "servicing a ListPartitions request" should {

      "list partitions" in withIngestService { ingestService =>
        ingestService ! ListPartitions()
        val listPartitionsResult = expectMsgClass(classOf[ListPartitionsResult])
        listPartitionsResult.partitions.length shouldEqual 1
      }
    }

    "servicing a GetCheckpoint request" should {

      "retrieve a checkpoint" in withIngestService { ingestService =>
        ingestService ! ListPartitions()
        val listPartitionsResult = expectMsgClass(classOf[ListPartitionsResult])
        ingestService ! GetCheckpoint(listPartitionsResult.partitions.head)
        expectMsgClass(classOf[GetCheckpointResult])
      }

      "return ResourceNotFound when the partition doesn't exist" in withIngestService { ingestService =>
        ingestService ! GetCheckpoint("doesntexist")
        val failure = expectMsgClass(classOf[IngestServiceOperationFailed])
        failure.failure shouldEqual ApiException(ResourceNotFound)
      }
    }

    "servicing a PutCheckpoint request" should {

    }
  }
}
