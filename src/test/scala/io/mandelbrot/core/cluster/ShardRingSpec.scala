package io.mandelbrot.core.cluster

import akka.actor.Address
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class ShardRingSpec extends WordSpec with ShouldMatchers {

  "A ShardRing" should {

    val addresses = Set(
      Address("akka", "actor-system", "localhost", 1024),
      Address("akka", "actor-system", "localhost", 1025),
      Address("akka", "actor-system", "localhost", 1026),
      Address("akka", "actor-system", "localhost", 1027),
      Address("akka", "actor-system", "localhost", 1028)
    )

    "initialize empty" in {
      val shardRing = ShardRing()
      shardRing.shards.toVector should be === Vector()
    }

    "initialize with cluster and number of shards specified" in {
      val shardRing = ShardRing(addresses, 64)
      shardRing.size should be(64)
    }

  }
}
