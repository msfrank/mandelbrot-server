package io.mandelbrot.core.cluster

import akka.actor.Address
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class ShardRingSpec extends WordSpec with ShouldMatchers {

  "A ShardRing" should {

    val address1 = Address("akka", "actor-system", "localhost", 1024)
    val address2 = Address("akka", "actor-system", "localhost", 1025)
    val address3 = Address("akka", "actor-system", "localhost", 1026)
    val address4 = Address("akka", "actor-system", "localhost", 1027)
    val address5 = Address("akka", "actor-system", "localhost", 1028)

    val addresses = Set(address1, address2, address3, address4, address5)

    "initialize empty" in {
      val shardRing = ShardRing()
      shardRing.nonEmpty should be(false)
      shardRing.isEmpty should be(true)
    }

    "map keys to shards" in {
      val shardRing = ShardRing()
      shardRing.put(10, 10, address1)
      shardRing.put(20, 10, address2)
      shardRing.put(30, 10, address3)
      shardRing(0) shouldEqual None
      shardRing(10) shouldEqual Some((10, address1))
      shardRing(15) shouldEqual Some((10, address1))
      shardRing(20) shouldEqual Some((20, address2))
      shardRing(25) shouldEqual Some((20, address2))
      shardRing(30) shouldEqual Some((30, address3))
      shardRing(35) shouldEqual Some((30, address3))
      shardRing(40) shouldEqual None
    }

    "not map a key to a shard if the ring is empty" in {
      val shardRing = ShardRing()
      shardRing(10) should be(None)
    }
  }
}
