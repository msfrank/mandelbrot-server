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
      shardRing.shards.toVector should be === Vector()
    }

    "map keys to shards" in {
      val shardRing = ShardRing()
      shardRing.put(10, address1)
      shardRing.put(20, address2)
      shardRing.put(30, address3)
      shardRing(0) should be === Some(address3)
      shardRing(10) should be === Some(address1)
      shardRing(15) should be === Some(address1)
      shardRing(20) should be === Some(address2)
      shardRing(25) should be === Some(address2)
      shardRing(30) should be === Some(address3)
      shardRing(35) should be === Some(address3)
    }

    "not map a key to a shard if the ring is empty" in {
      val shardRing = ShardRing()
      shardRing(10) should be(None)
    }
  }
}
