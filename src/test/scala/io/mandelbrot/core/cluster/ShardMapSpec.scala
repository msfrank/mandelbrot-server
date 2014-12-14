package io.mandelbrot.core.cluster

import akka.actor.Address
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class ShardMapSpec extends WordSpec with ShouldMatchers {

  "A ShardMap" should {

    val address1 = Address("akka", "actor-system", "localhost", 1024)
    val address2 = Address("akka", "actor-system", "localhost", 1025)
    val address3 = Address("akka", "actor-system", "localhost", 1026)
    val address4 = Address("akka", "actor-system", "localhost", 1027)
    val address5 = Address("akka", "actor-system", "localhost", 1028)

    val addresses = Set(address1, address2, address3, address4, address5)

    "initialize empty" in {
      val shardMap = ShardMap(4, 1)
      shardMap.nonEmpty shouldEqual false
      shardMap.isEmpty shouldEqual true
    }

    "map keys to shards" in {
      val shardMap = ShardMap(8, 2)
      shardMap.put(0, address1)
      shardMap.put(2, address2)
      shardMap.put(4, address3)
      shardMap.put(6, address4)
      shardMap(0) shouldEqual Some((0, address1))
      shardMap(1) shouldEqual Some((0, address1))
      shardMap(2) shouldEqual Some((1, address2))
      shardMap(3) shouldEqual Some((1, address2))
      shardMap(4) shouldEqual Some((2, address3))
      shardMap(5) shouldEqual Some((2, address3))
      shardMap(6) shouldEqual Some((3, address4))
      shardMap(7) shouldEqual Some((3, address4))
    }

    "not map a key to a shard if the ring is empty" in {
      val shardMap = ShardMap(4, 1)
      shardMap(10) should be(None)
    }

    "detect if it is full" in {
      val shardMap = ShardMap(4, 1)
      0.until(4).foreach(shardMap.put(_, address1))
      shardMap.nonFull shouldEqual false
      shardMap.isFull shouldEqual true
    }

    "detect if it is not full when empty" in {
      val shardMap = ShardMap(4, 1)
      shardMap.nonFull shouldEqual true
      shardMap.isFull shouldEqual false
    }

    "detect if it is not full when missing an entry" in {
      val shardMap = ShardMap(2, 1)
      shardMap.put(0, address1)
      shardMap.nonFull shouldEqual true
      shardMap.isFull shouldEqual false
    }

    "iterate missing shards in an empty map" in {
      val shardMap = ShardMap(4, 1)
      shardMap.missing shouldEqual Set(0, 1, 2, 3)
      shardMap.put(0, address1)
      shardMap.put(2, address2)
      shardMap.missing shouldEqual Set(1, 3)
    }
  }
}
