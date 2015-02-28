package io.mandelbrot.core.entity

import akka.actor.Address
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

class ShardMapSpec extends WordSpec with ShouldMatchers {

  "A ShardMap" should {

    val address1 = Address("akka", "actor-system", "localhost", 1024)
    val address2 = Address("akka", "actor-system", "localhost", 1025)
    val address3 = Address("akka", "actor-system", "localhost", 1026)
    val address4 = Address("akka", "actor-system", "localhost", 1027)
    val address5 = Address("akka", "actor-system", "localhost", 1028)

    val addresses = Set(address1, address2, address3, address4, address5)

    "initialize empty" in {
      val shardMap = ShardMap(4)
      shardMap.nonEmpty shouldEqual false
      shardMap.isEmpty shouldEqual true
    }

    "map keys to shards" in {
      val shardMap = ShardMap(4)
      shardMap.assign(0, address1)
      shardMap.assign(1, address2)
      shardMap.assign(2, address3)
      shardMap.assign(3, address4)
      shardMap(0) shouldEqual AssignedShardEntry(0, address1)
      shardMap(1) shouldEqual AssignedShardEntry(1, address2)
      shardMap(2) shouldEqual AssignedShardEntry(2, address3)
      shardMap(3) shouldEqual AssignedShardEntry(3, address4)
    }

    "not map a key to a shard if the ring is empty" in {
      val shardMap = ShardMap(4)
      shardMap(10) shouldEqual MissingShardEntry(2)
    }

    "detect if it is full" in {
      val shardMap = ShardMap(4)
      0.until(4).foreach(shardMap.assign(_, address1))
      shardMap.nonFull shouldEqual false
      shardMap.isFull shouldEqual true
    }

    "detect if it is not full when empty" in {
      val shardMap = ShardMap(4)
      shardMap.nonFull shouldEqual true
      shardMap.isFull shouldEqual false
    }

    "detect if it is not full when missing an entry" in {
      val shardMap = ShardMap(2)
      shardMap.assign(0, address1)
      shardMap.nonFull shouldEqual true
      shardMap.isFull shouldEqual false
    }

    "iterate missing shards in an empty map" in {
      val shardMap = ShardMap(4)
      shardMap.missing.toSet shouldEqual Set(MissingShardEntry(0), MissingShardEntry(1), MissingShardEntry(2), MissingShardEntry(3))
      shardMap.assign(0, address1)
      shardMap.assign(2, address2)
      shardMap.missing.toSet shouldEqual Set(MissingShardEntry(1), MissingShardEntry(3))
    }
  }
}
