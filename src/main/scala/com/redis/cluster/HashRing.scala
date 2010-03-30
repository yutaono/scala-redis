package com.redis.cluster

import java.util.zip.CRC32
import scala.collection.immutable.TreeSet
import scala.collection.mutable.{ArrayBuffer, Map, ListBuffer}

case class HashRing[T](nodes: List[T], replicas: Int) {
  var sortedKeys = new TreeSet[Long]
  val cluster = new ArrayBuffer[T]
  var ring = Map[Long, T]()

  nodes.foreach(addNode(_))

  // adds a node to the hash ring (including a number of replicas)
  def addNode(node: T) = {
    cluster += node
    (1 to replicas).foreach {replica =>
      val key = calculateChecksum(node + ":" + replica)
      ring += (key -> node)
      sortedKeys = sortedKeys + key
    }
  }

  // remove node from the ring
  def removeNode(node: T) {
    cluster -= node
    (1 to replicas).foreach {replica =>
      val key = calculateChecksum(node + ":" + replica)
      ring -= key
      sortedKeys = sortedKeys - key
    }
  }

  // get node for the key
  def getNode(key: String): T = {
    val crc = calculateChecksum(key)
    if (sortedKeys contains crc) ring(crc)
    else ring(sortedKeys.rangeImpl(None, Some(crc)).lastKey)
  }

  // Computes the CRC-32 of the given String
  def calculateChecksum(value: String): Long = {
    val checksum = new CRC32
    checksum.update(value.getBytes)
    checksum.getValue
  }
}

