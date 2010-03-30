package com.redis.cluster

import java.util.zip.CRC32
import scala.collection.immutable.TreeSet
import scala.collection.mutable.{ArrayBuffer, Map, ListBuffer}

import com.redis.RedisClient

class RedisCluster(val hosts: String*) {

  // not needed at cluster level
  val host = null
  val port = 0

  val POINTS_PER_SERVER = 160 // default in libmemcached

  // instantiating a cluster will automatically connect participating nodes to the server
  val clients = hosts.toList.map {host => 
    val hp = host.split(":")
    new RedisClient(hp(0), hp(1).toInt)
  }

  // the hash ring will instantiate with the nodes up and added
  val hr = HashRing[RedisClient](clients, POINTS_PER_SERVER)

  // get node for the key
  def nodeForKey(key: String) = hr.getNode(key)

  // add a server
  def addServer(server: String) = {
    val hp = server.split(":")
    hr addNode new RedisClient(hp(0), hp(1).toInt)
  }

  // collect all keys from nodes
  def nodeKeys(glob: String) = {
    val rs = hr.cluster.toList
    for(r <- rs;
        val ks = r.keys(glob) if ks.isDefined)
      yield ks.get.toList
  }

  def keys(glob: String) = nodeKeys(glob).flatten[String]

  def save = hr.cluster.map(_.save).forall(_ == true)
  def bgsave = hr.cluster.map(_.bgsave).forall(_ == true)
  def flushdb = hr.cluster.map(_.flushdb).forall(_ == true)
  def flushall = hr.cluster.map(_.flushall).forall(_ == true)
  def quit = hr.cluster.map(_.quit).forall(_ == true)

  def set(key: String, value: String): Boolean = 
    nodeForKey(key).set(key, value)
}
