package com.redis.cluster

import java.util.zip.CRC32
import scala.collection.immutable.TreeSet
import scala.collection.mutable.{ArrayBuffer, Map, ListBuffer}

import com.redis._

/**
 * Consistent hashing distributes keys across multiple servers. But there are situations
 * like <i>sorting</i> or computing <i>set intersections</i> or operations like <tt>rpoplpush</tt>
 * in redis that require all keys to be collocated on the same server.
 * <p/>
 * One of the techniques that redis encourages for such forced key locality is called
 * <i>key tagging</i>. See <http://code.google.com/p/redis/wiki/FAQ> for reference.
 * <p/>
 * The trait <tt>KeyTag</tt> defines a method <tt>tag</tt> that takes a key and returns
 * the part of the key on which we hash to determine the server on which it will be located.
 * If it returns <tt>None</tt> then we hash on the whole key, otherwise we hash only on the
 * returned part.
 * <p/>
 * redis-rb implements a regex based trick to achieve key-tagging. Here is the technique
 * explained in redis FAQ:
 * <i>
 * A key tag is a special pattern inside a key that, if preset, is the only part of the key 
 * hashed in order to select the server for this key. For example in order to hash the key 
 * "foo" I simply perform the CRC32 checksum of the whole string, but if this key has a 
 * pattern in the form of the characters {...} I only hash this substring. So for example 
 * for the key "foo{bared}" the key hashing code will simply perform the CRC32 of "bared". 
 * This way using key tags you can ensure that related keys will be stored on the same Redis
 * instance just using the same key tag for all this keys. Redis-rb already implements key tags.
 * </i>
 */
trait KeyTag {
  def tag(key: String): Option[String]
}

import scala.util.matching.Regex
object RegexKeyTag extends KeyTag {
  def tag(key: String) = {
    val t = """\{(.*)?\}""".r
    t.findFirstIn(key)
  }
}

object NoOpKeyTag extends KeyTag {
  def tag(key: String) = Some(key)
}

abstract class RedisCluster(hosts: String*) extends Redis
  with NodeOperations
  with Operations
  with StringOperations
  with ListOperations 
  with SetOperations
  with SortedSetOperations {

  // not needed at cluster level
  val host = null
  val port = 0

  // abstract val
  val keyTag: Option[KeyTag]

  // default in libmemcached
  val POINTS_PER_SERVER = 160 // default in libmemcached

  // instantiating a cluster will automatically connect participating nodes to the server
  val clients = hosts.toList.map {h => 
    val hp = h.split(":")
    new RedisClient(hp(0), hp(1).toInt)
  }

  // the hash ring will instantiate with the nodes up and added
  val hr = HashRing[RedisClient](clients, POINTS_PER_SERVER)

  // get node for the key
  def nodeForKey(key: String) = {
    keyTag.get.tag(key) match {
      case Some(s) => hr.getNode(s)
      case None => hr.getNode(key)
    }
  }

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
      yield ks.get 
  }

  /**
   * Operations
   */
  override def keys(glob: String) = nodeKeys(glob) match {
    case List() => None
    case l => l.flatten[Option[String]] match {
      case List() => None
      case ks: List[Option[String]] => Some(ks)  
    }
  }

  override def flushdb = hr.cluster.map(_.flushdb).forall(_ == true)
  override def flushall = hr.cluster.map(_.flushall).forall(_ == true)
  override def quit = hr.cluster.map(_.quit).forall(_ == true)
  override def rename(oldkey: String, newkey: String): Boolean = nodeForKey(oldkey).rename(oldkey, newkey)
  override def renamenx(oldkey: String, newkey: String): Boolean = nodeForKey(oldkey).renamenx(oldkey, newkey)
  override def dbsize: Option[Int] = {
    hr.cluster.map(_.dbsize).foldLeft(0) { (a, b) =>
      b match {
        case Some(i) => a + i
        case None => a
      }
    } match {
      case 0 => None
      case i => Some(i)
    }
  }
  override def exists(key: String): Boolean = nodeForKey(key).exists(key)
  override def del(key: String, keys: String*): Option[Int] = {
    val ks = key :: keys.toList
    import scala.collection.immutable.Map

    // iterate over the Map and invoke delete on each entry
    node2KeysMap(ks).foldLeft(0) {(s, kv) =>
      kv._1.del(kv._2.head, kv._2.tail: _*) match {
        case Some(i) => s + i
        case None => s
      }
    } match {
      case 0 => None
      case i => Some(i)
    }
  }
  override def getType(key: String) = nodeForKey(key).getType(key)
  override def expire(key: String, expiry: Int) = nodeForKey(key).expire(key, expiry)
  override def select(index: Int) = throw new UnsupportedOperationException("not supported on a cluster")

  // make a Map containing RedisClient -> List(keys) which map to this client
  private def node2KeysMap(keys: List[String]) = {
    keys.foldLeft(Map[RedisClient, List[String]]()) {(s, k) => 
      val n = nodeForKey(k)
      s(n) = s.getOrElse(n, List()) ::: List(k)
      s
    }
  }

  /**
   * NodeOperations
   */
  override def save = hr.cluster.map(_.save).forall(_ == true)
  override def bgsave = hr.cluster.map(_.bgsave).forall(_ == true)
  override def shutdown = hr.cluster.map(_.shutdown).forall(_ == true)
  override def bgrewriteaof = hr.cluster.map(_.bgrewriteaof).forall(_ == true)
  override def lastsave = throw new UnsupportedOperationException("not supported on a cluster")
  override def monitor = throw new UnsupportedOperationException("not supported on a cluster")
  override def info = throw new UnsupportedOperationException("not supported on a cluster")
  override def slaveof(options: Any) = throw new UnsupportedOperationException("not supported on a cluster")
  override def move(key: String, db: Int) = throw new UnsupportedOperationException("not supported on a cluster")
  override def auth(secret: String) = throw new UnsupportedOperationException("not supported on a cluster")


  /**
   * StringOperations
   */
  override def set(key: String, value: String) = nodeForKey(key).set(key, value)
  override def get(key: String) = nodeForKey(key).get(key)
  override def getset(key: String, value: String) = nodeForKey(key).getset(key, value)
  override def setnx(key: String, value: String) = nodeForKey(key).setnx(key, value)
  override def incr(key: String) = nodeForKey(key).incr(key)
  override def incrby(key: String, increment: Int) = nodeForKey(key).incrby(key, increment)
  override def decr(key: String) = nodeForKey(key).decr(key)
  override def decrby(key: String, increment: Int) = nodeForKey(key).decrby(key, increment)

  override def mget(key: String, keys: String*) = {
    // get a Map of RedisClient -> Keys
    val m = node2KeysMap(key :: keys.toList)

    // get the mget result for each of the map entries : List[List[(String, Option[String])]]
    val pairs = for((node, k::ks) <- m) yield (k :: ks) zip node.mget(k, ks: _*).get  

    val n = scala.collection.mutable.Map.empty[String, Option[String]]

    // flatten out the List and form a Map of Key -> Option[String]
    // we need to return the values in order of the original keys : hence the trouble
    pairs.foreach(e => e.foreach(f => n += f))

    // iterate the original list and fetch the values from the Map
    (key :: keys.toList).map(k => n(k)) match {
      case List() => None
      case x => Some(x)
    }
  }

  override def mset(kvs: (String, String)*) = kvs.toList.map{ case (k, v) => set(k, v) }.forall(_ == true)
  override def msetnx(kvs: (String, String)*) = kvs.toList.map{ case (k, v) => setnx(k, v) }.forall(_ == true)

  /**
   * ListOperations
   */
  override def lpush(key: String, value: String) = nodeForKey(key).lpush(key, value)
  override def rpush(key: String, value: String) = nodeForKey(key).rpush(key, value)
  override def llen(key: String) = nodeForKey(key).llen(key)
  override def lrange(key: String, start: Int, end: Int) = nodeForKey(key).lrange(key, start, end)
  override def ltrim(key: String, start: Int, end: Int) = nodeForKey(key).ltrim(key, start, end)
  override def lindex(key: String, index: Int) = nodeForKey(key).lindex(key, index)
  override def lset(key: String, index: Int, value: String) = nodeForKey(key).lset(key, index, value)
  override def lrem(key: String, count: Int, value: String) = nodeForKey(key).lrem(key, count, value)
  override def lpop(key: String) = nodeForKey(key).lpop(key)
  override def rpop(key: String) = nodeForKey(key).rpop(key)
  override def rpoplpush(srcKey: String, dstKey: String) = 
    inSameNode(srcKey, dstKey) {n => n.rpoplpush(srcKey, dstKey)}

  private def inSameNode[T](keys: String*)(body: RedisClient => T): T = {
    val nodes = keys.toList.map(nodeForKey(_))
    nodes.forall(_ == nodes.head) match {
      case true => body(nodes.head)  // all nodes equal
      case _ => 
        throw new UnsupportedOperationException("can only occur if both keys map to same node")
    }
  }

  /**
   * SetOperations
   */
  override def sadd(key: String, value: String) = nodeForKey(key).sadd(key, value)
  override def srem(key: String, value: String) = nodeForKey(key).srem(key, value)
  override def spop(key: String) = nodeForKey(key).spop(key)

  override def smove(sourceKey: String, destKey: String, value: String) = 
    inSameNode(sourceKey, destKey) {n => n.smove(sourceKey, destKey, value)}

  override def scard(key: String) = nodeForKey(key).scard(key)
  override def sismember(key: String, value: String) = nodeForKey(key).sismember(key, value)

  override def sinter(key: String, keys: String*) = 
    inSameNode((key :: keys.toList): _*) {n => n.sinter(key, keys: _*)}

  override def sinterstore(key: String, keys: String*) = 
    inSameNode((key :: keys.toList): _*) {n => n.sinterstore(key, keys: _*)}

  override def sunion(key: String, keys: String*) = 
    inSameNode((key :: keys.toList): _*) {n => n.sunion(key, keys: _*)}

  override def sunionstore(key: String, keys: String*) = 
    inSameNode((key :: keys.toList): _*) {n => n.sunionstore(key, keys: _*)}

  override def sdiff(key: String, keys: String*) = 
    inSameNode((key :: keys.toList): _*) {n => n.sdiff(key, keys: _*)}

  override def sdiffstore(key: String, keys: String*) = 
    inSameNode((key :: keys.toList): _*) {n => n.sdiffstore(key, keys: _*)}

  override def smembers(key: String) = nodeForKey(key).smembers(key)
  override def srandmember(key: String)  = nodeForKey(key).srandmember(key)


  import Commands._
  import RedisClient._

  /**
   * SortedSetOperations
   */
  override def zadd(key: String, score: String, member: String) = nodeForKey(key).zadd(key, score, member)
  override def zrem(key: String, member: String) = nodeForKey(key).zrem(key, member)
  override def zincrby(key: String, incr: String, member: String) = nodeForKey(key).zincrby(key, incr, member)
  override def zcard(key: String) = nodeForKey(key).zcard(key)
  override def zscore(key: String, element: String) = nodeForKey(key).zscore(key, element)
  override def zrange(key: String, start: String, end: String, sortAs: SortOrder, withScores: Boolean ) = 
    nodeForKey(key).zrange(key, start, end, sortAs, withScores)
  override def zrangeWithScore(key: String, start: String, end: String, sortAs: SortOrder) =
    nodeForKey(key).zrangeWithScore(key, start, end, sortAs)
  override def zrangebyscore(key: String, min: String, max: String, limit: Option[(String, String)]) =
    nodeForKey(key).zrangebyscore(key, min, max, limit)
}
