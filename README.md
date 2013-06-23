# Redis Scala client (Non blocking based on Akka IO)

## Key features of the library

- Native Scala types Set and List responses.
- Transparent serialization
- Non blocking
- Composable with Futures

## Information about redis

Redis is a key-value database. It is similar to memcached but the dataset is not volatile, and values can be strings, exactly like in memcached, but also lists and sets with atomic operations to push/pop elements.

http://redis.io

### Key features of Redis

- Fast in-memory store with asynchronous save to disk.
- Key value get, set, delete, etc.
- Atomic operations on sets and lists, union, intersection, trim, etc.

## Requirements

- sbt (get it at http://code.google.com/p/simple-build-tool/)

## Sample usage

```scala
val kvs = (1 to 10).map(i => s"key_$i").zip(1 to 10)
val setResults = kvs map {case (k, v) =>
  set(k, v) apply client
}
val sr = Future.sequence(setResults)

Await.result(sr.map(_.flatten), 2 seconds).forall(_ == true) should equal(true)

val ks = (1 to 10).map(i => s"key_$i")
val getResults = ks.map {k =>
  get[Long](k) apply client
}

val gr = Future.sequence(getResults)
val result = gr.map(_.flatten.sum)

Await.result(result, 2 seconds) should equal(55)
```

```scala
val values = (1 to 100).toList
val pushResult = lpush("key", 0, values:_*) apply client
val getResult = lrange[Long]("key", 0, -1) apply client
      
val res = for {
  p <- pushResult.mapTo[Option[Long]]
  if p.get > 0
  r <- getResult.mapTo[Option[List[Long]]]
} yield (p, r)

val (count, list) = Await.result(res, 2 seconds)
count should equal(Some(101))
list.get.reverse should equal((0 to 100).map(a => Some(a)))
```
