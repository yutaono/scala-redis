# Redis Scala client

## Key features of the library

- Native Scala types Set and List responses.
- Transparent serialization
- Connection pooling
- Consistent Hashing on the client.
- Support for Clustering of Redis nodes.

## Information about redis

Redis is a key-value database. It is similar to memcached but the dataset is not volatile, and values can be strings, exactly like in memcached, but also lists and sets with atomic operations to push/pop elements.

http://redis.io

### Key features of Redis

- Fast in-memory store with asynchronous save to disk.
- Key value get, set, delete, etc.
- Atomic operations on sets and lists, union, intersection, trim, etc.

## Requirements

- sbt (get it at http://code.google.com/p/simple-build-tool/)

## Usage

Start your redis instance (usually redis-server will do it)

    $ cd scala-redis
    $ sbt
    > update
    > console

And you are ready to start issuing commands to the server(s)

Redis 2 implements a new protocol for binary safe commands and replies

let us connect and get a key:

    scala> import com.redis._
    import com.redis._

    scala> val r = new RedisClient("localhost", 6379)
    r: com.redis.RedisClient = localhost:6379

    scala> r.set("key", "some value")
    res3: Boolean = true

    scala> r.get("key")
    res4: Option[String] = Some(some value)

Let us try out some List operations:

    scala> r.lpush("list-1", "foo")
    res0: Option[Int] = Some(1)

    scala> r.rpush("list-1", "bar")
    res1: Option[Int] = Some(2)

    scala> r.llen("list-1")
    res2: Option[Int] = Some(2)


## License

This software is licensed under the Apache 2 license, quoted below.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

