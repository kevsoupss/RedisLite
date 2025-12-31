# RedisLite 

Building a lite Redis Server in C++ with the same operations and support of the original Redis. 

You can connect to this server using the official Redis CLI.
```
redis-cli -p 6379
127.0.0.1:6379> SET user_1 "Kevin"
OK
127.0.0.1:6379> GET user_1
"Kevin"
```

## Current Functionality
- Implements the Redis Serialization Protocol (RESP) for I/O
- Supports core `GET` and `SET` commands along with `EXIST`, `DEL`, `FLUSHALL`
- Stores data in `unordered_map` for O(1) access
- Handles concurrency using a single-threaded event loop using `WSAPoll`, mirroring the core design of Redis
- Expiration/TTL functionality with lazy deletion
- Persistence with Append-Only File (AOF)

## Current Redis Benchmarking
- using `redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 100000 -q`
- SET: 74682.60 requests per second
- GET: 85910.65 requests per second

## Working towards
- Redis "SAVE" commands
- Using Redis Benchmark to performance test server

