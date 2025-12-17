# RedisLite 

Attempting to build a lite Redis Server in C++ with the same operations and support of the original Redis.

## Current Functionality
- Implements the Redis Serialization Protocol (RESP) for I/O
- Supports core "GET" and "SET" commands
- Stores data in `unordered_map, no persistence on shutdown
- Handles concurrency using a single-threaded event loop

## Current Redis Benchmarking
- using `redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 100000 -q`
- SET: 74682.60 requests per second
- GET: 85910.65 requests per second

## Working towards
- "SET" expiration and other options
- Persistence on shutdown
- Redis "EXIST", "DEL", "SAVE" commands
- Using Redis Benchmark to performance test server

