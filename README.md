# RedisLite 

Attempting to build a lite Redis Server in C++ with the same operations and support of the original Redis.

## Current Functionality
- Implements the Redis Serialization Protocol (RESP) for I/O
- Supports core "GET" and "SET" commands
- Stores data in `unordered_map, no persistence on shutdown

## Working towards
- Handling concurrent clients
- "SET" expiration and other options
- Persistence on shutdown
- Redis "EXIST", "DEL", "SAVE" commands
- Using Redis Benchmark to performance test server
