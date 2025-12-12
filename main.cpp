#include "server.h"
#include "serializer.h"
#include "parser.h"
#include <iostream>

RespValue simpleHandler(const RespValue& req);

int main() {
    Server server(6379, simpleHandler);

    if (!server.start()) {
        std::cerr << "Failed to start RedisLite server\n";
        return 1;
    }

    std::cout << "Server stopped.\n";
    return 0;
}
