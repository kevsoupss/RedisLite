#include "server.h"
#include "serializer.h"
#include "parser.h"
#include <iostream>
#include "handler.h"

int main() {
    std::string filename = "cache.aof";

    AofLogger logger = AofLogger(filename);
    Handler commandHandler(logger);

    // Aof file persistence
    commandHandler.recover(filename);

    Server server(6379, [&commandHandler](const auto& args) {
        return commandHandler.handler(args);
    });

    if (!server.start()) {
        std::cerr << "Failed to start RedisLite server\n";
        return 1;
    }

    std::cout << "Server stopped.\n";
    return 0;
}
