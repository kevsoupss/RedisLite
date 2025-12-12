#ifndef CACHE_SERVER_H
#define CACHE_SERVER_H

#include <winsock2.h>
#include <ws2tcpip.h>
#include <functional>
#include <string>
#include "parser.h"
#include "serializer.h"
#include "resp.h"

class Server {
public:
    using CommandHandler = std::function<RespValue(const RespValue&)>;
    Server(unsigned short port, CommandHandler handler);

    bool start();
    void stop();

private:
    unsigned short port_;
    CommandHandler handler_;

    SOCKET listenSocket_;
    bool running_;
};


#endif //CACHE_SERVER_H