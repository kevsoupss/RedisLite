#include "server.h"
#include <iostream>
#include <vector>
#include <stdexcept>

#pragma comment(lib, "Ws2_32.lib")

Server::Server(unsigned short port, CommandHandler handler)
    : port_(port), handler_(std::move(handler)), listenSocket_(INVALID_SOCKET), running_(false) {}

static RespValue makeProtocolError(const std::string& msg) {
    RespValue err;
    err.respType = RespType::ERR;
    err.value = std::string("ERR " + msg);
    return err;
}

bool Server::start() {
    WSADATA wsaData;
    int res = WSAStartup(MAKEWORD(2,2), &wsaData);
    if (res != 0) {
        std::cerr << "WSAStartup failed: " << res << "\n";
        return false;
    }

    listenSocket_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSocket_ == INVALID_SOCKET) {
        std::cerr << "socket() failed: " << WSAGetLastError() << "\n";
        WSACleanup();
        return false;
    }


    int opt = 1;
    setsockopt(listenSocket_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    sockaddr_in service{};
    service.sin_family = AF_INET;
    service.sin_addr.s_addr = INADDR_ANY;
    service.sin_port = htons(port_);

    if (bind(listenSocket_, (sockaddr*)&service, sizeof(service)) == SOCKET_ERROR) {
        std::cerr << "bind() failed: " << WSAGetLastError() << "\n";
        closesocket(listenSocket_);
        WSACleanup();
        return false;
    }

    if (listen(listenSocket_, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "listen() failed: " << WSAGetLastError() << "\n";
        closesocket(listenSocket_);
        WSACleanup();
        return false;
    }

    std::cout << "RedisLite listening on port " << port_ << "...\n";
    running_ = true;

    // Single thread
    while (running_) {
        SOCKET clientSock = accept(listenSocket_, nullptr, nullptr);
        if (clientSock == INVALID_SOCKET) {
            std::cerr << "accept() failed: " << WSAGetLastError() << "\n";
            continue;
        }
        std::cout << "Client connected.\n";


        std::string readBuffer;
        const int BUFFER_SZ = 4096;
        std::vector<char> tmp(BUFFER_SZ);


        while (true) {
            int bytes = recv(clientSock, tmp.data(), BUFFER_SZ, 0);
            if (bytes == 0) {
                std::cout << "Client closed connection.\n";
                closesocket(clientSock);
                break;
            } else if (bytes < 0) {
                std::cerr << "recv() error: " << WSAGetLastError() << "\n";
                closesocket(clientSock);
                break;
            }

            readBuffer.append(tmp.data(), bytes);

            size_t pos = 0;
            while (pos < readBuffer.size()) {
                try {
                    RespValue req = parseValue(readBuffer, pos);

                    // Reply from handler
                    RespValue reply = handler_(req);

                    // Serialize and send back
                    std::string out = serialize(reply);
                    int sent = send(clientSock, out.c_str(), (int)out.size(), 0);
                    if (sent == SOCKET_ERROR) {
                        std::cerr << "send() failed: " << WSAGetLastError() << "\n";
                        break;
                    }
                } catch (const std::runtime_error& e) {
                    std::string what = e.what();

                    bool likelyIncomplete = false;
                    if (what.find("No CRLF") != std::string::npos ||
                        what.find("Bulk string too short") != std::string::npos ||
                        what.find("Missing CRLF") != std::string::npos) {
                        likelyIncomplete = true;
                    }

                    if (likelyIncomplete) {
                        break;
                    } else {
                        std::cerr << "Protocol error while parsing: " << what << "\n";
                        RespValue err = makeProtocolError(what);
                        std::string out = serialize(err);
                        send(clientSock, out.c_str(), (int)out.size(), 0);

                        readBuffer.clear();
                        pos = 0;
                        break;
                    }
                }
            }

            if (pos > 0) {
                readBuffer.erase(0, pos);
            }

        }
    }

    // Cleanup
    closesocket(listenSocket_);
    WSACleanup();
    return true;
}

void Server::stop() {
    running_ = false;
    if (listenSocket_ != INVALID_SOCKET) {
        closesocket(listenSocket_);
        listenSocket_ = INVALID_SOCKET;
    }
    WSACleanup();
}