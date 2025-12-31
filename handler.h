//
// Created by kevin on 12/11/2025.
//

#ifndef CACHE_HANDLER_H
#define CACHE_HANDLER_H
#include <cstdint>
#include <functional>

#include "resp.h"
#include "logger.cpp"

class Handler {
public:
    RespValue handler(const RespValue& req);
    Handler(AofLogger& logger): logger_(logger) {};
    void recover(const std::string& filename);

private:
    struct DataEntry {
        RespValue value;
        std::optional<uint64_t> expireAt;
    };

    using CommandFunction = RespValue (Handler::*)(const std::vector<RespValue>&);
    static std::unordered_map<std::string, CommandFunction> commandMap_;
    static std::unordered_map<std::string, DataEntry> dataStore_;
    AofLogger& logger_;

    RespValue routeCommand(std::string &commandName, const std::vector<RespValue> &array);
    RespValue handleGet(const std::vector<RespValue> &array);
    RespValue handleSet(const std::vector<RespValue> &array);
    RespValue handleExists(const std::vector<RespValue> &array);
    RespValue handleDel(const std::vector<RespValue> &array);
    RespValue handleFlush(const std::vector<RespValue> &array);
    std::vector<RespValue> parseAofLine(const std::string& line);

    void internalSet(const std::string& key, const RespValue& value, uint64_t expireAt);
    void internalClear();

};


#endif //CACHE_HANDLER_H