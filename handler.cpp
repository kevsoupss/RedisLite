#include "handler.h"
#include "resp.h"
#include <algorithm>
#include <chrono>
#include <iostream>

// Command Mapper
std::unordered_map<std::string, Handler::CommandFunction> Handler::commandMap_ = {
    {"GET",    &Handler::handleGet},
    {"SET",    &Handler::handleSet},
    {"EXISTS", &Handler::handleExists},
    {"DEL",    &Handler::handleDel},
    {"FLUSHALL", &Handler::handleFlush}
};

std::unordered_map<std::string, Handler::DataEntry> Handler::dataStore_ {};

uint64_t getNowMS() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

RespValue Handler::handler(const RespValue &req) {
    if (req.respType != RespType::ARRAY) {
        return RespValue::makeProtocolError("Request must be an array of bulk strings.");
    }

    using ArrayOpt = std::optional<std::vector<RespValue>>;
    if (const ArrayOpt* opt_ptr = std::get_if<ArrayOpt>(&req.value)) {

        if (opt_ptr->has_value()) {

            const std::vector<RespValue> &array = opt_ptr->value();
            if (array.empty()) {
                return RespValue::makeProtocolError("Command array cannot be empty.");
            }

            const RespValue commandValue = array[0];
            if (commandValue.respType != RespType::BULK_STRING) {
                return RespValue::makeProtocolError("Command must be a bulk string.");
            }

            using StringOpt = std::optional<std::string>;
            const StringOpt* commandOptPtr = std::get_if<StringOpt>(&commandValue.value);

            if (!commandOptPtr || !commandOptPtr->has_value()) {
                return RespValue::makeProtocolError("Internal error: Command bulk string empty.");
            }

            std::string commandName = commandOptPtr->value();

            std::transform(commandName.begin(), commandName.end(), commandName.begin(),
                           [](unsigned char c){ return std::toupper(c); });

            return this->routeCommand(commandName, array);
        } else {
            return RespValue::makeProtocolError("Array is null or empty.");
        }
    } else {
        return RespValue::makeProtocolError("Internal error: Array value misconfigured.");
    }
}

RespValue Handler::routeCommand(std::string &commandName, const std::vector<RespValue> &array) {
    auto it = commandMap_.find(commandName);

    if (it != commandMap_.end()) {
        CommandFunction func = it->second;
        return (this->*func)(array);
    } else {
        return RespValue::makeProtocolError(commandName + " does not exist");
    }
}

RespValue Handler::handleGet(const std::vector<RespValue> &array) {
    if (array.size() < 2) {
        return RespValue::makeProtocolError("Wrong number of arguments for 'GET' command");
    }
    const std::string& key = array[1].getString();

    auto it = dataStore_.find(key);
    if (it == dataStore_.end()) {
        return RespValue::makeNullBulkString();
    }

    if (it->second.expireAt != -1 && getNowMS() > it ->second.expireAt) {
        dataStore_.erase(it);
        return RespValue::makeNullBulkString();
    }

    return it->second.value;
}

RespValue Handler::handleSet(const std::vector<RespValue> &array) {
    if (array.size() < 3) {
        return RespValue::makeProtocolError("Wrong number of arguments for 'SET' command");
    }
    const std::string& key = array[1].getString();
    const RespValue& value = array[2];
    uint64_t expireAt = -1;

    if (array.size() >= 5) {
        std::string option = array[3].getString();
        std::transform(option.begin(), option.end(), option.begin(), ::toupper);

        if (option == "PX") {
            expireAt = getNowMS() + std::stoll(array[4].getString());
        } else if (option == "EX") {
            expireAt = getNowMS() + (std::stoll(array[4].getString())* 1000);
        }else if (option == "PXAT") {
            expireAt = std::stoll(array[4].getString());
        } else if (option == "EXAT") {
            expireAt = std::stoll(array[4].getString()) * 1000;
        }
    }

    dataStore_[key] = {value, expireAt};

    if (expireAt > 0) {
        std::string aofCmd = "SET " + key + " " + value.getString() + " " + std::to_string(expireAt);
        logger_.append(aofCmd);
    } else {
        std::string aofCmd = "SET " + key + " " + value.getString();
        logger_.append(aofCmd);
    }

    return RespValue::makeSimpleString("OK");
}

RespValue Handler::handleExists(const std::vector<RespValue> &array) {
    if (array.size() < 2) {
        return RespValue::makeProtocolError("Wrong number of arguments for 'EXISTS' command");
    }

    const std::string& key = array[1].getString();

    auto it = dataStore_.find(key);

    if (it == dataStore_.end()) {
        return RespValue::makeLongLong(0);
    }

    if (it->second.expireAt != -1 && getNowMS() > it ->second.expireAt) {
        dataStore_.erase(it);
        return RespValue::makeLongLong(0);
    }

    return RespValue::makeLongLong(1);

}

RespValue Handler::handleDel(const std::vector<RespValue> &array) {
    int deletedCount = 0;
    for (int i = 1; i < array.size(); i++) {
        const std::string& key = array[i].getString();
        auto it = dataStore_.find(key);
        if (it != dataStore_.end()) {
            dataStore_.erase(it);
            deletedCount++;
        }
    }
    return RespValue::makeLongLong(deletedCount);
}

RespValue Handler::handleFlush(const std::vector<RespValue> &array) {
    internalClear();
    logger_.clearFile();
    return RespValue::makeSimpleString("OK");
}

void Handler::recover(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) return;

    std::string line;
    while (std::getline(file, line)) {
        std::vector<RespValue> cmd = parseAofLine(line);

        if (cmd.size() >= 3) {
            std::string key = cmd[1].getString();
            RespValue val = cmd[2];

            uint64_t expireAt = -1;
            if (cmd.size() >= 4) {
                expireAt = std::stoull(cmd[3].getString());
            }

            uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch()).count();

            if (expireAt == -1 || expireAt > now) {
                internalSet(key, val, expireAt);
            }
        }
    }
}

std::vector<RespValue> Handler::parseAofLine(const std::string& line) {
    std::vector<RespValue> cmd;
    std::stringstream ss(line);
    std::string word;

    while (ss >> word) {
        cmd.push_back(RespValue::makeBulkString(word));
    }

    return cmd;
}

void Handler::internalSet(const std::string& key, const RespValue& value, uint64_t expireAt) {
    dataStore_[key] = { value, expireAt };
}

void Handler::internalClear() {
    dataStore_.clear();
}