#include "serializer.h"

#include <stdexcept>

std::string serialize(const RespValue& value) {
    switch (value.respType) {
        case RespType::SIMPLE_STRING:
            return serializeSimpleString(std::get<std::string>(value.value));
        case RespType::ERROR:
            return serializeError(std::get<std::string>(value.value));
        case RespType::INTEGER:
            return serializeInteger(std::get<long long>(value.value));
        case RespType::BULK_STRING:
            return serializeBulkString(std::get<std::optional<std::string>>(value.value));
        case RespType::ARRAY:
            return serializeArray(std::get<std::optional<std::vector<RespValue>>>(value.value));
    }
    throw std::runtime_error("Unknown RespType in serializer.");
}

std::string serializeSimpleString(const std::string& value) {
    std::string resp = "+" + value + "\r\n";
    return resp;
}

std::string serializeError(const std::string& value) {
    std::string resp = "-" + value + "\r\n";
    return resp;
}

std::string serializeInteger(long long value) {
    std::string resp = ":" + std::to_string(value) + "\r\n";
    return resp;
}

std::string serializeBulkString(const std::optional<std::string>& value) {
    if (!value.has_value()) {
        std::string nullResp = "$-1\r\n";
        return nullResp;
    }
    const std::string& content = *value;
    size_t length = content.size();
    return "$" + std::to_string(length) + "\r\n" + content + "\r\n";
}

std::string serializeArray(const std::optional<std::vector<RespValue>>& value) {
    if (!value.has_value()) {
        std::string nullResp = "*-1\r\n";
        return nullResp;
    }

    const std::vector<RespValue>& content = *value;
    size_t length = content.size();
    std::string resp = "*" + std::to_string(length) + "\r\n";

    for (const auto& elem : content) {
        resp += serialize(elem);
    }
    return resp;
}