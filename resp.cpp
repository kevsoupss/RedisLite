#include "resp.h"

#include <stdexcept>

size_t RespValue::getArraySize() const {
    if (respType != RespType::ARRAY) {
        throw std::runtime_error("Cannot call getArraySize on non-ARRAY type.");
    }
    const auto& optional_vec = std::get<std::optional<std::vector<RespValue>>>(value);

    if (!optional_vec.has_value()) {
        return 0;
    }

    return optional_vec->size();
}

const RespValue& RespValue::getArrayElement(size_t index) const {
    if (respType != RespType::ARRAY) {
        throw std::runtime_error("Cannot call getArrayElement on non-ARRAY type.");
    }
    const auto& optional_vec = std::get<std::optional<std::vector<RespValue>>>(value);

    if (index >= getArraySize()) {
        throw std::runtime_error("Index too large.");
    }

    return optional_vec->at(index);
}

std::string RespValue::getString() const {
    if (respType != RespType::BULK_STRING) {
        throw std::runtime_error("Cannot call getString on non-BULK_STRING type.");
    }
    const auto& optional_string = std::get<std::optional<std::string>>(value);

    if (!optional_string.has_value()) {
        throw std::runtime_error("Empty BULK_STRING");
    }

    return *optional_string;
}

RespValue RespValue::makeProtocolError(const std::string& msg) {
    RespValue err;
    err.respType = RespType::ERR;
    err.value = std::string(msg);
    return err;
}

RespValue RespValue::makeNullBulkString() {
    RespValue nullBulkString;
    nullBulkString.respType = RespType::BULK_STRING;
    nullBulkString.value = std::optional<std::string>{std::nullopt};
    return nullBulkString;
}

RespValue RespValue::makeSimpleString(const std::string& msg) {
    RespValue simpleString;
    simpleString.respType = RespType::SIMPLE_STRING;
    simpleString.value = msg;
    return simpleString;
}

RespValue RespValue::makeLongLong(const long long& value) {
    RespValue res;
    res.respType = RespType::INTEGER;
    res.value = value;
    return res;
}


