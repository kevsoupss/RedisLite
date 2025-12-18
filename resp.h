#ifndef CACHE_RESP_H
#define CACHE_RESP_H
#include <optional>
#include <string>
#include <variant>
#include <vector>

enum RespType {
    SIMPLE_STRING,
    ERR,
    INTEGER,
    BULK_STRING,
    ARRAY
};

struct RespValue;

using RespPayload = std::variant<
    std::string,
    long long,
    std::optional<std::string>,
    std::optional<std::vector<RespValue>>
>;

struct RespValue {
    RespType respType;
    RespPayload value;

    RespValue() = default;
    RespValue(RespType type, RespPayload val)
        : respType(type), value(std::move(val)) {}


    size_t getArraySize() const;
    const RespValue& getArrayElement(size_t index) const;
    std::string getString() const;

    static RespValue makeProtocolError(const std::string& msg);
    static RespValue makeNullBulkString();
    static RespValue makeSimpleString(const std::string& msg);
    static RespValue makeLongLong(const long long& value);

};

#endif //CACHE_RESP_H