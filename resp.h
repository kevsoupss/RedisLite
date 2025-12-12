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

    RespValue(RespType type, RespPayload val)
        : respType(type), value(std::move(val)) {}

    RespValue() = default;
};

#endif //CACHE_RESP_H