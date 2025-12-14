#include "parser.h"

#include <iostream>
#include <stdexcept>
void expectChar(const std::string& input, const size_t& pos, char expected) {
    if (pos >= input.size() || input[pos] != expected) {
        throw std::runtime_error(
            "Expected '" + std::string(1, expected) +
            "' at position " + std::to_string(pos)
        );
    }
}

bool isInteger(const std::string& s) {
    if (s.empty()) return false;

    size_t i = 0;
    if (s[0] == '-' || s[0] == '+') {
        if (s.size() == 1) return false;
        i = 1;
    }

    for (; i < s.size(); i++) {
        if (!std::isdigit(s[i])) return false;
    }
    return true;
}

static std::string readLine(const std::string& input, size_t& pos) {
    const size_t startIndex = pos;
    while (pos < input.size()) {
        if (input[pos] == '\r' && pos + 1 < input.size() && input[pos + 1] == '\n') {
            std::string line = input.substr(startIndex, pos - startIndex);
            pos += 2;
            return line;
        }
        pos++;
    }
    throw std::runtime_error("No CRLF found.");
}

RespValue parseValue(const std::string& input, size_t& pos) {
    char type = input.at(pos);

    switch (type) {
        case '+':
            return parseSimpleString(input, pos);
        case '-':
            return parseError(input, pos);
        case ':':
            return parseInteger(input, pos);
        case '$':
            return parseBulkString(input, pos);
        case '*':
            return parseArray(input, pos);
        default:
            throw std::runtime_error("Invalid RESP type prefix.");
    }
}

RespValue parseSimpleString(const std::string& input, size_t& pos) {
    expectChar(input, pos, '+');
    pos++;

    std::string lineContent = readLine(input, pos);
    return RespValue(RespType::SIMPLE_STRING, lineContent);
}

RespValue parseError(const std::string& input, size_t& pos) {
    expectChar(input, pos, '-');
    pos++;

    std::string lineContent = readLine(input, pos);
    return RespValue(RespType::ERR, lineContent);
}

RespValue parseInteger(const std::string& input, size_t& pos) {
    expectChar(input, pos, ':');
    pos++;

    std::string lineContent = readLine(input, pos);

    if (!isInteger(lineContent)) {
        throw std::runtime_error("Invalid RESP integer at position " + std::to_string(pos));
    }

    long long value = std::stoll(lineContent);
    return RespValue(RespType::INTEGER, value);
}

RespValue parseBulkString(const std::string& input, size_t& pos) {
    expectChar(input, pos, '$');
    pos++;
    std::string lengthStr = readLine(input, pos);
    long long length;
    try {
        length = std::stoll(lengthStr);
    } catch (const std::exception&) {
        throw std::runtime_error("Invalid bulk string length at position " + std::to_string(pos));
    }

    if (length == -1) {
        return RespValue(RespType::BULK_STRING, std::optional<std::string>{});
    }

    if (length < 0) {
        throw std::runtime_error("Invalid bulk string length (negative) at position " + std::to_string(pos));
    }

    if (pos + length + 2 > input.size()) {
        throw std::runtime_error("Bulk string too short at position " + std::to_string(pos));
    }

    std::string content = input.substr(pos, length);
    pos += length;

    if (input[pos] != '\r' || input[pos + 1] != '\n') {
        throw std::runtime_error("Missing CRLF after bulk string content at position " + std::to_string(pos));
    }
    pos += 2;

    return RespValue(RespType::BULK_STRING, std::optional<std::string>{content});
}

RespValue parseArray(const std::string& input, size_t& pos) {
    expectChar(input, pos, '*');
    pos++;

    std::string numStr = readLine(input, pos);
    long long numElements;
    try {
        numElements = std::stoll(numStr);
    } catch (const std::exception&) {
        throw std::runtime_error("Invalid array length at position " + std::to_string(pos));
    }

    if (numElements == -1) {
        return RespValue(RespType::ARRAY, std::optional<std::vector<RespValue>>{});
    }

    if (numElements < 0) {
        throw std::runtime_error("Invalid array length (negative) at position " + std::to_string(pos));
    }

    std::vector<RespValue> elements;

    for (long long i = 0; i < numElements; ++i) {
        elements.push_back(parseValue(input, pos));
    }

    return RespValue(RespType::ARRAY, std::optional<std::vector<RespValue>>{elements});
}
