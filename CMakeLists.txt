cmake_minimum_required(VERSION 4.0)
project(Cache)

set(CMAKE_CXX_STANDARD 20)

add_executable(Cache main.cpp
        resp.h
        parser.cpp
        parser.h
        serializer.cpp
        serializer.h
        server.cpp
        server.h
        handler.cpp
        handler.h
        resp.cpp
        logger.cpp
)

target_link_libraries(Cache ws2_32)

