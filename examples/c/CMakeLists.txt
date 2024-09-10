CMAKE_MINIMUM_REQUIRED(VERSION 3.29)
PROJECT("C Example")

ADD_SUBDIRECTORY(../../bindings/c/polodb polodb)

ADD_EXECUTABLE(example src/main.c)
TARGET_LINK_LIBRARIES(example polodb)