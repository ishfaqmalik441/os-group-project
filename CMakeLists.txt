cmake_minimum_required(VERSION 3.16)
project(group C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(main main.c frames.c frames.h)
target_include_directories(main PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
