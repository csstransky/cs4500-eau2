cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_COMPILER g++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++11 -ggdb3")

find_package(Threads REQUIRED)

add_subdirectory(src)
add_subdirectory(tests)
