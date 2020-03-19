cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

find_package(Threads REQUIRED)

add_subdirectory(dataframe)

add_executable(serial_examples serial_examples.cpp)

add_executable(test_serial test_serial.cpp)
add_executable(test-array test-array.cpp)
add_executable(test-map test-map.cpp)
add_executable(test-pair test-pair.cpp)
add_executable(test_sorer test_sor.cpp)
add_executable(test_key test_key.cpp)
add_executable(test_kv_store test_kv_store.cpp)