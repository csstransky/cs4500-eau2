cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

find_package(Threads REQUIRED)

add_executable(rserver application/rendezvous_server.cpp)
target_link_libraries (rserver ${CMAKE_THREAD_LIBS_INIT})
add_executable(trivial application/trivial.cpp)
target_link_libraries (trivial ${CMAKE_THREAD_LIBS_INIT})
add_executable(demo application/demo.cpp)
target_link_libraries (demo ${CMAKE_THREAD_LIBS_INIT})
add_executable(word_count application/word_count.cpp)
target_link_libraries (word_count ${CMAKE_THREAD_LIBS_INIT})
add_executable(linus application/linus.cpp)
target_link_libraries (linus ${CMAKE_THREAD_LIBS_INIT})
