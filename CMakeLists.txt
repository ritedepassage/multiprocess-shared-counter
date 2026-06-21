cmake_minimum_required(VERSION 3.21)
project(multiprocess-shared-counter)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(mpsc
    main.cc)

target_include_directories(mpsc PRIVATE include)
