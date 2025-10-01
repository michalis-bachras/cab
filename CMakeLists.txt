cmake_minimum_required(VERSION 3.16)
project(cab_debug)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(benchmark benchmark-gen/benchmark.cpp)

target_compile_options(benchmark PRIVATE
        -Wall
        -Werror=return-type
        -Werror=non-virtual-dtor
        -Werror=sequence-point
        -Wsign-compare
        -march=native
        -Wfatal-errors)

target_compile_options(benchmark PRIVATE
        $<$<CONFIG:Debug>:-O0 -g>
        $<$<CONFIG:Release>:-O2>
)