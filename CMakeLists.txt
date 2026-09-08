cmake_minimum_required(VERSION 3.28)
project(MontlokTerminal VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MONTLOK_BUILD_NATIVE "Build the Qt native terminal" ON)
option(MONTLOK_BUILD_TESTS "Build Montlok's own client tests" ON)
if(MONTLOK_BUILD_TESTS)
  enable_testing()
endif()

if(MONTLOK_BUILD_NATIVE)
  add_subdirectory(apps/native)
endif()
