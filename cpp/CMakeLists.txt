cmake_minimum_required(VERSION 3.28)

project(cpp_vs_mojo_ds LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Python3 REQUIRED COMPONENTS Interpreter)

execute_process(
  COMMAND git rev-parse --short HEAD
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/.."
  OUTPUT_VARIABLE GIT_SHA
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)
if(NOT GIT_SHA)
  set(GIT_SHA "unknown")
endif()

add_library(project_options INTERFACE)
set(GENERATED_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../build/generated")
set(GENERATED_CPP_DIR "${GENERATED_ROOT}/cpp")
set(GENERATED_CPP_HEADER "${GENERATED_CPP_DIR}/hash_constants.hpp")

add_custom_command(
  OUTPUT "${GENERATED_CPP_HEADER}"
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/../common/generate_hash_assets.py"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/../common/generate_hash_assets.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common/hash_reference.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common/hash_spec.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../common/hash_spec.json"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/.."
  COMMENT "Generating shared hash bindings"
  VERBATIM
)
set_source_files_properties("${GENERATED_CPP_HEADER}" PROPERTIES GENERATED TRUE)

target_include_directories(
  project_options
  INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${GENERATED_CPP_DIR}"
)
target_compile_definitions(project_options INTERFACE DB_STRUCTURES_GIT_SHA="${GIT_SHA}")

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(
    project_options
    INTERFACE
      -Wall
      -Wextra
      -Wpedantic
      $<$<CONFIG:Release>:-O3 -march=native -DNDEBUG>
  )
endif()

add_executable(cpp_tests tests/test_main.cpp)
target_link_libraries(cpp_tests PRIVATE project_options)
target_sources(cpp_tests PRIVATE "${GENERATED_CPP_HEADER}")

add_executable(cpp_bench benches/bench_main.cpp)
target_link_libraries(cpp_bench PRIVATE project_options)
target_sources(cpp_bench PRIVATE "${GENERATED_CPP_HEADER}")

add_executable(cpp_hash tools/hash_cli.cpp)
target_link_libraries(cpp_hash PRIVATE project_options)
target_sources(cpp_hash PRIVATE "${GENERATED_CPP_HEADER}")

add_executable(cpp_structure_parity tools/structure_parity.cpp)
target_link_libraries(cpp_structure_parity PRIVATE project_options)
target_sources(cpp_structure_parity PRIVATE "${GENERATED_CPP_HEADER}")
