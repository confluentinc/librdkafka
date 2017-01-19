cmake_minimum_required(VERSION 3.4)
project(RdKafka)

# Options. No 'RDKAFKA_' prefix to match old C++ code. {

# This option doesn't affect build in fact, only C code
# (see 'rd_kafka_version_str'). In CMake the build type feature usually used
# (like Debug, Release, etc.).
option(WITHOUT_OPTIMIZATION "Disable optimization" OFF)

option(ENABLE_DEVEL "Enable development asserts, checks, etc" OFF)
option(ENABLE_REFCNT_DEBUG "Enable refcnt debugging" OFF)
option(ENABLE_SHAREDPTR_DEBUG "Enable sharedptr debugging" OFF)

option(WITH_ZLIB "With ZLIB" ON)
option(WITH_SSL "With SSL" ON)

# }

option(RDKAFKA_BUILD_EXAMPLES "Build examples" ON)
option(RDKAFKA_BUILD_TESTS "Build tests" ON)

set(TRYCOMPILE_SRC_DIR "${CMAKE_CURRENT_LIST_DIR}/try_compile")

# In:
# * TRYCOMPILE_SRC_DIR
# Out:
# * HAVE_ATOMICS_32
# * HAVE_ATOMICS_32_SYNC
# * HAVE_ATOMICS_64
# * HAVE_ATOMICS_64_SYNC
# * HAVE_REGEX
# * HAVE_STRNDUP
# * LINK_ATOMIC
include("try_compile/cmake/rdkafka_setup.cmake")

set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

# In:
# * WITHOUT_OPTIMIZATION
# * ENABLE_DEVEL
# * ENABLE_REFCNT_DEBUG
# * ENABLE_SHAREDPTR_DEBUG
# * HAVE_ATOMICS_32
# * HAVE_ATOMICS_32_SYNC
# * HAVE_ATOMICS_64
# * HAVE_ATOMICS_64_SYNC
# * WITH_ZLIB
# * WITH_SSL
# * HAVE_REGEX
# * HAVE_STRNDUP
configure_file(config.h.in "${GENERATED_DIR}/config.h" @ONLY)

add_subdirectory(src)
add_subdirectory(src-cpp)

if(RDKAFKA_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(RDKAFKA_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
