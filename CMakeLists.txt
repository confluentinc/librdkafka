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

try_compile(
    HAVE_REGEX
    "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
    "${CMAKE_CURRENT_LIST_DIR}/try_compile/regex_test.cpp"
)

try_compile(
    HAVE_STRNDUP
    "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
    "${CMAKE_CURRENT_LIST_DIR}/try_compile/strndup_test.cpp"
)

# Atomic 32 tests {
set(LINK_ATOMIC NO)
set(HAVE_ATOMICS_32 NO)
set(HAVE_ATOMICS_32_SYNC NO)

try_compile(
    _atomics_32
    "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
    "${CMAKE_CURRENT_LIST_DIR}/try_compile/atomic_32_test.cpp"
)

if(_atomics_32)
  set(HAVE_ATOMICS_32 YES)
else()
  try_compile(
      _atomics_32_lib
      "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
      "${CMAKE_CURRENT_LIST_DIR}/try_compile/atomic_32_test.cpp"
      LINK_LIBRARIES "-latomic"
  )
  if(_atomics_32_lib)
    set(HAVE_ATOMICS_32 YES)
    set(LINK_ATOMIC YES)
  else()
    try_compile(
        HAVE_ATOMICS_32_SYNC
        "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
        "${CMAKE_CURRENT_LIST_DIR}/try_compile/sync_32_test.cpp"
    )
  endif()
endif()
# }

# Atomic 64 tests {
set(HAVE_ATOMICS_64 NO)
set(HAVE_ATOMICS_64_SYNC NO)

try_compile(
    _atomics_64
    "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
    "${CMAKE_CURRENT_LIST_DIR}/try_compile/atomic_64_test.cpp"
)

if(_atomics_64)
  set(HAVE_ATOMICS_64 YES)
else()
  try_compile(
      _atomics_64_lib
      "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
      "${CMAKE_CURRENT_LIST_DIR}/try_compile/atomic_64_test.cpp"
      LINK_LIBRARIES "-latomic"
  )
  if(_atomics_64_lib)
    set(HAVE_ATOMICS_64 YES)
    set(LINK_ATOMIC YES)
  else()
    try_compile(
        HAVE_ATOMICS_64_SYNC
        "${CMAKE_CURRENT_BINARY_DIR}/try_compile"
        "${CMAKE_CURRENT_LIST_DIR}/try_compile/sync_64_test.cpp"
    )
  endif()
endif()
# }

set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
configure_file(config.h.in "${GENERATED_DIR}/config.h" @ONLY)

add_subdirectory(src)
add_subdirectory(src-cpp)

if(RDKAFKA_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
