# WARNING: globbing approach is not recommended.
# From documentation (https://cmake.org/cmake/help/latest/command/file.html):
#
#    We do not recommend using GLOB to collect a list of source files from your
#    source tree. If no CMakeLists.txt file changes when a source is added or
#    removed then the generated build system cannot know when to ask CMake to
#    regenerate.
#
# Some extra information:
# * http://stackoverflow.com/q/1027247/2288008
# Forced here:
# * https://github.com/edenhill/librdkafka/pull/1014#discussion_r97294390
file(GLOB c_sources "0*-*.c")
file(GLOB cpp_sources "0*-*.cpp")

set(sources ${c_sources} ${cpp_sources} test.c testcpp.cpp sockem.c)

add_executable(rdkafka_test ${sources})
target_link_libraries(rdkafka_test PUBLIC rdkafka++)

add_test(NAME RdKafkaTestInParallel COMMAND rdkafka_test -p5)
add_test(NAME RdKafkaTestSequentially COMMAND rdkafka_test -p1)
add_test(NAME RdKafkaTestBrokerLess COMMAND rdkafka_test -p5 -l)
