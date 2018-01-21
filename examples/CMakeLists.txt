add_executable(rdkafka_example rdkafka_example.c)
target_link_libraries(rdkafka_example PUBLIC rdkafka)

add_executable(rdkafka_simple_producer rdkafka_simple_producer.c)
target_link_libraries(rdkafka_simple_producer PUBLIC rdkafka)

add_executable(rdkafka_consumer_example rdkafka_consumer_example.c)
target_link_libraries(rdkafka_consumer_example PUBLIC rdkafka)

add_executable(rdkafka_performance rdkafka_performance.c)
target_link_libraries(rdkafka_performance PUBLIC rdkafka)

set(rdkafka_example_cpp_sources rdkafka_example.cpp)
if(WIN32)
    list(APPEND rdkafka_example_cpp_sources ../win32/wingetopt.c ../win32/wingetopt.h)
endif()
add_executable(rdkafka_example_cpp ${rdkafka_example_cpp_sources})
target_link_libraries(rdkafka_example_cpp PUBLIC rdkafka++ crypt32)

add_executable(kafkatest_verifiable_client kafkatest_verifiable_client.cpp)
target_link_libraries(kafkatest_verifiable_client PUBLIC rdkafka++)

add_executable(rdkafka_consumer_example_cpp rdkafka_consumer_example.cpp)
target_link_libraries(rdkafka_consumer_example_cpp PUBLIC rdkafka++)
