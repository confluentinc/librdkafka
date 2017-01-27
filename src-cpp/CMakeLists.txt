add_library(
    rdkafka++
    ConfImpl.cpp
    ConsumerImpl.cpp
    HandleImpl.cpp
    KafkaConsumerImpl.cpp
    MessageImpl.cpp
    MetadataImpl.cpp
    ProducerImpl.cpp
    QueueImpl.cpp
    RdKafka.cpp
    TopicImpl.cpp
    TopicPartitionImpl.cpp
)

target_link_libraries(rdkafka++ PUBLIC rdkafka)

# Support '#include <rdkafcpp.h>'
target_include_directories(rdkafka++ PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>")
