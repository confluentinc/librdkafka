set(LIBVER 1)

set(
    sources
    ConfImpl.cpp
    ConsumerImpl.cpp
    HandleImpl.cpp
    HeadersImpl.cpp
    KafkaConsumerImpl.cpp
    MessageImpl.cpp
    MetadataImpl.cpp
    ProducerImpl.cpp
    QueueImpl.cpp
    RdKafka.cpp
    TopicImpl.cpp
    TopicPartitionImpl.cpp
)

set(CMAKE_DEBUG_POSTFIX d)

add_library(rdkafka++ SHARED ${sources})
add_library(rdkafka++_static STATIC ${sources})

set_property(TARGET rdkafka++ PROPERTY SOVERSION ${LIBVER})
target_compile_definitions(rdkafka++ PRIVATE LIBRDKAFKACPP_EXPORTS)
target_compile_definitions(rdkafka++_static PUBLIC LIBRDKAFKACPP_STATICLIB)

target_link_libraries(rdkafka++ PUBLIC rdkafka)
target_link_libraries(rdkafka++_static PUBLIC rdkafka_static)

foreach(target rdkafka++ rdkafka++_static)
    # Support '#include <rdkafcpp.h>'
    target_include_directories(${target} PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>")
endforeach()

# Generate pkg-config file
set(PKG_CONFIG_NAME
    "librdkafka++"
)
set(PKG_CONFIG_DESCRIPTION
    "The Apache Kafka C/C++ library"
)
set(PKG_CONFIG_VERSION
    "${PROJECT_VERSION}"
)
set(PKG_CONFIG_REQUIRES "rdkafka")
set(PKG_CONFIG_CFLAGS
    "-I\${includedir}"
)
set(PKG_CONFIG_LIBS
    "-L\${libdir}"
)
set(PKG_CONFIG_LIBS_PRIVATE
    "-lrdkafka"
)
configure_file(
    "../packaging/cmake/rdkafka.pc.in"
    "${GENERATED_DIR}/rdkafka++.pc"
    @ONLY
)
install(FILES ${GENERATED_DIR}/rdkafka++.pc
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
)

install(
    TARGETS rdkafka++ rdkafka++_static
    EXPORT "${targets_export_name}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

if (MSVC)
    set_target_properties(rdkafka++_static
        PROPERTIES
        COMPILE_PDB_NAME_RELWITHDEBINFO rdkafka++_static
        COMPILE_PDB_NAME_DEBUG          rdkafka++_static${CMAKE_DEBUG_POSTFIX})
endif()

install(
    FILES "rdkafkacpp.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/librdkafka"
)
