# CMake generated Testfile for 
# Source directory: /Users/pvemula/Desktop/conf/librdkafka/tests
# Build directory: /Users/pvemula/Desktop/conf/librdkafka/build/tests
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(RdKafkaTestInParallel "/Users/pvemula/Desktop/conf/librdkafka/build/tests/test-runner" "-p5")
set_tests_properties(RdKafkaTestInParallel PROPERTIES  _BACKTRACE_TRIPLES "/Users/pvemula/Desktop/conf/librdkafka/tests/CMakeLists.txt;147;add_test;/Users/pvemula/Desktop/conf/librdkafka/tests/CMakeLists.txt;0;")
add_test(RdKafkaTestSequentially "/Users/pvemula/Desktop/conf/librdkafka/build/tests/test-runner" "-p1")
set_tests_properties(RdKafkaTestSequentially PROPERTIES  _BACKTRACE_TRIPLES "/Users/pvemula/Desktop/conf/librdkafka/tests/CMakeLists.txt;148;add_test;/Users/pvemula/Desktop/conf/librdkafka/tests/CMakeLists.txt;0;")
add_test(RdKafkaTestBrokerLess "/Users/pvemula/Desktop/conf/librdkafka/build/tests/test-runner" "-p5" "-l")
set_tests_properties(RdKafkaTestBrokerLess PROPERTIES  _BACKTRACE_TRIPLES "/Users/pvemula/Desktop/conf/librdkafka/tests/CMakeLists.txt;149;add_test;/Users/pvemula/Desktop/conf/librdkafka/tests/CMakeLists.txt;0;")
