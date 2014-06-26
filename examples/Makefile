EXAMPLES ?= rdkafka_example rdkafka_performance rdkafka_example_cpp

all: $(EXAMPLES)

include ../mklove/Makefile.base

CFLAGS += -I../src
CXXFLAGS += -I../src-cpp

# librdkafka must be compiled with -gstrict-dwarf, but rdkafka_example must not,
# due to some clang bug on OSX 10.9
CPPFLAGS := $(subst strict-dwarf,,$(CPPFLAGS))

rdkafka_example: ../src/librdkafka.a rdkafka_example.c
	$(CC) $(CPPFLAGS) $(CFLAGS) rdkafka_example.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer (write messages on stdin)"
	@echo "./rdkafka_example -P -t <topic> -p <partition>"
	@echo ""
	@echo "# or consumer"
	@echo "./rdkafka_example -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./rdkafka_example --help"

rdkafka_performance: ../src/librdkafka.a rdkafka_performance.c
	$(CC) $(CPPFLAGS) $(CFLAGS) rdkafka_performance.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer"
	@echo "./$@ -P -t <topic> -p <partition> -s <msgsize>"
	@echo ""
	@echo "# or consumer"
	@echo "./$@ -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./$@ --help"


rdkafka_example_cpp: ../src-cpp/librdkafka++.a ../src/librdkafka.a rdkafka_example.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) rdkafka_example.cpp -o $@ $(LDFLAGS) \
		../src-cpp/librdkafka++.a ../src/librdkafka.a $(LIBS) -lstdc++

rdkafka_zookeeper_example: ../src/librdkafka.a rdkafka_zookeeper_example.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -I/usr/include/zookeeper rdkafka_zookeeper_example.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS) -lzookeeper_mt -ljansson
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer (write messages on stdin)"
	@echo "./rdkafka_zookeeper_example -P -t <topic> -p <partition>"
	@echo ""
	@echo "# or consumer"
	@echo "./rdkafka_zookeeper_example -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./rdkafka_zookeeper_example --help"

clean:
	rm -f $(EXAMPLES)

