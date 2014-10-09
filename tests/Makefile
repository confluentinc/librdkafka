TESTSRCS ?= $(wildcard 0*-*.c) $(wildcard 0*-*.cpp)
TESTS    ?= $(TESTSRCS:%.cpp=%.test)
TESTS0    = $(TESTS:%.c=%.test)

%.test: ../src/librdkafka.a test.o %.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka $(LIBS)

%.test: ../src/librdkafka.a test.o %.cpp
	$(CXX) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka++ -lrdkafka $(LIBS)

all: $(TESTS0)
	for i in $^ ; do \
	 LD_LIBRARY_PATH=../src:../src-cpp ./run-test.sh $$i || exit 1 ; \
	done

include ../mklove/Makefile.base

CFLAGS += -I../src -I../src-cpp
LDFLAGS += -L../src -L../src-cpp

test.o: test.c
	$(CC) $(CFLAGS) -c $<


clean:
	rm -f *.test test.o

