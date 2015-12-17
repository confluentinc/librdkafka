TESTSRCS ?= $(wildcard 0*-*.c) $(wildcard 0*-*.cpp)
OBJS	  = $(TESTSRCS:%.c=%.o)
TESTS    ?= $(TESTSRCS:%.cpp=%.test)
TESTS0    = $(TESTS:%.c=%.test)

BIN	  = merged
LIBS	 += -lrdkafka
OBJS	 += test.o

-include ../Makefile.config

%.test: ../src/librdkafka.a test.o %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka $(LIBS)

%.test: ../src/librdkafka.a test.o %.cpp
	$(CXX) $(CPPFLAGS) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka++ -lrdkafka $(LIBS)

all: $(BIN) run_par
	LD_LIBRARY_PATH=../src:../src-cpp ./run-test.sh -p ./$(BIN)

run_par:
	@echo "Running tests in parallel"
	LD_LIBRARY_PATH=../src:../src-cpp ./run-test.sh -p ./$(BIN)

run_seq:
	@echo "Running tests sequentially"
	LD_LIBRARY_PATH=../src:../src-cpp ./run-test.sh ./$(BIN)

build: $(BIN)

test.o: ../src/librdkafka.a


include ../mklove/Makefile.base

CFLAGS += -I../src -I../src-cpp
LDFLAGS += -L../src -L../src-cpp

test.o: test.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $<


clean:
	rm -f *.test test.o $(OBJS) $(BIN)

