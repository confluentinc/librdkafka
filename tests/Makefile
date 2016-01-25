TESTSRCS ?= $(wildcard 0*-*.c) $(wildcard 0*-*.cpp)
OBJS	  = $(TESTSRCS:%.c=%.o)
TESTS    ?= $(TESTSRCS:%.cpp=%.test)
TESTS0    = $(TESTS:%.c=%.test)

BIN	  = merged
LIBS	 += -lrdkafka
OBJS	 += test.o tinycthread.o

-include ../Makefile.config

%.test: ../src/librdkafka.a $(OBJS) %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka $(LIBS)

%.test: ../src/librdkafka.a $(OBJS) %.cpp
	$(CXX) $(CPPFLAGS) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka++ -lrdkafka $(LIBS)

all: $(BIN) run_par

run_par: $(BIN)
	@echo "Running tests in parallel"
	LD_LIBRARY_PATH=../src:../src-cpp \
	DYLD_LIBRARY_PATH=../src:../src-cpp \
	./run-test.sh -p5 ./$(BIN)

run_seq: $(BIN)
	@echo "Running tests sequentially"
	LD_LIBRARY_PATH=../src:../src-cpp \
	DYLD_LIBRARY_PATH=../src:../src-cpp \
	./run-test.sh -p1 ./$(BIN)

run_local: $(BIN)
	@echo "Running local broker-less tests"
	LD_LIBRARY_PATH=../src:../src-cpp \
	DYLD_LIBRARY_PATH=../src:../src-cpp \
	./run-test.sh -p5 -l ./$(BIN)

build: $(BIN)

test.o: ../src/librdkafka.a


include ../mklove/Makefile.base

CFLAGS += -I../src -I../src-cpp
LDFLAGS += -L../src -L../src-cpp

tinycthread.o: ../src/tinycthread.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $<


clean:
	rm -f *.test $(OBJS) $(BIN)


clean-output:
	# Clean test output files
	rm -f stats_*.json *.offset

realclean: clean clean-output
	rm -f test_report_*.json
