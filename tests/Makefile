TESTS ?= 0001-multiobj.test 0002-msgtimeout.test
CC ?= cc
CXX ?= g++
CFLAGS += -g -Wall -Werror -Wfloat-equal -Wpointer-arith -O2 -I../
CXXFLAGS += $(CFLAGS)
LDFLAGS +=  ../librdkafka.a
LDFLAGS += -lpthread -lz
ifeq ($(shell uname -s), Linux)
	LDFLAGS += -lrt
endif

# Profiling
#CFLAGS += -O0 -pg
#LDFLAGS += -pg


all: $(TESTS)
	for i in $^ ; do ./run-test.sh $$i || exit 1 ; done

%.test: ../librdkafka.a test.o %.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test.o: test.c
	$(CC) $(CFLAGS) -c $<


clean:
	rm -f $(TESTS)

