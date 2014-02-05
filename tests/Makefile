TESTSRCS ?= $(wildcard 0*-*.c)
TESTS    ?= $(TESTSRCS:%.c=%.test)

CC ?= cc
CXX ?= g++
CFLAGS += -g -Wall -Werror -Wfloat-equal -Wpointer-arith -O2 -I../
CXXFLAGS += $(CFLAGS)
LDFLAGS +=  -L../ -lrdkafka
LDFLAGS += -lpthread -lz
ifeq ($(shell uname -s), Linux)
	LDFLAGS += -lrt
endif

# Profiling
#CFLAGS += -O0 -pg
#LDFLAGS += -pg


%.test: ../librdkafka.a test.o %.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

all: $(TESTS)
	for i in $^ ; do \
	 LD_LIBRARY_PATH=../ ./run-test.sh $$i || exit 1 ; \
	done

test.o: test.c
	$(CC) $(CFLAGS) -c $<


clean:
	rm -f *.test

