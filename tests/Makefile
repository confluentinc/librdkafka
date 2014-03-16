TESTSRCS ?= $(wildcard 0*-*.c)
TESTS    ?= $(TESTSRCS:%.c=%.test)

%.test: ../src/librdkafka.a test.o %.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lrdkafka $(LIBS)

all: $(TESTS)
	for i in $^ ; do \
	 LD_LIBRARY_PATH=../src ./run-test.sh $$i || exit 1 ; \
	done

include ../mklove/Makefile.base

CFLAGS += -I../src
LDFLAGS += -L../src

test.o: test.c
	$(CC) $(CFLAGS) -c $<


clean:
	rm -f *.test test.o

