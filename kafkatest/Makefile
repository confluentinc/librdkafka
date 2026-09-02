BINARIES ?= verifiable_producer verifiable_share_consumer

all: $(BINARIES)

include ../mklove/Makefile.base

CFLAGS += -I../src -Icommon

CPPFLAGS := $(subst strict-dwarf,,$(CPPFLAGS))

COMMON_SRC := common/common.c
COMMON_HDR := common/common.h

verifiable_producer: ../src/librdkafka.a verifiable_producer/verifiable_producer.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CPPFLAGS) $(CFLAGS) verifiable_producer/verifiable_producer.c $(COMMON_SRC) -o verifiable_producer/$@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

verifiable_share_consumer: ../src/librdkafka.a verifiable_share_consumer/verifiable_share_consumer.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CPPFLAGS) $(CFLAGS) verifiable_share_consumer/verifiable_share_consumer.c $(COMMON_SRC) -o verifiable_share_consumer/$@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

clean:
	rm -f verifiable_producer/verifiable_producer
	rm -f verifiable_share_consumer/verifiable_share_consumer

.PHONY: all clean $(BINARIES)