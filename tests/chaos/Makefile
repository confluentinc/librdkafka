# Build the share-consumer verify workload binary.
#
# Picks up CC/CFLAGS/LDFLAGS/LIBS from the root Makefile.config and
# links statically against ../../src/librdkafka.a so the binary
# always exercises the in-tree librdkafka, not whatever .so happens
# to be on the system path.
#
# Built automatically by `make` at the librdkafka root (the root
# Makefile recurses into this folder). To rebuild only this binary
# after editing share_consume_verify.c:
#
#   make chaos             # at the librdkafka root
#   make -C tests/chaos    # equivalently, directly

PROGS = share_consume_verify

CFLAGS += -I../../src

-include ../../Makefile.config

all: $(PROGS)

include ../../mklove/Makefile.base

share_consume_verify: ../../src/librdkafka.a share_consume_verify.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../../src/librdkafka.a $(LIBS)

clean:
	rm -f $(PROGS)

.PHONY: all clean
