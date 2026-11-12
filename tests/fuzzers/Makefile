PROGRAMS?=fuzz_regex fuzz_protocol

all: $(PROGRAMS)


fuzz_%:
	$(CC) -fsanitize=address -D WITH_MAIN -g -Wall \
		-I../../src $@.c -o $@ ../../src/librdkafka.a

# fuzz_protocol drives the full client (rd_kafka_new), so it needs librdkafka's
# transitive link dependencies rather than just librdkafka.a.
fuzz_protocol:
	$(CC) -fsanitize=address -D WITH_MAIN -g -Wall \
		-I../../src fuzz_protocol.c -o $@ ../../src/librdkafka.a \
		-lssl -lcrypto -lz -lcurl -lm -ldl -lpthread -lrt


clean:
	rm -f $(PROGRAMS)
