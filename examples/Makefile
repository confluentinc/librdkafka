CC ?= cc
CFLAGS += -g
CFLAGS += -Wall -Werror -Wfloat-equal -Wpointer-arith -O2 -I../
LDFLAGS += -L../ -lrdkafka
LDFLAGS += -lpthread -lrt -lz

# Profiling
#CFLAGS += -O0 -pg
#LDFLAGS += -pg

all:
	@echo "# Examples are built individually, i.e.:"
	@echo " make rdkafka_example"


rdkafka_example: rdkafka_example.c
	@(test $@ -nt $< || \
	  $(CC) $(CFLAGS) $< -o $@ $(LDFLAGS))
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer (write messages on stdin)"
	@echo "LD_LIBRARY_PATH=../ ./rdkafka_example -P -t <topic> -p <partition>"
	@echo ""
	@echo "# or consumer"
	@echo "LD_LIBRARY_PATH=../ ./rdkafka_example -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "LD_LIBRARY_PATH=../ ./rdkafka_example --help"


clean:
	rm -f rdkafka_example
