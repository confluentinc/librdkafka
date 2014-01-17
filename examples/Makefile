EXAMPLES ?= rdkafka_example rdkafka_performance
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

all: $(EXAMPLES)

rdkafka_example: ../librdkafka.a rdkafka_example.c
	@(test $@ -nt $< || \
	  $(CC) $(CFLAGS) rdkafka_example.c -o $@ $(LDFLAGS))
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer (write messages on stdin)"
	@echo "./rdkafka_example -P -t <topic> -p <partition>"
	@echo ""
	@echo "# or consumer"
	@echo "./rdkafka_example -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./rdkafka_example --help"

rdkafka_performance: ../librdkafka.a rdkafka_performance.c
	@(test $@ -nt $< || \
	  $(CC) $(CFLAGS) rdkafka_performance.c -o $@ $(LDFLAGS))
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer"
	@echo "./$@ -P -t <topic> -p <partition> -s <msgsize>"
	@echo ""
	@echo "# or consumer"
	@echo "./$@ -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./$@ --help"


clean:
	rm -f rdkafka_example rdkafka_performance
