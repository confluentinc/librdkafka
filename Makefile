LIBNAME=librdkafka
LIBVER=1

DESTDIR?=/usr/local

SRCS=	rdkafka.c rdkafka_broker.c rdkafka_msg.c rdkafka_topic.c \
	rdkafka_defaultconf.c rdkafka_timer.c rdkafka_offset.c
SRCS+=  rdcrc32.c rdgz.c rdaddr.c rdrand.c rdthread.c rdqueue.c rdlog.c
SRCS+=	snappy.c
HDRS=	rdkafka.h

OBJS=	$(SRCS:.c=.o)
DEPS=	${OBJS:%.o=%.d}

CFLAGS+=-O2 -Wall -Werror -Wfloat-equal -Wpointer-arith -I.
CFLAGS+=-g

# Clang warnings to ignore
ifeq ($(CC),clang)
	CFLAGS+=-Wno-gnu-designator
endif

# Enable iovecs in snappy
CFLAGS+=-DSG

# Profiling
#CFLAGS+=-O0
#CFLAGS += -pg
#LDFLAGS += -pg

LDFLAGS+= -g
UNAME_S = $(shell uname -s)
ifneq ($(findstring CYGWIN,$(UNAME_S)),CYGWIN)
	LDFLAGS+=-fPIC
	CFLAGS+=-fPIC
endif

.PHONY:

all: libs check

libs: $(LIBNAME).so.$(LIBVER) $(LIBNAME).a CONFIGURATION.md

%.o: %.c
	$(CC) -MD -MP $(CFLAGS) -c $<

librdkafka.lds: rdkafka.h
	@echo "\033[33mGenerating linker script $@\033[0m"
	@./lds-gen.pl > $@

$(LIBNAME).so.$(LIBVER): $(OBJS) librdkafka.lds
	@echo "\033[33mCreating shared library $@\033[0m"
	@(if [ $(UNAME_S) = "Darwin" ]; then \
		$(CC) $(LDFLAGS) \
			$(OBJS) -dynamiclib -o $@ -lpthread -lz -lc ; \
	else \
		$(CC) $(LDFLAGS) \
			-shared -Wl,-soname,$@ \
			-Wl,--version-script=librdkafka.lds \
			$(OBJS) -o $@ -lpthread -lrt -lz -lc ; \
	fi)

$(LIBNAME).a:	$(OBJS)
	@echo "\033[33mCreating static library $@\033[0m"
	$(AR) rcs $@ $(OBJS)


CONFIGURATION.md: rdkafka.h examples
	@echo "\033[33mUpdating $@\033[0m"
	@(examples/rdkafka_performance -X list > CONFIGURATION.md.tmp; \
	cmp CONFIGURATION.md CONFIGURATION.md.tmp || \
		mv CONFIGURATION.md.tmp CONFIGURATION.md; \
	rm -f CONFIGURATION.md.tmp)

examples: .PHONY
	make -C $@

install:
	@echo "\033[33mInstall to root $(DESTDIR)\033[0m"
	if [ "$(DESTDIR)" != "/usr/local" ]; then \
		DESTDIR="$(DESTDIR)/usr"; \
	else \
		DESTDIR="$(DESTDIR)" ; \
	fi ; \
	install -d $$DESTDIR/include/librdkafka $$DESTDIR/lib ; \
	install $(HDRS) $$DESTDIR/include/$(LIBNAME) ; \
	install $(LIBNAME).a $$DESTDIR/lib ; \
	install $(LIBNAME).so.$(LIBVER) $$DESTDIR/lib ; \
	(cd $$DESTDIR/lib && ln -sf $(LIBNAME).so.$(LIBVER) $(LIBNAME).so)

tests: .PHONY check
	make -C tests

check:
	@echo "\033[33mChecking integrity\033[0m"
	@(RET=true ; \
	 for f in librdkafka.so.1 librdkafka.a CONFIGURATION.md \
		examples/rdkafka_example examples/rdkafka_performance ; do \
		printf "%-30s " $$f ; \
		if [ -f "$$f" ]; then \
			echo "\033[32mOK\033[0m"; \
		else \
			echo "\033[31mMISSING\033[0m"; \
			RET=false ; \
		fi; \
	done ; \
	$$($$RET))

	@(printf "%-30s " "Symbol visibility" ; \
	((nm -D librdkafka.so.1 | grep -q rd_kafka_new) && \
	  (nm -D librdkafka.so.1 | grep -vq rd_kafka_destroy) && \
		echo "\033[32mOK\033[0m") || \
	  echo "\033[31mFAILED\033[0m")



clean:
	rm -f $(OBJS) $(DEPS) \
		$(LIBNAME)*.a $(LIBNAME)*.so $(LIBNAME)*.so.$(LIBVER) \
		librdkafka.lds
	make -C tests clean
	make -C examples clean

-include $(DEPS)
