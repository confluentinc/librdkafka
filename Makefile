
LIBNAME=librdkafka
LIBVER=1

DESTDIR?=/usr/local

SRCS=	rdkafka.c rdkafka_broker.c rdkafka_msg.c rdkafka_topic.c \
	rdkafka_defaultconf.c
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
ifneq ($(shell uname -o),Cygwin)
	LDFLAGS+=-fPIC
	CFLAGS+=-fPIC
endif

.PHONY:

all: libs

libs: $(LIBNAME).so.$(LIBVER) $(LIBNAME).a CONFIGURATION.md

%.o: %.c
	$(CC) -MD -MP $(CFLAGS) -c $<


$(LIBNAME).so.$(LIBVER): $(OBJS)
	@(if [ "`uname -s`" = "Linux" -o "`uname -o`" = "Cygwin" ]; then \
		$(CC) $(LDFLAGS) \
			-shared -Wl,-soname,$@ \
			-Wl,--version-script=librdkafka.lds \
			$(OBJS) -o $@ -lpthread -lrt -lz -lc ; \
	elif [ "`uname -s`" = "Darwin" ]; then \
		$(CC) $(LDFLAGS) \
			$(OBJS) -dynamiclib -o $@ -lpthread -lz -lc ; \
	fi)

$(LIBNAME).a:	$(OBJS)
	$(AR) rcs $@ $(OBJS)


CONFIGURATION.md: rdkafka.h examples
	examples/rdkafka_performance -X list > CONFIGURATION.md.tmp
	cmp CONFIGURATION.md CONFIGURATION.md.tmp || \
		mv CONFIGURATION.md.tmp CONFIGURATION.md
	rm -f CONFIGURATION.md.tmp

examples: .PHONY
	make -C $@

install:
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

tests: .PHONY
	make -C tests

clean:
	rm -f $(OBJS) $(DEPS) \
		$(LIBNAME)*.a $(LIBNAME)*.so $(LIBNAME)*.so.$(LIBVER)
	make -C tests clean
	make -C examples clean

-include $(DEPS)
