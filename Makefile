
LIBNAME=librdkafka
LIBVER=1

DESTDIR?=/usr/local

SRCS=	rdkafka.c rdkafka_broker.c rdkafka_msg.c rdkafka_topic.c \
	rdkafka_defaultconf.c
SRCS+=  rdcrc32.c rdgz.c rdaddr.c rdrand.c rdthread.c rdqueue.c rdlog.c
HDRS=	rdkafka.h

OBJS=	$(SRCS:.c=.o)
DEPS=	${OBJS:%.o=%.d}

CFLAGS+=-O2 -Wall -Werror -Wfloat-equal -Wpointer-arith -fPIC -I.
CFLAGS+=-g -rdynamic

# Profiling
#CFLAGS+=-O0
#CFLAGS += -pg
#LDFLAGS += -pg

LDFLAGS+=-shared -g -fPIC -lpthread -lrt -lz -lc

.PHONY:

all: libs

libs: $(LIBNAME).so.$(LIBVER) $(LIBNAME).a

%.o: %.c
	$(CC) -MD -MP $(CFLAGS) -c $<


$(LIBNAME).so.$(LIBVER): $(OBJS)
	$(LD) -shared -soname $@ $(LDFLAGS) \
		--version-script=librdkafka.lds \
		$(OBJS) -o $@

$(LIBNAME).a:	$(OBJS)
	$(AR) rcs $@ $(OBJS)

install:
	if [ "$(DESTDIR)" != "/usr/local" ]; then \
		DESTDIR="$(DESTDIR)/usr"; \
	else \
		DESTDIR="$(DESTDIR)" ; \
	fi ; \
	install -d $$DESTDIR/include/librdkafka $$DESTDIR/lib ; \
	install -t $$DESTDIR/include/$(LIBNAME) $(HDRS) ; \
	install -t $$DESTDIR/lib $(LIBNAME).a ; \
	install -t $$DESTDIR/lib $(LIBNAME).so.$(LIBVER) ; \
	(cd $$DESTDIR/lib && ln -sf $(LIBNAME).so.$(LIBVER) $(LIBNAME).so)

clean:
	rm -f $(OBJS) $(DEPS) \
		$(LIBNAME)*.a $(LIBNAME)*.so $(LIBNAME)*.so.$(LIBVER)

-include $(DEPS)
