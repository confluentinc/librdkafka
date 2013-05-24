
LIBNAME=librdkafka
LIBVER=0
LIBVER_FULL=$(LIBVER).0.0


PREFIX?=/usr/local

# The preferred way to compile is to have a separate checkout of librd
# and link with it. If that is not desirable or possible the required librd
# functionality is included with librdkafka for compile-time inclusion.
# Define WITH_LIBRD to use an external librd, or leave undefined for the
# integrated version.
#WITH_LIBRD=1

# Use gcc as ld to avoid __stack_chk_fail_error symbol error.
LD=gcc


SRCS=	rdkafka.c

ifndef WITH_LIBRD
SRCS+=rdcrc32.c rdgz.c rdaddr.c rdrand.c rdfile.c
endif

HDRS=	rdkafka.h rdkafkacpp.h rdtypes.h rd.h rdaddr.h 

OBJS=	$(SRCS:.c=.o)
DEPS=	${OBJS:%.o=%.d}

CFLAGS+=-O2 -Wall -Werror -Wfloat-equal -Wpointer-arith -fPIC -I.
CFLAGS+=-g

# Profiling
#CFLAGS+=-O0
#CFLAGS += -pg
#LDFLAGS += -pg

LDFLAGS+=-shared -g -fPIC -lpthread -lrt -lz -lc

.PHONY:

all: libs

libs: $(LIBNAME).so $(LIBNAME).a

%.o: %.c
	$(CC) -MD -MP $(CFLAGS) -c $<

$(LIBNAME).so:	$(OBJS)
	$(LD) -shared -Wl,-soname,$(LIBNAME).so.$(LIBVER) \
		$(LDFLAGS) $(OBJS) -o $@
	ln -fs $(LIBNAME).so $(LIBNAME).so.$(LIBVER)

$(LIBNAME).a:	$(OBJS)
	$(AR) rcs $@ $(OBJS)

install:
	install -d $(PREFIX)/include/librdkafka $(PREFIX)/lib
	install -t $(PREFIX)/include/$(LIBNAME) $(HDRS)
	install -t $(PREFIX)/lib $(LIBNAME).so
	install -t $(PREFIX)/lib $(LIBNAME).so.$(LIBVER)
	install -t $(PREFIX)/lib $(LIBNAME).a

clean:
	rm -f $(OBJS) $(DEPS) $(LIBNAME)*.a $(LIBNAME)*.so $(LIBNAME)*.so.?

-include $(DEPS)
