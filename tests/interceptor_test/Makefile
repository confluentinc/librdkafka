PKGNAME=	interceptor_test
LIBNAME=	interceptor_test
LIBVER=		1

-include ../../Makefile.config

SRCS=		interceptor_test.c

OBJS=		$(SRCS:.c=.o)

# For rdkafka.h
CPPFLAGS+=-I../../src

# Don't link any libraries, rely on the parent process
# to have done all necessary librdkafka linking.
LIBS=

all: lib

include ../../mklove/Makefile.base

clean: lib-clean

-include $(DEPS)
