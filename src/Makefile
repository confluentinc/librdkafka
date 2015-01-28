PKGNAME=	librdkafka
LIBNAME=	librdkafka
LIBVER=		1


SRCS=		rdkafka.c rdkafka_broker.c rdkafka_msg.c rdkafka_topic.c \
		rdkafka_defaultconf.c rdkafka_timer.c rdkafka_offset.c \
		rdcrc32.c rdgz.c rdaddr.c rdrand.c rdthread.c rdqueue.c \
		rdlog.c \
		snappy.c

HDRS=		rdkafka.h

OBJS=		$(SRCS:.c=.o)


all: lib check

include ../mklove/Makefile.base

CHECK_FILES+= $(LIBFILENAME) $(LIBNAME).a

file-check: lib
check: file-check
	@(printf "%-30s " "Symbol visibility" ; \
	(($(SYMDUMPER) $(LIBFILENAME) | grep -q rd_kafka_new) && \
	  ($(SYMDUMPER) $(LIBFILENAME) | grep -vq rd_kafka_destroy) && \
		echo "\033[32mOK\033[0m") || \
	  echo "\033[31mFAILED\033[0m")

install: lib-install

clean: lib-clean

ifeq ($(WITH_LDS),y)
# Enable linker script if supported by platform
LIB_LDFLAGS+= $(LDFLAG_LINKERSCRIPT)$(LIBNAME).lds
endif

$(LIBNAME).lds: $(HDRS)
	@(echo "$(MKL_YELLOW)Generating linker script $@ from $(HDRS)$(MKL_CLR_RESET)" ; \
	  cat $(HDRS) | ../lds-gen.pl > $@)

-include $(DEPS)
