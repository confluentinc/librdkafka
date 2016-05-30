PKGNAME=	librdkafka
LIBNAME=	librdkafka
LIBVER=		1

-include ../Makefile.config

SRCS_$(WITH_SASL) += rdkafka_sasl.c
SRCS_$(WITH_SNAPPY) += snappy.c
SRCS_$(WITH_ZLIB) += rdgz.c

SRCS=		rdkafka.c rdkafka_broker.c rdkafka_msg.c rdkafka_topic.c \
		rdkafka_conf.c rdkafka_timer.c rdkafka_offset.c \
		rdkafka_transport.c rdkafka_buf.c rdkafka_queue.c rdkafka_op.c \
		rdkafka_request.c rdkafka_cgrp.c rdkafka_pattern.c \
		rdkafka_partition.c rdkafka_subscription.c \
		rdkafka_assignor.c rdkafka_range_assignor.c \
		rdkafka_roundrobin_assignor.c rdkafka_feature.c \
		rdcrc32.c rdaddr.c rdrand.c rdlist.c tinycthread.c \
		rdlog.c rdstring.c \
		$(SRCS_y)

HDRS=		rdkafka.h

OBJS=		$(SRCS:.c=.o)


all: lib check

include ../mklove/Makefile.base

CHECK_FILES+= $(LIBFILENAME) $(LIBNAME).a

file-check: lib
check: file-check
	@(printf "%-30s " "Symbol visibility" ; \
		(($(SYMDUMPER) $(LIBFILENAME) | grep rd_kafka_new >/dev/null) && \
		($(SYMDUMPER) $(LIBFILENAME) | grep -v rd_kafka_destroy >/dev/null) && \
		printf "$(MKL_GREEN)OK$(MKL_CLR_RESET)\n") || \
		printf "$(MKL_RED)FAILED$(MKL_CLR_RESET)\n")

install: lib-install

clean: lib-clean

ifeq ($(WITH_LDS),y)
# Enable linker script if supported by platform
LIB_LDFLAGS+= $(LDFLAG_LINKERSCRIPT)$(LIBNAME).lds
endif

$(LIBNAME).lds: $(HDRS)
	@(printf "$(MKL_YELLOW)Generating linker script $@ from $(HDRS)$(MKL_CLR_RESET)\n" ; \
	  cat $(HDRS) | ../lds-gen.py > $@)

-include $(DEPS)
