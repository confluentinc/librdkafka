PKGNAME=	librdkafka
LIBNAME=	librdkafka++
LIBVER=		1

CXXSRCS=	RdKafka.cpp ConfImpl.cpp HandleImpl.cpp \
		ConsumerImpl.cpp ProducerImpl.cpp \
		TopicImpl.cpp MessageImpl.cpp

HDRS=		rdkafkacpp.h

OBJS=		$(CXXSRCS:%.cpp=%.o)



all: lib check


LIBS = -L../src -lrdkafka
include ../mklove/Makefile.base

# No linker script/symbol hiding for C++ library
WITH_LDS=n

LIBS += -lstdc++

CHECK_FILES+= $(LIBFILENAME) $(LIBNAME).a

check: file-check

install: lib-install

clean: lib-clean

ifeq ($(WITH_LDS),y)
# Enable linker script if supported by platform
LIB_LDFLAGS+= $(LDFLAG_LINKERSCRIPT)$(LIBNAME).lds
endif

$(LIBNAME).lds: $(HDRS)
	@(echo "$(MKL_YELLOW)Generating linker script $@ from $(HDRS)$(MKL_CLR_RESET)" ; \
	  cat ../src/rdkafka.h | ../lds-gen.pl > $@)

-include $(DEPS)
