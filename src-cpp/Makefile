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
LIBS += -lstdc++

CHECK_FILES+= $(LIBNAME).so.$(LIBVER) $(LIBNAME).a

check: file-check

install: lib-install

clean: lib-clean

$(LIBNAME).lds: $(HDRS)
	@(echo "$(MKL_YELLOW)Generating linker script $@ from $(HDRS)$(MKL_CLR_RESET)" ; \
	  cat $(HDRS) | ../lds-gen.pl > $@)

-include $(DEPS)
