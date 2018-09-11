PKGNAME=	librdkafka
LIBNAME=	librdkafka++
LIBVER=		1

CXXSRCS=	RdKafka.cpp ConfImpl.cpp HandleImpl.cpp \
		ConsumerImpl.cpp ProducerImpl.cpp KafkaConsumerImpl.cpp \
		TopicImpl.cpp TopicPartitionImpl.cpp MessageImpl.cpp \
		QueueImpl.cpp MetadataImpl.cpp

HDRS=		rdkafkacpp.h

OBJS=		$(CXXSRCS:%.cpp=%.o)



all: lib check


include ../mklove/Makefile.base

# No linker script/symbol hiding for C++ library
WITH_LDS=n

# OSX and Cygwin requires linking required libraries
ifeq ($(_UNAME_S),Darwin)
	FWD_LINKING_REQ=y
endif
ifeq ($(_UNAME_S),AIX)
	FWD_LINKING_REQ=y
endif
ifeq ($(shell uname -o 2>/dev/null),Cygwin)
	FWD_LINKING_REQ=y
endif

# Ignore previously defined library dependencies for the C library,
# we'll get those dependencies through the C library linkage.

# TODO: does this need to be a build switch or can we do this all the time?
# TODO: will this cause issues with static build?
# We'll additionally look for this library's dependancies within the same directory.
# eg librdkafka++.so will look for its dependancy library librdkafka.so within the same directory.
LIBS := -L../src -lrdkafka -lstdc++ -Wl,-rpath,\$$ORIGIN

CHECK_FILES+= $(LIBFILENAME) $(LIBNAME).a


file-check: lib
check: file-check

install: lib-install
uninstall: lib-uninstall

clean: lib-clean

-include $(DEPS)
