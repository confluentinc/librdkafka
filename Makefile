LIBSUBDIRS=	src src-cpp

CHECK_FILES+=	CONFIGURATION.md \
		examples/rdkafka_example examples/rdkafka_performance \
		examples/rdkafka_example_cpp

VERSION = $(shell python rpm/get_version.py)
# Jenkins CI integration
BUILD_NUMBER ?= 1

.PHONY:

all: mklove-check libs CONFIGURATION.md check

include mklove/Makefile.base

libs:
	@(for d in $(LIBSUBDIRS); do $(MAKE) -C $$d || exit $?; done)

CONFIGURATION.md: src/rdkafka.h examples
	@printf "$(MKL_YELLOW)Updating$(MKL_CLR_RESET)\n"
	@(examples/rdkafka_performance -X list > CONFIGURATION.md.tmp; \
		cmp CONFIGURATION.md CONFIGURATION.md.tmp || \
		mv CONFIGURATION.md.tmp CONFIGURATION.md; \
		rm -f CONFIGURATION.md.tmp)

file-check: CONFIGURATION.md examples
check: file-check
	@(for d in $(LIBSUBDIRS); do $(MAKE) -C $$d $@ || exit $?; done)

install:
	@(for d in $(LIBSUBDIRS); do $(MAKE) -C $$d $@ || exit $?; done)

examples tests: .PHONY libs
	$(MAKE) -C $@

clean:
	@$(MAKE) -C tests $@
	@$(MAKE) -C examples $@
	@(for d in $(LIBSUBDIRS); do $(MAKE) -C $$d $@ ; done)

build_prepare: clean
	mkdir -p SOURCES
	git archive --format tar --output SOURCES/librdkafka-$(VERSION).tar HEAD:

srpm: clean build_prepare
	/usr/bin/mock \
		--define "__version $(VERSION)"\
		--define "__release $(BUILD_NUMBER)"\
		--resultdir=. \
		--buildsrpm \
		--spec=rpm/librdkafka.spec \
		--sources=SOURCES

rpm: srpm
	/usr/bin/mock \
		--define "__version $(VERSION)"\
		--define "__release $(BUILD_NUMBER)"\
		--resultdir=. \
		--rebuild *.src.rpm
