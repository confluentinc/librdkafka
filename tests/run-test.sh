#!/usr/bin/env bash
#

RED='\033[31m'
GREEN='\033[32m'
CYAN='\033[36m'
CCLR='\033[0m'

if [ -z "$1" ]; then
    echo "Usage: $0 [-p] <00xx-...test> [modes..]"
    echo ""
    echo "  Modes: bare valgrind helgrind drd gdb"
    echo "  Options:"
    echo "   -p    - run all tests in parallel"
    exit 1
fi

ARGS=
if [[ $1 == "-p" ]]; then
    ARGS="$ARGS $1"
    shift
fi

TEST=$1
if [ ! -z "$2" ]; then
    MODES=$2
else
    MODES="bare"
    # Enable valgrind:
    #MODES="bare valgrind"
fi

FAILED=0

# Enable valgrind suppressions for false positives
SUPP="--suppressions=librdkafka.suppressions"

# Uncomment to generate valgrind suppressions
#GEN_SUPP="--gen-suppressions=yes"

# Common valgrind arguments
VALGRIND_ARGS="--error-exitcode=3"

# Enable vgdb on valgrind errors.
#VALGRIND_ARGS="$VALGRIND_ARGS --vgdb-error=1"

echo -e "${CYAN}############## $TEST ################${CCLR}"

for mode in $MODES; do
    echo -e "${CYAN}### Running test $TEST in $mode mode ###${CCLR}"
    case "$mode" in
	valgrind)
	    valgrind $VALGRIND_ARGS --leak-check=full --show-leak-kinds=all \
		     --track-origins=yes \
		     $SUPP $GEN_SUPP \
		$TEST $ARGS
	    RET=$?
	    ;;
	helgrind)
	    valgrind $VALGRIND_ARGS --tool=helgrind \
                     --sim-hints=no-nptl-pthread-stackcache \
                     $SUPP $GEN_SUPP \
		$TEST	$ARGS
	    RET=$?
	    ;;
	drd)
	    valgrind $VALGRIND_ARGS --tool=drd $SUPP $GEN_SUPP \
		$TEST	$ARGS
	    RET=$?
	    ;;
        gdb)
            gdb $ARGS $TEST
            RET=$?
            ;;
	bare)
	    $TEST $ARGS
	    RET=$?
	    ;;
	*)
	    echo -e "${RED}### Unknown mode $mode for $TEST ###${CCLR}"
	    RET=1
	    ;;
    esac

    if [ $RET -gt 0 ]; then
	echo -e "${RED}###"
	echo -e "### Test $TEST in $mode mode FAILED! ###"
	echo -e "###${CCLR}"
	FAILED=1
    else
	echo -e "${GREEN}###"
	echo -e "### $Test $TEST in $mode mode PASSED! ###"
	echo -e "###${CCLR}"
    fi
done

exit $FAILED

