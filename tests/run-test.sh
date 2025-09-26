#!/usr/bin/env bash
#

RED='\033[31m'
GREEN='\033[32m'
CYAN='\033[36m'
CCLR='\033[0m'

if [[ $1 == -h ]]; then
    echo "Usage: $0 [-..] [modes..]"
    echo ""
    echo "  Modes: bare valgrind helgrind cachegrind drd gdb lldb bash"
    echo "  Options:"
    echo "   -..    - test-runner command arguments (pass thru)"
    exit 0
fi

ARGS=

while [[ $1 == -* ]]; do
    ARGS="$ARGS $1"
    shift
done

TEST=./test-runner

if [ ! -z "$1" ]; then
    MODES=$1
else
    MODES="bare"
    # Enable valgrind:
    #MODES="bare valgrind"
fi

FAILED=0

export RDKAFKA_GITVER="$(git rev-parse --short HEAD)@$(git symbolic-ref -q --short HEAD)"

# Enable valgrind suppressions for false positives
SUPP="--suppressions=librdkafka.suppressions"

# Uncomment to generate valgrind suppressions
#GEN_SUPP="--gen-suppressions=yes"

# Common valgrind arguments
VALGRIND_ARGS="--error-exitcode=3"

# Enable vgdb on valgrind errors.
#VALGRIND_ARGS="$VALGRIND_ARGS --vgdb-error=1"

# Exit valgrind on first error
VALGRIND_ARGS="$VALGRIND_ARGS --exit-on-first-error=yes"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../src:../src-cpp
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:../src:../src-cpp

echo -e "${CYAN}############## $TEST ################${CCLR}"

for mode in $MODES; do
    echo -e "${CYAN}### Running test $TEST in $mode mode ###${CCLR}"
    export TEST_MODE=$mode
    case "$mode" in
	valgrind)
	    valgrind $VALGRIND_ARGS --leak-check=full --show-leak-kinds=all \
		     --errors-for-leak-kinds=all \
		     --track-origins=yes \
                     --track-fds=yes \
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
	cachegrind|callgrind)
	    valgrind $VALGRIND_ARGS --tool=$mode \
		     $SUPP $GEN_SUPP \
		$TEST $ARGS
	    RET=$?
	    ;;
	drd)
	    valgrind $VALGRIND_ARGS --tool=drd $SUPP $GEN_SUPP \
		$TEST	$ARGS
	    RET=$?
	    ;;
        callgrind)
	    valgrind $VALGRIND_ARGS --tool=callgrind $SUPP $GEN_SUPP \
		$TEST	$ARGS
	    RET=$?
	    ;;
        gdb)
            grun=$(mktemp gdbrunXXXXXX)
            cat >$grun <<EOF
set \$_exitcode = -999
run $ARGS
if \$_exitcode != -999
 quit
end
EOF
            export ASAN_OPTIONS="$ASAN_OPTIONS:abort_on_error=1"
            gdb -x $grun $TEST
            RET=$?
            rm $grun
            ;;
	bare)
	    $TEST $ARGS
	    RET=$?
	    ;;
        lldb)
            lldb -b -o "process launch --environment DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH" -- $TEST $ARGS
            RET=$?
            ;;
	bash)
	    PS1="[run-test.sh] $PS1" bash
	    RET=$?
	    ;;
	*)
	    echo -e "${RED}### Unknown mode $mode for $TEST ###${CCLR}"
	    RET=1
	    ;;
    esac

    if [ $RET -gt 0 ]; then
	echo -e "${RED}###"
	echo -e "### Test $TEST in $mode mode FAILED! (return code $RET) ###"
	echo -e "###${CCLR}"
	FAILED=1
    else
	echo -e "${GREEN}###"
	echo -e "### $Test $TEST in $mode mode PASSED! ###"
	echo -e "###${CCLR}"
    fi
    
    # Clean up topics after test completion
    cleanup_test_topics
done

# Function to extract topic prefix from test.conf and delete matching topics
cleanup_test_topics() {
    local test_conf="test.conf"
    local topic_prefix=""
    
    # Check if test.conf exists
    if [ ! -f "$test_conf" ]; then
        echo "No test.conf found, skipping topic cleanup"
        return 0
    fi
    
    # Extract topic prefix from test.conf
    topic_prefix=$(grep "^test\.topic\.prefix=" "$test_conf" 2>/dev/null | cut -d'=' -f2 | tr -d ' ')
    
    # Skip cleanup if no prefix is configured
    if [ -z "$topic_prefix" ]; then
        echo "No test.topic.prefix configured, skipping topic cleanup"
        return 0
    fi
    
    echo -e "${CYAN}### Cleaning up topics with prefix: $topic_prefix ###${CCLR}"
    
    # Extract bootstrap servers from test.conf
    local bootstrap_servers=""
    bootstrap_servers=$(grep "^metadata\.broker\.list=" "$test_conf" 2>/dev/null | cut -d'=' -f2 | tr -d ' ')
    
    if [ -z "$bootstrap_servers" ]; then
        bootstrap_servers="localhost:9092"
        echo "Using default bootstrap servers: $bootstrap_servers"
    fi
    
    # Use kafka-topics.sh to list and delete topics with the prefix
    local kafka_topics_cmd=""
    
    # Try to find kafka-topics.sh in common locations
    for path in "/usr/local/bin/kafka-topics.sh" "/opt/kafka/bin/kafka-topics.sh" "kafka-topics.sh" "kafka-topics"; do
        if command -v "$path" >/dev/null 2>&1; then
            kafka_topics_cmd="$path"
            break
        fi
    done
    
    if [ -z "$kafka_topics_cmd" ]; then
        echo -e "${RED}kafka-topics command not found, skipping topic cleanup${CCLR}"
        return 0
    fi
    
    echo "Using kafka-topics command: $kafka_topics_cmd"
    
    # List topics with the prefix
    local topics_to_delete=""
    topics_to_delete=$($kafka_topics_cmd --bootstrap-server "$bootstrap_servers" --list 2>/dev/null | grep "^$topic_prefix" || true)
    
    if [ -z "$topics_to_delete" ]; then
        echo "No topics found with prefix '$topic_prefix'"
        return 0
    fi
    
    echo "Found topics to delete:"
    echo "$topics_to_delete"
    
    # Delete each topic
    echo "$topics_to_delete" | while read -r topic; do
        if [ -n "$topic" ]; then
            echo "Deleting topic: $topic"
            $kafka_topics_cmd --bootstrap-server "$bootstrap_servers" --delete --topic "$topic" 2>/dev/null || {
                echo -e "${RED}Failed to delete topic: $topic${CCLR}"
            }
        fi
    done
    
    echo -e "${GREEN}Topic cleanup completed${CCLR}"
}

exit $FAILED

