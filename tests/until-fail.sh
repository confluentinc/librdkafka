#!/bin/bash
#

set -e

modes=$*
if [[ -z "$modes" ]]; then
   modes="valgrind"
fi

if [[ -z "$TESTS" ]]; then
    tests=$(echo 0???-*.c 0???-*.cpp)
else
    tests="$TESTS"
fi

iter=0
while true ; do
    iter=$(expr $iter + 1)

    for t in $tests ; do
        # Strip everything after test number (0001-....)
        t=$(echo $t | cut -d- -f1)

        for mode in $modes ; do

            echo "##################################################"
            echo "##################################################"
            echo "############ Test iteration $iter ################"
            echo "############ Test $t in mode $mode ###############"
            echo "##################################################"
            echo "##################################################"

            TESTS=$t ./run-test.sh ./merged $mode || (echo "Failed on iteration $iter, test $t, mode $mode" ; exit 1)
        done
    done
done


