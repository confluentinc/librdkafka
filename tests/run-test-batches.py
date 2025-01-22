#!/usr/bin/env python3
import os
import sys
import re
import subprocess
import signal
curr_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(curr_dir)

# Max number of tests
max_automatic_tests = 1000
# Higher test number
max_test = 0
tests = set()
for f in os.listdir(curr_dir):
    m = re.match(r'^(\d{4}).*\.c$', f)
    if m:
        test_num = int(m.group(1))
        if test_num < max_automatic_tests:
            tests.add(test_num)
max_test = max(tests)
# Tests batch size
batch_size = os.environ.get('TESTS_BATCH_SIZE', str(max_test))
batch_size = batch_size.isnumeric() and int(batch_size) or max_test
# Number of iterations for each batch
batch_iterations = os.environ.get('TESTS_BATCH_ITERATIONS', "1")
batch_iterations = batch_iterations.isnumeric() and int(batch_iterations) or 1


def valid_test_number(test_number):
    return test_number >= 0 and test_number <= max_test


def extract_tests(tests):
    test_ranges = [test_range for test_range in tests.strip().split(',')
                   if len(test_range.strip()) > 0]
    all_tests = []
    for test_range in test_ranges:

        if '-' in test_range:
            start, end = test_range.split('-')
            start, end = start.strip(), end.strip()

            if start.isnumeric() and end.isnumeric():
                start, end = int(start), int(end)
                if valid_test_number(start) and valid_test_number(
                        end) and start <= end:
                    all_tests.extend(range(start, end + 1))

            elif start.isnumeric() and end == '':
                start = int(start)
                if valid_test_number(start):
                    all_tests.extend(range(int(start), max_test + 1))

        elif test_range.isnumeric():
            all_tests.append(int(test_range))

    return list(sorted(set(all_tests)))


if 'TESTS' in os.environ:
    TESTS = extract_tests(os.environ['TESTS'])
else:
    TESTS = range(0, max_test + 1)

TESTS = [str(n).rjust(4, '0') for n in TESTS]

args = sys.argv[1:]
first_error = None
exit_on_first_error = '-a' in args


def run_tests():
    global first_error
    for i in range(0, len(TESTS), batch_size):
        for j in range(batch_iterations):
            interrupted = False
            TESTS_BATCH = ','.join(TESTS[i:i + batch_size])
            print(f"Running tests: {TESTS_BATCH}, iteration: {j+1}")
            p = subprocess.Popen(['./run-test.sh', '-D'] + args,
                                 env={**os.environ, 'TESTS': TESTS_BATCH},
                                 start_new_session=True)
            try:
                p.communicate()
                if p.returncode != 0 and first_error is None:
                    first_error = p.returncode
                    if exit_on_first_error:
                        return
            except BaseException:
                first_error = 1
                interrupted = True
                return
            finally:
                if interrupted:
                    print('Terminating process group...')
                    os.killpg(p.pid, signal.SIGTERM)
                    try:
                        p.wait(10)
                    except subprocess.TimeoutExpired:
                        os.killpg(p.pid, signal.SIGKILL)
                        p.wait(10)


run_tests()
print('End of run-test-batches')
if first_error:
    sys.exit(first_error)
