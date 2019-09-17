#!/bin/bash
#
#
# Check that the code style is honoured.
#
# Usage: .../checkstyle.sh [options] [<scan-root>]
#
# Run from top-level source directory.
#
# Options:
#   --fix     - Fix style issues (otherwise just report)
#   --install - Install astyle
#
#
# For each source file in the git repository, style it according
# to the style guide and then check for differences.
#
# Requires edenhill's fork of 'astyle' to be installed and an .astylerc file
# at the top-level project directory.

set -e
fix=0

toolsdir=$(dirname $(readlink -f $0))

# Require our modified astyle
astyle="$toolsdir/bin/astyle"

while [[ $1 == --* ]]; do
    case "$1" in
        --fix)
            fix=1
            ;;
        --install)
            if [[ ! -x $astyle ]]; then
                $toolsdir/build-astyle.sh "$toolsdir"
            fi
            $astyle -V
            ;;
        *)
            echo "Usage: $0 [options] [<scan-root>]" 1>&2
            exit 1
            ;;
    esac
    shift
done

if [[ ! -x $astyle ]] ; then
    echo "astyle ($astyle) needs to be built, rerun with --install"
    exit 1
fi

scanroot=$1
ignore_spec="$toolsdir/ignore_style.txt"

# Ignore C++ files until we can get astyle to follow
# the Google C++ style better.
files=$(git ls-files $scanroot | grep -E '\.(c|h)$' || true)
if [[ -z $files ]]; then
    echo "# Warning: No files in $scanroot exist in git, using anyway"
    files="$scanroot"
fi

tmpdir=$(mktemp -d)

checkcnt=0
errcnt=0
errfiles=
if [[ $fix == 1 ]]; then
    echo "# Checking and fixing style.."
else
    echo "# Checking style.."
fi

for f in $files ; do
    if echo "$f" | grep -q -f "$ignore_spec"; then
        continue
    fi
    if [[ $f == *.h ]] & grep -q '^namespace ' "$f"; then
        # Ignore C++ header files
        continue
    fi

    checkcnt=$(expr $checkcnt + 1)
    tmpfile="${tmpdir}//${f//\//__}"
    $astyle --project --stdin="$f" --stdout="$tmpfile"
    if ! cmp -s "$f" "${tmpfile}" ; then
        errcnt=$(expr $errcnt + 1)
        errfiles="${errfiles} $f"
        echo -e "\033[31m########################################"
        echo "# Style mismatch in $f"
        echo -e "########################################\033[0m"
        (cat "$tmpfile" | diff -u "$f" -) || true
        if [[ $fix == 1 ]]; then
            echo -e "\033[33m# Fixing $f !\033[0m"
            cp "$tmpfile" "$f"
        fi
    fi
    rm -f "$tmpfile"
done

rmdir "$tmpdir"

if [[ $checkcnt = 0 ]]; then
    echo "# No files checked"
    exit 0
fi

if [[ $errcnt -gt 0 ]]; then
    echo -e "\033[31m########################################"
    echo "# $errcnt files with style mismatch:"
    for f in $errfiles; do
        echo "  $f"
    done
    echo "# ^ $errcnt files with style mismatch"
    echo -e "########################################\033[0m"
    if [[ $fix == 1 ]]; then
        echo -e "\033[33m# Style errors were automatically fixed.\033[0m"
        exit 0
    else
        echo "You can fix these issues automatically by running 'make fixstyle'."
        exit 1
    fi

fi


exit 0
