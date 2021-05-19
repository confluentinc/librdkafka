#!/bin/bash
#
# Check or apply/fix the project coding style to all files passed as arguments.
#

set -e

ret=0

if [[ -z $1 ]]; then
    echo "Usage: $0 [--fix] srcfile1.c srcfile2.h srcfile3.c ..."
    echo ""
    exit 0
fi

if [[ $1 == "--fix" ]]; then
    fix=1
    shift
else
    fix=0
fi

function ignore {
    local file=${1//q./\.}

    grep -q "^$file$" .formatignore
}

# Read the C++ style from src-cpp/.clang-format and store it
# in a json-like string which is passed to --style.
# (It would be great if clang-format could take a file path for the
#  format file..).
cpp_style="{ $(grep -v '^...$' .clang-format-cpp | grep -v '^$' | tr '\n' ',' | sed -e 's/,$//') }"
if [[ -z $cpp_style ]]; then
    echo "$0: Unable to read .clang-format-cpp"
    exit 1
fi

extra_info=""

for f in $*; do

    if ignore $f ; then
        echo "$f is ignored by .formatignore" 1>&2
        continue
    fi

    if [[ $f == *.cpp ]]; then
        style="$cpp_style"
        stylename="C++"
    elif [[ $f == *.h && $(basename $f) == *cpp* ]]; then
        style="$cpp_style"
        stylename="C++ (header)"
    elif [[ $f == *.py ]]; then
        lang="py"
        style="pep8"
        stylename="pep8"
    else
        style="file"  # Use .clang-format
        stylename="C"
    fi

    if [[ $fix == 0 ]]; then
        # Check for tabs
        if grep -q $'\t' "$f" ; then
            echo "$f: contains tabs: convert to 8 spaces instead"
            ret=1
        fi

        # Check style
        if ! clang-format --style=$style --dry-run "$f" ; then
            echo "$f: had style errors ($style): see clang-format output above"
            ret=1
        fi

    else
        # Convert tabs to spaces first.
        sed -i -e 's/\t/        /g' "$f"

        if [[ $lang == c ]]; then
            # Run clang-format to reformat the file
            clang-format --style="$style" "$f" > _styletmp

        else
            # Run autopep8 to reformat the file.
            python3 -m autopep8 -a "$f" > _styletmp
            # autopep8 can't fix all errors, so we also perform a flake8 check.
            check=1
        fi

        if ! cmp -s "$f" _styletmp; then
            echo "$f: style fixed ($stylename)"
            # Use cp to preserve target file mode/attrs.
            cp _styletmp "$f"
            rm _styletmp
        fi
    fi

    if [[ $fix == 0 || $check == 1 ]]; then
        # Check for tabs
        if grep -q $'\t' "$f" ; then
            echo "$f: contains tabs: convert to 8 spaces instead"
            ret=1
        fi

        # Check style
        if [[ $lang == c ]]; then
            if ! clang-format --style="$style" --dry-run "$f" ; then
                echo "$f: had style errors ($stylename): see clang-format output above"
                ret=1
            fi
        elif [[ $lang == py ]]; then
            if ! python3 -m flake8 "$f"; then
                echo "$f: had style errors ($stylename): see flake8 output above"
                if [[ $fix == 1 ]]; then
                    # autopep8 couldn't fix all errors. Let the user know.
                    extra_info="Error: autopep8 could not fix all errors, fix the flake8 errors manually and run again."
                fi
                ret=1
            fi
        fi
    fi

done

rm -f _styletmp

if [[ $ret != 0 ]]; then
    echo "You can run the following command to automatically fix the style:"
    echo "  $ $0 --fix $*"
fi

exit $ret
