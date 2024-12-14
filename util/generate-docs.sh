#!/bin/bash

tmp_log_file=$(mktemp)
rm -rf docs

# Generate documentation using jsdoc.
# We can't rely on the output of the command since it doesn't support import() syntax for
# types, and that is required for type hinting. So we'll just check if there are any errors
# except for that later, by checking the file.
./node_modules/jsdoc/jsdoc.js --destination docs --recurse -R ./README.md -c ./jsdoc.conf -a public,package,undefined 2>&1 | grep -v 'import(' > $tmp_log_file

errors=$(wc -l $tmp_log_file | awk '{print $1}')

if [ $errors -gt 0 ]; then
  echo "Errors found in documentation generation"
  cat $tmp_log_file
  exit 1
fi