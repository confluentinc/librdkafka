# How to patch our fork

 1. Create the necessary changes in this repo
 2. Validate the changes either locally or by opening a PR in the main ClickHouse repo referencing the commit temporarily
 3. Open a PR in this repo to merge your changes to the latest release branch
 4. Open the upstream PR
 5. Merge the PR in this repo
 6. In the PR in the ClickHouse repo reference the merged commit and update [contrib/librdkafka-cmake/HOW_TO_UPDTE.md](https://github.com/ClickHouse/ClickHouse/blob/master/contrib/librdkafka-cmake/HOW_TO_UPDTE.md)

