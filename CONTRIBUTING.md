# Contributing to `confluent-kafka-javascript`

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

The following is a set of guidelines for contributing to `confluent-kafka-javascript`
which is hosted by [Confluent Inc.](https://github.com/confluentinc)
on GitHub. This document lists rules, guidelines, and help getting started,
so if you feel something is missing feel free to send a pull request.

## What should I know before I get started?

### Contributor Agreement

Required (please follow instructions after making any Pull Requests).

## How can I contribute?

### Reporting Bugs

Please use __Github Issues__ to report bugs. When filling out an issue report,
make sure to copy any related code and stack traces so we can properly debug.
We need to be able to reproduce a failing test to be able to fix your issue
most of the time, so a custom written failing test is very helpful.

Please also note the Kafka broker version that you are using and how many
replicas, partitions, and brokers you are connecting to, because some issues
might be related to Kafka. A list of `librdkafka` configuration key-value pairs
also helps.

Adding the property `debug` in your `librdkafka` configuration will help us. A list of
possible values is available [here](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md),
but you can set it to `all` if verbose logs are okay.

### Suggesting Enhancements

Please use __Github Issues__ to suggest enhancements. We are happy to consider
any extra functionality or features to the library, as long as they add real
and related value to users. Describing your use case and why such an addition
helps the user base can help guide the decision to implement it into the
library's core.

### Pull Requests

* Include new test cases (either end-to-end or unit tests) with your change.
* Follow our style guides.
* Make sure all tests are still passing and the linter does not report any issues.
* End files with a new line.
* Document the new code in the comments (if it is JavaScript) so the
  documentation generator can update the reference documentation.
* Avoid platform-dependent code.
  <br>**Note:** If making modifications to the underlying C++, please use built-in
  precompiler directives to detect such platform specificities. Use `Nan`
  whenever possible to abstract node/v8 version incompatibility.
* Make sure your branch is up to date and rebased.
* Squash extraneous commits unless their history truly adds value to the library.

## Styleguides

### General style guidelines

Download the [EditorConfig](http://editorconfig.org) plugin for your preferred
text editor to automate the application of the following guidelines:

* Use 2-space indent (no tabs).
* Do not leave trailing whitespace on lines.
* Files should end with a final newline.

Also, adhere to the following not enforced by EditorConfig:

* Limit lines to 80 characters in length. A few extra (<= 5) is fine if it helps
  readability, use good judgement.
* Use `lf` line endings. (git's `core.autocrlf` setting can help)

### Git Commit Messages

Commit messages should adhere to the guidelines in tpope's
[A Note About Git Commit Messages](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)

In short:

* Use the imperative mood. ("Fix bug", not "Fixed bug" or "Fixes bug")
* Limit the first line to 50 characters or less, followed by a blank line
  and detail paragraphs (limit detail lines to about 72 characters).
* Reference issue numbers or pull requests whenever possible.

### JavaScript Styleguide

* All callbacks should follow the standard Node.js callback signature.
* Your JavaScript should properly pass the linter (`make jslint` and `make eslint`).

### C++ Styleguide

* Class member variables should be prefixed with `m_`.
* Use a comment when pointer ownership has changed hands.
* Your C++ should properly pass the `cpplint.py` in the `make lint` test.

### Specs Styleguide

* Write JavaScript tests by using the `mocha` testing framework for the
  non-promisified API and `jest` for the promisified API.
* All `mocha` tests should use exports syntax.
* All `mocha` test files should be suffixed with `.spec.js` instead of `.js`.
* Unit tests should mirror the JavaScript files they test (for example,
  `lib/client.js` is tested in `test/client.spec.js`).
* Unit tests should have no outside service dependencies. Any time a dependency,
  like Kafka, exists, you should create an end-to-end test.
* You may mock a connection in a unit test if it is reliably similar to its real
  variant.

### Documentation Styleguide

* Write all JavaScript documentation in jsdoc-compatible inline comments.
* Each docblock should have references to return types and parameters. If an
  object is a parameter, you should also document any required subproperties.
* Use `@see` to reference similar pieces of code.
* Use comments to document your code when its intent may be difficult to understand.
* All documentation outside of the code should be in Github-compatible markdown.
* Make good use of font variations like __bold__ and *italics*.
* Use headers and tables of contents when they make sense.

## Editor

Using Visual Studio code to develop on `confluent-kafka-javascript`. If you use it you can configure the C++ plugin to resolve the paths needed to inform your intellisense. This is the config file I am using on a mac to resolve the required paths:

`c_cpp_properties.json`
```
{
    "configurations": [
        {
            "name": "Mac",
            "includePath": [
                "${workspaceFolder}/**",
                "${workspaceFolder}",
                "${workspaceFolder}/src",
                "${workspaceFolder}/node_modules/nan",
                "${workspaceFolder}/deps/librdkafka/src",
                "${workspaceFolder}/deps/librdkafka/src-cpp",
                "/usr/local/include/node",
                "/usr/local/include/node/uv"
            ],
            "defines": [],
            "macFrameworkPath": [
                "/Library/Developer/CommandLineTools/SDKs/MacOSX10.14.sdk/System/Library/Frameworks"
            ],
            "compilerPath": "/usr/bin/clang",
            "cStandard": "c11",
            "cppStandard": "c++17",
            "intelliSenseMode": "clang-x64"
        }
    ],
    "version": 4
}
```

## Tests

This project includes three types of tests in this project:
* end-to-end integration tests (`mocha`)
* unit tests (`mocha`)
* integration tests for promisified API (`jest`)

You can run all types of tests by using `Makefile`. Doing so calls `mocha` or `jest` in your locally installed `node_modules` directory.

* Before you run the tests, be sure to init and update the submodules:
  1. `git submodule init`
  2. `git submodule update`
* To run the unit tests, you can run `make lint` or `make test`.
* To run the promisified integration tests, you can use `make promisified_test`.
  You must have a running Kafka installation available. By default, the test tries to connect to `localhost:9092`;
  however, you can supply the `KAFKA_HOST` environment variable to override this default behavior.
* To run the integration tests, you can use `make e2e`.
  You must have a running Kafka installation available. By default, the test tries to connect to `localhost:9092`;
  however, you can supply the `KAFKA_HOST` environment variable to override this default behavior. Run `make e2e`.

## Debugging

### Debugging C++

Use `gdb` for debugging (as shown in the following example).

```
node-gyp rebuild --debug

gdb node
(gdb) set args "path/to/file.js"
(gdb) run
[output here]
```

You can add breakpoints and so on after that.

### Debugging and Profiling JavaScript

Run the code with the `--inspect` flag, and then open `chrome://inspect` in Chrome and connect to the debugger.

Example:

```
node --inspect path/to/file.js
```

## Updating librdkafka version

The librdkafka should be periodically updated to the latest release in https://github.com/confluentinc/librdkafka/releases

Steps to update:
1. Update the `librdkafka` property in [`package.json`](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/package.json) to the desired version.

1. Update the librdkafka git submodule to that versions release commit (example below)

    ```bash
    cd deps/librdkafka
    git checkout 063a9ae7a65cebdf1cc128da9815c05f91a2a996 # for version 1.8.2
    ```

    If you get an error during that checkout command, double check that the submodule was initialized / cloned! You may need to run `git submodule update --init --recursive`

1. Update [`config.d.ts`](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/config.d.ts) and [`errors.d.ts`](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/errors.d.ts) TypeScript definitions by running:
    ```bash
    node ci/librdkafka-defs-generator.js
    ```

1. Run `npm install --lockfile-version 2` to build with the new version and fix any build errors that occur.

1. Run unit tests: `npm run test`

1. Update the version numbers referenced in the [`README.md`](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/README.md) file to the new version.

## Releasing

1. Increment the `version` in `package.json`. Change the version in `client.js` and `README.md`. Change the librdkafka version in `semaphore.yml` and in `package.json`.

1. Run `npm install` to update the `package-lock.json` file.

1. Create a PR and merge the above changes, and tag the merged commit with the new version, e.g. `git tag vx.y.z && git push origin vx.y.z`.
   This should be the same string as `version` in `package.json`.

1. The CI will run on the tag, which will create the release artifacts in Semaphore CI.

1. Create a new GitHub release with the tag, and upload the release artifacts from Semaphore CI.
   The release title should be the same string as `version` in `package.json`.