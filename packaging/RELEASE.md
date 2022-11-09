# librdkafka release process

This guide outlines the steps needed to release a new version of librdkafka
and publish packages to channels (NuGet, Homebrew, etc,..).

Releases are done in two phases:
 * release-candidate(s) - RC1 will be the first release candidate, and any
   changes to the repository will require a new RC.
 * final release - the final release is based directly on the last RC tag
   followed by a single version-bump commit (see below).

Release tag and version format:
 * tagged release builds to verify CI release builders: vA.B.C-PREn
 * release-candidate: vA.B.C-RCn
 * final release: vA.B.C


## Update protocol requests and error codes

Check out the latest version of Apache Kafka (not trunk, needs to be a released
version since protocol may change on trunk).

### Protocol request types

Generate protocol request type codes with:

    $ src/generate_proto.sh ~/src/your-kafka-dir

Cut'n'paste the new defines and strings to `rdkafka_protocol.h` and
`rdkafka_proto.h`.

### Error codes

Error codes must currently be parsed manually, open
`clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`
in the Kafka source directory and update the `rd_kafka_resp_err_t` and
`RdKafka::ErrorCode` enums in `rdkafka.h` and `rdkafkacpp.h`
respectively.
Add the error strings to `rdkafka.c`.
The Kafka error strings are sometimes a bit too verbose for our taste,
so feel free to rewrite them (usually removing a couple of 'the's).
Error strings must not contain a trailing period.

**NOTE**: Only add **new** error codes, do not alter existing ones since that
          will be a breaking API change.


## Run regression tests

**Build tests:**

    $ cd tests
    $ make -j build

**Run the full regression test suite:** (requires Linux and the trivup python package)

    $ make full


If all tests pass, carry on, otherwise identify and fix bug and start over.



## Write release notes / changelog

All relevant PRs should also include an update to [CHANGELOG.md](../CHANGELOG.md)
that in a user-centric fashion outlines what changed.
It might not be practical for all contributors to write meaningful changelog
entries, so it is okay to add them separately later after the PR has been
merged (make sure to credit community contributors for their work).

The changelog should include:
 * What type of release (maintenance or feature release)
 * A short intro to the release, describing the type of release: maintenance
   or feature release, as well as fix or feature high-lights.
 * A section of **New features**, if any.
 * A section of **Upgrade considerations**, if any, to outline important changes
   that require user attention.
 * A section of **Enhancements**, if any.
 * A section of **Fixes**, if any, preferably with Consumer, Producer, and
   Generic sub-sections.


## Pre-release code tasks

**Switch to the release branch which is of the format `A.B.C.x` or `A.B.x`.**

    $ git checkout -b 0.11.1.x


**Update in-code versions.**

The last octet in the version hex number is the pre-build/release-candidate
number, where 0xAABBCCff is the final release for version 0xAABBCC.
Release candidates start at 200, thus 0xAABBCCc9 is RC1, 0xAABBCCca is RC2, etc.

Change the `RD_KAFKA_VERSION` defines in both `src/rdkafka.h` and
`src-cpp/rdkafkacpp.h` to the version to build, such as 0x000b01c9
for v0.11.1-RC1, or 0x000b01ff for the final v0.11.1 release.
Update the librdkafka version in `vcpkg.json`.

   # Update defines
   $ $EDITOR src/rdkafka.h src-cpp/rdkafkacpp.h vcpkg.json

   # Reconfigure and build
   $ ./configure
   $ make

   # Check git diff for correctness
   $ git diff

   # Commit
   $ git commit -m "Version v0.11.1-RC1" src/rdkafka.h src-cpp/rdkafkacpp.h


**Create tag.**

    $ git tag v0.11.1-RC1 # for an RC
    # or for the final release:
    $ git tag v0.11.1     # for the final release


**Push branch and commit to github**

    # Dry-run first to make sure things look correct
    $ git push --dry-run origin 0.11.1.x

    # Live
    $ git push origin 0.11.1.x
**Push tags and commit to github**

    # Dry-run first to make sure things look correct.
    $ git push --dry-run --tags origin v0.11.1-RC1

    # Live
    $ git push --tags origin v0.11.1-RC1


## Creating packages

As soon as a tag is pushed the CI systems (Travis and AppVeyor) will
start their builds and eventually upload the packaging artifacts to S3.
Wait until this process is finished by monitoring the two CIs:

 * https://travis-ci.org/edenhill/librdkafka
 * https://ci.appveyor.com/project/edenhill/librdkafka


### Create NuGet package

On a Linux host with docker installed, this will also require S3 credentials
to be set up.

    $ cd packaging/nuget
    $ python3 -m pip install -r requirements.txt  # if necessary
    $ ./release.py v0.11.1-RC1

Test the generated librdkafka.redist.0.11.1-RC1.nupkg and
then upload it to NuGet manually:

 * https://www.nuget.org/packages/manage/upload


### Create static bundle (for Go)

    $ cd packaging/nuget
    $ ./release.py --class StaticPackage v0.11.1-RC1

Follow the Go client release instructions for updating its bundled librdkafka
version based on the tar ball created here.


## Publish release on github

Create a release on github by going to https://github.com/edenhill/librdkafka/releases
and Draft a new release.
Name the release the same as the final release tag (e.g., `v1.9.0`) and set
the tag to the same.
Paste the CHANGELOG.md section for this release into the release description,
look at the preview and fix any formatting issues.

Run the following command to get checksums of the github release assets:

    $ packaging/tools/gh-release-checksums.py <the-tag>

It will take some time for the script to download the files, when done
paste the output to the end of the release page.

Make sure the release page looks okay, is still correct (check for new commits),
and has the correct tag, then click Publish release.



### Homebrew recipe update

**Note**: This is typically not needed since homebrew seems to pick up new
    release versions quickly enough. Recommend you skip this step.

The brew-update-pr.sh script automatically pushes a PR to homebrew-core
with a patch to update the librdkafka version of the formula.
This should only be done for final releases and not release candidates.

On a MacOSX host with homebrew installed:

    $ cd package/homebrew
    # Dry-run first to see that things are okay.
    $ ./brew-update-pr.sh v0.11.1
    # If everything looks good, do the live push:
    $ ./brew-update-pr.sh --upload v0.11.1


### Deb and RPM packaging

Debian and RPM packages are generated by Confluent packaging, called
Independent client releases, which is a separate non-public process and the
resulting packages are made available on Confluent's client deb and rpm
repositories.

That process is outside the scope of this document.

See the Confluent docs for instructions how to access these packages:
https://docs.confluent.io/current/installation.html




## Build and release artifacts

The following chapter explains what, how, and where artifacts are built.
It also outlines where these artifacts are used.

### So what is an artifact?

An artifact is a build of the librdkafka library, dynamic/shared and/or static,
with a certain set of external or built-in dependencies, for a specific
architecture and operating system (and sometimes even operating system version).

If you build librdkafka from source with no special `./configure` arguments
you will end up with:

 * a dynamically linked library (e.g., `librdkafka.so.1`)
   with a set of dynamically linked external dependencies (OpenSSL, zlib, etc),
   all depending on what dependencies are available on the build host.

 * a static library (`librdkafka.a`) that will have external dependencies
   that needs to be linked dynamically. There is no way for a static library
   to express link dependencies, so there will also be `rdkafka-static.pc`
   pkg-config file generated that contains linker flags for the external
   dependencies.
   Those external dependencies are however most likely only available on the
   build host, so this static library is not particularily useful for
   repackaging purposes (such as for high-level clients using librdkafka).

 * a self-contained static-library (`librdkafka-static.a`) which attempts
   to contain static versions of all external dependencies, effectively making
   it possible to link just with `librdkafka-static.a` to get all
   dependencies needed.
   Since the state of static libraries in the various distro and OS packaging
   systems is of varying quality and availability, it is usually not possible
   for the librdkafka build system (mklove) to generate this completely
   self-contained static library simply using dependencies available on the
   build system, and the make phase of the build will emit warnings when it
   can't bundle all external dependencies due to this.
   To circumvent this problem it is possible for the build system (mklove)
   to download and build static libraries of all needed external dependencies,
   which in turn allows it to create a complete bundle of all dependencies.
   This results in a `librdkafka-static.a` that has no external dependecies
   other than the system libraries (libc, pthreads, rt, etc).
   To achieve this you will need to pass
   `--install-deps --source-deps-only --enable-static` to
   librdkafka's `./configure`.

 * `rdkafka.pc` and `rdkafka-static.pc` pkg-config files that tells
   applications and libraries that depend on librdkafka what external
   dependencies are needed to successfully link with librdkafka.
   This is mainly useful for the dynamic librdkafka librdkafka
   (`librdkafka.so.1` or `librdkafka.1.dylib` on OSX).


**NOTE**: Due to libsasl2/cyrus-sasl's dynamically loaded plugins, it is
not possible for us to provide a self-contained static library with
GSSAPI/Kerberos support.



### The artifact pipeline

We rely solely on CI systems to build our artifacts; no artifacts must be built
on a non-CI system (e.g., someones work laptop, some random ec2 instance, etc).

The reasons for this are:

 1. Reproducible builds: we want a well-defined environment that doesn't change
    (too much) without notice and that we can rebuild artifacts on at a later
    time if required.
 2. Security; these CI systems provide at least some degree of security
    guarantees, and they're managed by people who knows what they're doing
    most of the time. This minimizes the risk for an artifact to be silently
    compromised due to the developer's laptop being hacked.
 3. Logs; we have build logs for all artifacts, which contains checksums.
    This way we can know how an artifact was built, what features were enabled
    and what versions of dependencies were used, as well as know that an
    artifact has not been tampered with after leaving the CI system.


By default the CI jobs are triggered by branch pushes and pull requests
and contain a set of jobs to validate that the changes that were pushed does
not break compilation or functionality (by running parts of the test suite).
These jobs do not produce any artifacts.


For the artifact pipeline there's tag builds, which are triggered by pushing a
tag to the git repository.
These tag builds will generate artifacts, and those artifacts are then uploaded
to an S3 bucket (librdkafka-ci-packages) with a key-value based path format
that allows us to identify where each artifact was built, how, for what
platform, os, with what linkage (dynamic or static), etc.

Once all the CI jobs for a tagged build has finished (successfully), it is time
to collect the artifacts and create release packages.

There are two scripts to run in the `packaging/nuget` directory:

 1. `./release.py --upload <your-nuget-key-file> <the-tag>`
    This creates a NuGet package containing various build artifacts from the
    previous CI step, typically `librdkafka.redist.<the-tag-minus-v-prefix>.nupkg`. NuGet packages are zip files, so you can inspect the contents by
    doing `uzip -l librdkafka.redist.<tag..>.nupkg`.

 2. `./release.py -class StaticPackage <the-tag>`
    This creates a tar-ball named `librdkafka-static-bundle-<tag>.tgz`
    with the self-contained static libraries for various platforms.
    This tar-ball is used by `import.sh` in the confluent-kafka-go to import
    and integrate the static libraries into the Go client.


**Note**: You will need AWS S3 credentials to run these scripts as they
          download the artifacts from the S3 buckets.

**Note**: You will need a NuGet API key to upload nuget packages.


### The artifacts

Let's break it down and look at each of the build artifacts from the above
artifact pipeline that end up in release packages.


#### librdkafka.redist NuGet package artifacts

(See `packaging/nuget/packaging.py`) to see how packages are assembled
from build artifacts.)


If we look inside the NuGet redist package (with `unzip -l librdkafka.redist.<version>.nupkg`)
we'll see the following build artifacts:

##### `runtimes/linux-x64/native/librdkafka.so`

Dynamic library, x64, Linux glibc.

Built on Ubuntu 16.04.

Missing features: none

OpenSSL: 1.0.2

External dependencies:

 * libsasl2.so.2 (cyrus-sasl) for GSSAPI/Kerberos.
 * libz (zlib) for GZip compression.
 * libcrypto/libssl (OpenSSL 1.0.2) for SSL/TLS and SASL SCRAM and OAUTHBEARER.
 * libcurl (curl) for SASL OAUTHBEARER OIDC.



##### `runtimes/linux-x64/native/centos6-librdkafka.so`

Dynamic library, x64, Linux older glibc for broad backwards compatibility
across glibc-based Linux distros.

Built on CentOS 6.

Missing features: SASL GSSAPI/Kerberos

OpenSSL: 1.0.2

No external dependencies except system libraries.


##### `runtimes/linux-x64/native/centos7-librdkafka.so`

Dynamic library, x64, Linux glibc.

Built on CentOS 7.

Missing features: none

OpenSSL: 1.0.2

External dependencies:

 * libsasl2.so.3 (cyrus-sasl) for GSSAPI/Kerberos.
 * libz (zlib) for GZip compression.
 * libcrypto/libssl (OpenSSL 1.0.2) for SSL/TLS and SASL SCRAM and OAUTHBEARER.


##### `runtimes/linux-x64/native/alpine-librdkafka.so`

Dynamic library, x64, Linux musl (Alpine).

Built on Alpine 3.12.

Missing features: SASL GSSAPI/Kerberos

OpenSSL: 1.1.1

No external dependencies except system libraries.


##### `runtimes/linux-arm64/native/librdkafka.so`

Dynamic library, arm64, Linux glibc.

Built on Ubuntu 18.04.

Missing features: SASL GSSAPI/Kerberos

OpenSSL: 1.1.1

No external dependencies except system libraries.



##### `runtimes/osx-x64/native/librdkafka.dylib`

Dynamic library, x64, MacOSX

Built on MacOSX 12.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.


##### `runtimes/osx-arm64/native/librdkafka.dylib`

Dynamic library, arm64, MacOSX

Built on MacOSX 12.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.



##### `runtimes/win-x86/native/librdkafka.dll`

Dynamic library, x86/i386, Windows.

Built on Windows.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.

All external dependencies are shipped alongside librdkafka.dll in the
NuGet package.


##### `runtimes/win-x64/native/librdkafka.dll`

Dynamic library, x64, Windows.

Built on Windows.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.

All external dependencies are shipped alongside librdkafka.dll in the
NuGet package.



#### librdkafka-static-bundle tarball

This tarball contains self-contained static libraries of librdkafka for various
platforms. It is used by the confluent-kafka-go client.

##### `librdkafka_darwin_amd64.a`

Static library, x64, Mac OSX.

Built on Mac OSX.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.


##### `librdkafka_darwin_arm64.a`

Static library, arm64/m1, Mac OSX.

Built on Mac OSX.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.


##### `librdkafka_glibc_linux.a`

Static library, x64, Linux glibc.

Built on ?

Missing features: SASL GSSAPI/Kerberos

OpenSSL: 1.1.1

No external dependencies except system libraries.


##### `librdkafka_musl_linux.a`

Static library, x64, Linux musl (Alpine).

Built on ?

Missing features: SASL GSSAPI/Kerberos

OpenSSL: 1.1.1

No external dependencies except system libraries.


##### `librdkafka_windows.a`

Static library, x64, Windows.

Built on Windows using MinGW.

Missing features: none

OpenSSL: 1.1.1

No external dependencies except system libraries.




#### NEW ARTIFACTS

Dynamic libraries for librdkafka.redist NuGet package and Python wheels:

linux-x64/librdkafka.so: all, libsasl.so.2 - using manylinux2010 (Centos 6).
linux-x64/centos6-librdkafka.so: all, no gssapi - using manylinux2010 (Centos 6).
linux-x64/centos7-librdkafka.so: all, libsasl.so.3 - using manylinux2014 (Centos7)

linux-arm64/librdkafka.so: all, no gssapi - using manylinux2014_aarch64 (Centos 7).

linux-x64/alpine-librdkafka.so: all, no gssapi - using alpine:3.16.


Need to verify that the glibc libraries work on centos and debian.


Static libraries for confluent-kafka-go:

