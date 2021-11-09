librdkafka - the Apache Kafka C/C++ client library
==================================================

This is a forked version of the **librdkafka** library. It provides support for the SASL `AWS_MSK_IAM` mechanism.
The following is a guide to using this fork with other library bindings. We will attempt to keep this 
fork up-to-date as much as possible but we cannot guarantee timelines.

If you have a request related to `AWS_MSK_IAM` support, please open an issue and we'll address as soon as possible.

For more information on `AWS_MSK_IAM`, please refer to the [AWS documentation](https://docs.aws.amazon.com/msk/latest/developerguide/security_iam_service-with-iam.html).

# Features #
  * SASL support for the `AWS_MSK_IAM` mechanism
  * Support for the use of STS temporary credentials with automatic credential refresh

# Binding to other libraries

## Prerequisites

This fork must be built from source on the machine running your workload. This fork requires the following libraries
to be installed before building from source:

  * libssl-dev
  * libcurl4-openss-dev
  * libsasl2-dev
  * liblz4-dev
  * libzstd-dev
  * libxml2-dev

Some of these packages may or may not already be installed on your machine. It is advisable to have each of these libraries
visible to `pkgconfig` for linking. To check the path that `pkgconfig` will search for the `.pc` files, run:

```bash
pkg-config --variable pc_path pkg-config
```

Make sure that the `.pc` files for each of the libraries above are in that path. If they are not, you may need to symlink them.

Once the required libraries are installed, you need to build this fork from source:
```bash
./configure
make
sudo make install
```

When you run `./configure`, you will see if the required libraries were picked up and `SASL_AWS_MSK_IAM` should be included on the `BUILT_WITH` line in your terminal.

## Using the fork with `kcat`
1. Install `kcat` from source
```bash
git clone git@github.com:edenhill/kcat.git
cd kcat
touch bootstrap-no-librdkafka.sh
vi bootstrap-no-librdkafka.sh
```
2. Copy the following shell script into the vim editor, save, and run `./bootstrap-no-librdkafka.sh`
```bash
#!/bin/bash
#
# This script provides a quick build alternative:
# * Dependencies are downloaded and built automatically
# * kcat is built automatically.
# * kcat is linked statically to avoid runtime dependencies.
#
# While this might not be the preferred method of building kcat, it
# is the easiest and quickest way.
#

set -o errexit -o nounset -o pipefail

: "${LIBRDKAFKA_VERSION:=v1.7.0-AWS_MSK_IAM}"

lrk_install_deps="--install-deps"
lrk_static="--enable-static"

for opt in $*; do
    case $opt in
        --no-install-deps)
            lrk_install_deps=""
            ;;

        --no-enable-static)
            lrk_static=""
            ;;

        *)
            echo "Unknown option: $opt"
            exit 1
            ;;
    esac
    shift
done


function download {
    local url=$1
    local dir=$2

    if [[ -d $dir ]]; then
        echo "Directory $dir already exists, not downloading $url"
        return 0
    fi

    echo "Downloading $url to $dir"
    if which wget 2>&1 > /dev/null; then
        local dl='wget -q -O-'
    else
        local dl='curl -s -L'
    fi

    local tar_args=

    # Newer Mac tar's will try to restore metadata/attrs from
    # certain tar files (avroc in this case), which fails for whatever reason.
    if [[ $(uname -s) == "Darwin" ]] &&
           tar --no-mac-metadata -h >/dev/null 2>&1; then
        tar_args="--no-mac-metadata"
    fi

    mkdir -p "$dir"
    pushd "$dir" > /dev/null
    ($dl "$url" | tar -xz $tar_args -f - --strip-components 1) || exit 1
    popd > /dev/null
}


function github_download {
    local repo=$1
    local version=$2
    local dir=$3

    local url=https://github.com/${repo}/archive/${version}.tar.gz

    download "$url" "$dir"
}

function build {
    dir=$1
    cmds=$2


    echo "Building $dir with commands:"
    echo "$cmds"
    pushd $dir > /dev/null
    set +o errexit
    eval $cmds
    ret=$?
    set -o errexit
    popd > /dev/null

    if [[ $ret == 0 ]]; then
        echo "Build of $dir SUCCEEDED!"
    else
        echo "Build of $dir FAILED!"
        exit 1
    fi

    # Some projects, such as yajl, puts pkg-config files in share/ rather
    # than lib/, copy them to the correct location.
    cp -v $DEST/share/pkgconfig/*.pc "$DEST/lib/pkgconfig/" || true

    return $ret
}

function pkg_cfg_lib {
    pkg=$1

    local libs=$(pkg-config --libs --static $pkg)

    # If pkg-config isnt working try grabbing the library list manually.
    if [[ -z "$libs" ]]; then
        libs=$(grep ^Libs.private $DEST/lib/pkgconfig/${pkg}.pc | sed -e s'/^Libs.private: //g')
    fi

    # Since we specify the exact .a files to link further down below
    # we need to remove the -l<libname> here.
    libs=$(echo $libs | sed -e "s/-l${pkg}//g")
    echo " $libs"

    >&2 echo "Using $libs for $pkg"
}

mkdir -p tmp-bootstrap
pushd tmp-bootstrap > /dev/null

export DEST="$PWD/usr"
export CFLAGS="-I$DEST/include"
if [[ $(uname -s) == Linux ]]; then
    export LDFLAGS="-L$DEST/lib -Wl,-rpath-link=$DEST/lib"
else
    export LDFLAGS="-L$DEST/lib"
fi
export PKG_CONFIG_PATH="$DEST/lib/pkgconfig"

# github_download "UrbanCompass/librdkafka" "$LIBRDKAFKA_VERSION" "librdkafka"
# build librdkafka "([ -f config.h ] || ./configure --prefix=$DEST $lrk_install_deps $lrk_static --disable-lz4-ext) && make -j && make install" || (echo "Failed to build librdkafka: bootstrap failed" ; false)

github_download "edenhill/yajl" "edenhill" "libyajl"
build libyajl "([ -d build ] || ./configure --prefix $DEST) && make install" || (echo "Failed to build libyajl: JSON support will probably be disabled" ; true)

download http://www.digip.org/jansson/releases/jansson-2.12.tar.gz libjansson
build libjansson "([[ -f config.status ]] || ./configure --enable-static --prefix=$DEST) && make && make install" || (echo "Failed to build libjansson: AVRO support will probably be disabled" ; true)

github_download "apache/avro" "release-1.8.2" "avroc"
build avroc "cd lang/c && mkdir -p build && cd build && cmake -DCMAKE_C_FLAGS=\"$CFLAGS\" -DCMAKE_INSTALL_PREFIX=$DEST .. && make install" || (echo "Failed to build Avro C: AVRO support will probably be disabled" ; true)

github_download "confluentinc/libserdes" "master" "libserdes"
build libserdes "([ -f config.h ] || ./configure  --prefix=$DEST --CFLAGS=-I${DEST}/include --LDFLAGS=-L${DEST}/lib) && make && make install" || (echo "Failed to build libserdes: AVRO support will probably be disabled" ; true)

popd > /dev/null

echo "Building kcat"
./configure --clean
export CPPFLAGS="${CPPFLAGS:-} -I$DEST/include"
export STATIC_LIB_avro="$DEST/lib/libavro.a"
export STATIC_LIB_rdkafka="$DEST/lib/librdkafka.a"
export STATIC_LIB_serdes="$DEST/lib/libserdes.a"
export STATIC_LIB_yajl="$DEST/lib/libyajl_s.a"
export STATIC_LIB_jansson="$DEST/lib/libjansson.a"

# libserdes does not have a pkg-config file to point out secondary dependencies
# when linking statically.
export LIBS="$(pkg_cfg_lib rdkafka) $(pkg_cfg_lib yajl) $STATIC_LIB_avro $STATIC_LIB_jansson -lcurl"

# Remove tinycthread from libserdes b/c same code is also in librdkafka.
ar dv $DEST/lib/libserdes.a tinycthread.o

./configure --enable-static --enable-json --enable-avro
make

echo ""
echo "Success! kcat is now built"
echo ""

make install

kcat -h
```
3. Once the bootstrap script finishes, you can make sure that the library is installed correctly and is picking up the right **librdkafka** by running `kcat -h` and checking that you see the `sasl_aws_msk_iam` under the `builtin.features` when looking at the help output at the top.

### Usage
When running `kcat` commands, you need to specify the right **librdkafka** configuration properties for enabling IAM auth along with setting the ENV variables for `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and (optionally if using STS) `AWS_SESSION_TOKEN`. The required properties are defined as follows and you need to substitute the proper values for <ROLE_ARN> as well as <SESSION_NAME>:
```bash
kcat -X security.protocol=SASL_SSL \
-X sasl.mechanisms=AWS_MSK_IAM \
-X sasl.aws.access.key.id=${AWS_ACCESS_KEY_ID} \
-X sasl.aws.secret.access.key=${AWS_SECRET_ACCESS_KEY} \
-X sasl.aws.region=us-east-1 \
-X sasl.aws.security.token=${AWS_SESSION_TOKEN} \
-X sasl.aws.role.arn=<ROLE_ARN> \
-X sasl.aws.role.session.name=<SESSION_NAME> \
-X enable.sasl.aws.use.sts=1 \
# see kcat documentation for further commands
```

The use of STS is NOT required. If you want to use permanent credentials instead, you can omit the following properties:

  * sasl.aws.security.token
  * sasl.aws.role.arn
  * sasl.aws.role.session.name
  * enable.sasl.aws.use.sts

## Using the fork with `confluent-kafka-go`
Using `confluent-kafka-go` or if you're building something that requires it, you must build the package with the dynamic build tag:
```bash
CGO_ENABLED=1 go build -tags dynamic ./...
```

Please see the [confluent-kafka-go README](https://github.com/confluentinc/confluent-kafka-go#librdkafka) for more details.

## Using the fork with `confluent-kafka-python` (NOT YET VERIFIED IF WORKS)
Install the Python package from source:
```bash
pip install --no-binary :all: confluent-kafka
```

Please see the [confluent-kafka-python README](https://github.com/confluentinc/confluent-kafka-python#install) for more details.