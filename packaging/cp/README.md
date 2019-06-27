# Confluent Platform package verification

This small set of scripts verifies the librdkafka packages that
are part of the Confluent Platform.

The base_url is the http S3 bucket path to the a PR job, or similar.

Some modifications to the scripts are needed to replace the CP version
in appended sub-urls, namely replacing 5.3 with the CP version.

## How to use

    $ ./verify-packages.sh https://thes3bucketpath/X/Y


Requires docker.

