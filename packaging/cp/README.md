# Confluent Platform package verification

This small set of scripts verifies the librdkafka packages that
are part of the Confluent Platform.

The base_url is the http S3 bucket path to the a PR job, or similar.

## How to use
Pass the repository base url and "yes" to test with QEMU on all
supported architectures.

```
$ ./verify-packages.sh https://packages.confluent.io yes|no
```

Requires docker and patience.

