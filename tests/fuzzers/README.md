# Fuzzing
Librdkafka supports fuzzing by way of Libfuzzer and OSS-Fuzz. This is ongoing work.

## Launching the fuzzers
The easiest way to launch the fuzzers are to go through OSS-Fuzz. The only prerequisite to this is having Docker installed.

With Docker installed, the following commands will build and run the fuzzers in this directory:

```
git clone https://github.com/google/oss-fuzz
cd oss-fuzz
sudo python3 infra/helper.py build_image librdkafka
sudo python3 infra/helper.py build_fuzzers librdkafka
sudo python3 infra/helper.py run_fuzzer librdkafka FUZZ_NAME
```
where FUZZ_NAME references the name opf the fuzzer. Currently the only fuzzer wave is fuzz_regex
