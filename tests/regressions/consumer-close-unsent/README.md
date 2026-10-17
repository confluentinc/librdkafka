# Unsent OffsetCommit during consumer close (Linux)

Build librdkafka, then run:

```sh
python3 tests/regressions/consumer-close-unsent/run.py \
  --output /tmp/librdkafka-consumer-close
```

No Kafka deployment is required: the probe owns three mock brokers. Its Linux
`LD_PRELOAD` shim selectively returns `EAGAIN` for OffsetCommit sends, leaving
them unsent rather than injecting a response after successful transmission.
The shim never records request payloads. An owned native thread releases the
fault after 25 seconds. Consumer close must finish before that release, within
the unchanged 20-second assertion, rather than retry an unsent request forever.
The parent kills only its probe at a 60-second deadline if it hangs.

Controls verify ordinary offset persistence and fatal `FENCED_INSTANCE_ID`
responses both before close and after its session budget expires. Fatal errors
must still be decoded; bypassing the response handler after the deadline is
not a valid fix. All native clients, mock brokers and the release thread are
joined on success. Logs and JSON results stay under the chosen output path.

Use `--library /path/to/librdkafka.so.1` to compare an unpatched build. The
unpatched held-send case fails its close-before-release assertion; the repaired
library passes all four modes. This network fault test is intentionally
Linux-only; the library change uses existing portable APIs.
