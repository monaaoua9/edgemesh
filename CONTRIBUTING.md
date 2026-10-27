# Contributing

```
make test        # unit tests
make race        # with the race detector (CI runs this)
make vet fmt-check
make proto       # regenerate gRPC code after editing api/edge/v1/edge.proto
make fuzz        # MQTT parsers + wire decoder
```

* Keep it dependency-light. The only third-party modules are gRPC and protobuf.
* New behaviour needs a test; anything parsing bytes from the network needs a fuzz seed.
* Delivery semantics (ack means durable, at-least-once upstream, per-device
  ordering) are documented in `docs/ARCHITECTURE.md`. A change that weakens
  them needs a note there.
