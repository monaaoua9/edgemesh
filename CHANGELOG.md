# Changelog

## 1.0.0
* MQTT 3.1.1 ingest (QoS 0/1, subscribe/downlink, keepalive, auth) and gRPC bidirectional-stream ingest.
* TLS and mutual TLS on MQTT, gRPC and the admin API, with hot certificate reload.
* Segmented CRC'd WAL with torn-tail recovery, disk cap and group commit; QoS1 / gRPC acks only after the durable write.
* Store-and-forward uploader: backoff with jitter, slow-start batches, checkpointing, dead-lettering of poison batches.
* Edge anomaly detection (threshold, rate of change, EWMA z-score) with cooldown.
* Fleet heartbeat tracking and task reassignment away from offline devices.
* Prometheus metrics (latency histograms, memory, queue depth), pprof, status / fleet / alerts endpoints.
* Docker, Compose (with Prometheus + Grafana), Kubernetes and systemd deployment assets.
