# Security

## Reporting

Report vulnerabilities through GitHub's private vulnerability reporting
(Security tab, "Report a vulnerability"), not in a public issue.
Expect an acknowledgement within 3 business days.

## Model

* Devices authenticate with a shared bearer token (MQTT password / gRPC
  `authorization` metadata, constant-time compared) and/or client certificates
  (`-tls-client-ca`, mTLS).
* A device may only publish under `devices/<its client id>/...`; anything else
  is refused and counted in `edge_messages_spoofed_total`.
* Mutating admin endpoints (`POST /tasks`, `DELETE /tasks/{id}`) need the same
  token as a bearer. Read-only endpoints (`/status`, `/fleet`, `/alerts`, pprof)
  are unauthenticated: **keep the admin port on a private network** (the shipped
  NetworkPolicy only lets Prometheus in) or put a proxy in front.
* Upstream is HTTPS with a bearer token; trust anchors come from `-cloud-ca` or system roots.
* Hard limits: 64 KB MQTT packet, 1000 readings per gRPC batch, 1 MB per WAL
  record, connection cap, connect and idle timeouts.
* Container: distroless, non-root, read-only root filesystem, all capabilities dropped.

## Known gaps

* A shared token cannot be revoked per device; use mTLS with short-lived certs where that matters.
* Without `-tls-cert` all traffic is plaintext. Do not expose that mode to untrusted networks.
* The WAL is not encrypted at rest; use volume encryption.
