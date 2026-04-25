# FlowDriver

FlowDriver exposes a local SOCKS5 proxy on the restricted side and carries the tunnel over Google Drive between your own VPSes and your own Google accounts. The transport in this version is no longer fire-and-forget file polling: it uses durable local spooling, per-session sequence numbers, receiver ACK state, and sender-side cleanup only after ACK.

## What Changed

- Outgoing tunnel segments are written to disk before upload.
- Upload retries do not drop bytes.
- Receiver ACKs the highest contiguous sequence per session and direction.
- Sender keeps uploaded segments until ACK arrives, then deletes from Drive and local spool.
- Idle timeout, cleanup, heartbeat, polling, batching, concurrency, retry, and security settings are configurable.
- Multiple Google backends can be configured and weighted for new-session load balancing.
- Per-backend `target_ip`, `sni`, `host_header`, separate token-endpoint transport settings, and explicit `token_url` overrides are supported.
- The client now uses a built-in CONNECT-only SOCKS5 server, so the project builds without an external SOCKS dependency.

## Deployment Model

- Restricted VPS in Iran: run `cmd/client`
- Foreign VPS: run `cmd/server`
- Client exposes `127.0.0.1:1080` by default
- Server resolves and dials the real destination
- Google Drive is the carrier

## Reliability Model

Each session stream is segmented and named per direction:

- `req-<client>-<session>-<seq>.bin`
- `res-<client>-<session>-<seq>.bin`
- `ack-req-<client>-<session>.json`
- `ack-res-<client>-<session>.json`

Receiver behavior:

- validates and decodes segments
- ignores duplicate sequence numbers
- buffers limited out-of-order segments
- delivers data in order
- updates ACK state locally and uploads ACK files

Sender behavior:

- writes segments to local spool first
- retries upload with backoff
- polls remote ACK state
- deletes remote and local segments only after ACK

## Build

Use the bundled Go toolchain:

```bash
./go1.26/go/bin/go test ./...
./go1.26/go/bin/go build ./cmd/client
./go1.26/go/bin/go build ./cmd/server
```

To format the repo:

```bash
rg --files -g '*.go' --glob '!go1.26/**' | xargs ./go1.26/go/bin/gofmt -w
```

## Configuration

Start from:

- `client_config.json.example`
- `server_config.json.example`

Backward compatibility is preserved for older configs that only used:

- `storage_type`
- `google_folder_id`
- `local_dir`
- `listen_addr`
- `client_id`
- `refresh_rate_ms`
- `flush_rate_ms`
- `transport`

### Recommended Client Settings For A Restricted Iran VPS

- `poll_rate_ms`: `750`
- `active_poll_rate_ms`: `500`
- `idle_poll_rate_ms`: `3000`
- `flush_rate_ms`: `500`
- `segment_bytes`: `65536`
- `max_segment_bytes`: `262144`
- `compression`: `"off"` at first
- `warn_raw_ip`: `true`
- `reject_raw_ip`: `false`
- per-backend `transport.target_ip`: a known Google IP that works from your network
- per-backend `transport.sni`: `google.com` for fronted/restricted setups
- per-backend `transport.host_header`: `www.googleapis.com`
- per-backend `token_url`: `https://www.googleapis.com/oauth2/v4/token` for domain-fronted/restricted setups

### Recommended Server Settings For A Foreign VPS

- keep the same `tunnel_id`
- keep the same backend names and folder IDs
- set `block_private_ips` to `true` unless you explicitly need RFC1918 reachability
- tune `allow_cidrs` and `deny_cidrs` if you want egress policy enforcement

## Google Account And Folder Setup

1. Create one OAuth desktop client per Google account, or reuse one if it is acceptable for your setup.
2. Download each credentials file, for example:
   - `credentials-g1.json`
   - `credentials-g2.json`
3. Enable the Google Drive API in each corresponding Google Cloud project.
4. Run the client once for each credentials file so the refresh token cache file is created next to it.
5. Copy both the credentials JSON and the generated `.token` file to the server.
6. Put the correct `folder_id` for each backend in both client and server configs.

If `folder_id` is omitted, FlowDriver tries to find or create a folder named `Flow-Data` for that backend and writes it back to the config file.

## Multiple Backend Setup

Each backend entry may have:

- its own credentials file
- its own Drive folder
- its own API transport settings
- its own token-endpoint transport settings
- its own token URL
- its own rate limits
- its own weight

New sessions are pinned by consistent weighted hashing. A single session’s ordered stream stays on one backend.

## Rate Limit Tuning

Useful knobs:

- `max_reads_per_second`
- `max_writes_per_second`
- `max_deletes_per_second`
- `max_requests_per_minute`
- `max_daily_upload_bytes`
- `max_daily_download_bytes`
- `stop_when_budget_exhausted`

Start conservatively. If Drive responds with `403` or `429`, lower polling and flush aggressiveness before raising concurrency.

## SOCKS And DNS Notes

- The client SOCKS server supports `CONNECT`.
- `UDP ASSOCIATE` is rejected.
- Domain targets stay unresolved on the client side and are resolved on the server side.
- Raw IP targets are allowed by default because some apps use hardcoded IPs.
- Raw IPs can still be warned or rejected with:
  - `warn_raw_ip`
  - `reject_raw_ip`
  - `allowed_raw_ip_cidrs`

Example:

```bash
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

## Run

Server:

```bash
./server -c server_config.json.example -gc credentials-g1.json
```

Client:

```bash
./client -c client_config.json.example -gc credentials-g1.json
```

If you prefer output binaries in a separate directory:

```bash
./go1.26/go/bin/go build -o bin/client ./cmd/client
./go1.26/go/bin/go build -o bin/server ./cmd/server
./bin/server -c server_config.json.example -gc credentials-g1.json
./bin/client -c client_config.json.example -gc credentials-g1.json
```

## Troubleshooting

### Frequent Reconnects

- Increase `session_idle_timeout_ms`
- Confirm heartbeats are enabled with a non-zero `heartbeat_interval_ms`
- Check logs for repeated upload or ACK failures

### Drive `403` Or `429`

- Lower `poll_rate_ms`
- Raise `idle_poll_rate_ms`
- Lower `max_concurrent_uploads` and `max_concurrent_downloads`
- Reduce per-backend request rates
- Add another backend and spread new sessions across it

### Token Refresh Failure

- Verify the `.token` file matches the credentials JSON beside it
- Check that the OAuth app is still valid and still has Drive API enabled
- Confirm `token_transport.sni` and `token_transport.host_header` match the endpoint you are fronting
- In restricted/fronted setups, set `token_url` to `https://www.googleapis.com/oauth2/v4/token`

### TLS, SNI, Or Host Mismatch

- `transport.target_ip` controls the TCP dial target
- `transport.sni` controls the TLS server name
- `transport.host_header` controls the HTTP Host header
- For token refresh, use the `token_transport` equivalents
- Keep `insecure_skip_verify` as `false` unless you have a very specific reason

### Raw IP Warnings

- They do not automatically mean a DNS leak
- Apps like Telegram or custom clients may use hardcoded IPs
- Use `allowed_raw_ip_cidrs` if you want to permit only a narrow set

### Slow Throughput

- Increase `segment_bytes` carefully
- Keep `max_segment_bytes` large enough for bursty flows
- Add another backend rather than pushing a single account harder
- Turn on `gzip` only after the base path is already stable

## Logging And Counters

The runtime logs session opens/closes, backend selection, retries, ACK updates, cleanup actions, raw-IP warnings, and periodic counters including:

- `active_sessions`
- `bytes_c2s`
- `bytes_s2c`
- `segments_pending`
- `segments_uploaded`
- `segments_downloaded`
- `retries`
- `upload_errors`
- `download_errors`
- `delete_errors`

## Notes

- This project is for your own infrastructure and accounts.
- Keep TLS verification enabled unless you knowingly accept the risk.
- The repo-level `go test ./...` excludes the bundled Go distribution sources by treating `go1.26/go` as a nested module.
