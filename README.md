# FlowDriver

FlowDriver exposes a local SOCKS5 proxy on the restricted side and carries TCP sessions over Google Drive between your own VPSes and your own Google accounts.

The current transport is performance-first:

- the client still serves SOCKS `CONNECT`
- the server still dials the real upstream target
- the carrier no longer uses a single ever-growing `stream.bin`
- the carrier now publishes small unordered chunk files plus a tiny manifest file
- delivery is best-effort; queued bytes can be lost on process crash or severe carrier loss

## Transport Model

Each side publishes chunk batches per direction and backend:

- `manifest-req-<client>.json`
- `manifest-res-<client>.json`
- `chunk-req-<client>-<epoch>-<id>.bin`
- `chunk-res-<client>-<epoch>-<id>.bin`

Steady-state polling only downloads the manifest files. The receiver then fetches only unseen chunk files referenced by the manifest.

Chunk order does not matter. Session order still matters, because the tunnel reconstructs TCP byte streams rather than forwarding original TCP packets.

## Adaptive Mode

Both binaries support `--adaptive`.

When adaptive mode is enabled:

- tunnel identity and safety settings still come from `config.json`
- credential paths, backend names, enabled state, and Drive folder bindings still come from `config.json`
- manual performance/fronting/retry/rate-limit tuning from `config.json` is ignored
- runtime tuning is chosen automatically and updated while the process is running
- adaptive state is persisted to `data_dir/adaptive_state.json`

Adaptive mode currently tunes:

- poll cadence
- flush cadence
- chunk size and mux depth
- upload/download/delete concurrency
- compression mode and threshold
- per-session buffer sizing
- gap grace timing
- backend preference for new sessions
- server dial timeout and TCP keepalive
- Google API/token transport profile selection at startup

## Build

Use the bundled Go toolchain:

```bash
./go1.26/go/bin/go test ./...
./go1.26/go/bin/go build ./cmd/client
./go1.26/go/bin/go build ./cmd/server
```

## Run

Server:

```bash
./server -c server_config.json.example -gc credentials-g1.json
```

Adaptive server:

```bash
./server -c server_config.json.example -gc credentials-g1.json --adaptive
```

Client:

```bash
./client -c client_config.json.example -gc credentials-g1.json
```

Adaptive client:

```bash
./client -c client_config.json.example -gc credentials-g1.json --adaptive
```

## Configuration

Start from:

- `client_config.json.example`
- `server_config.json.example`

Important persistent settings:

- `listen_addr`
- `client_id`
- `server_id`
- `tunnel_id`
- `data_dir`
- backend `credentials_path`
- backend `folder_id`
- raw-IP and CIDR safety policy

Still-supported manual tuning in non-adaptive mode:

- `poll_rate_ms`
- `active_poll_rate_ms`
- `idle_poll_rate_ms`
- `flush_rate_ms`
- `segment_bytes`
- `max_segment_bytes`
- `max_mux_segments`
- `max_concurrent_uploads`
- `max_concurrent_downloads`
- `max_concurrent_deletes`
- `max_pending_segments_per_session`
- `max_tx_buffer_bytes_per_session`
- `max_out_of_order_segments`
- `compression`
- `compression_min_bytes`
- `upload_interval_ms`
- `transport`
- `token_transport`
- `token_url`
- `retry`
- `rate_limits`

In adaptive mode those performance/fronting/rate-limit knobs are ignored.

## Google Backend Notes

Each backend may define:

- its own credentials file
- its own Drive folder
- its own API transport settings
- its own token-endpoint transport settings
- its own token URL
- its own rate limits

If `folder_id` is omitted, FlowDriver tries to find or create a folder named `Flow-Data` for that backend and writes it back to the config file.

## SOCKS Notes

- SOCKS `CONNECT` is supported
- `UDP ASSOCIATE` is rejected
- domain targets stay unresolved on the client side and are resolved on the server side
- raw IP targets can be warned or restricted with:
  - `warn_raw_ip`
  - `reject_raw_ip`
  - `allowed_raw_ip_cidrs`

Example:

```bash
curl --socks5-hostname 127.0.0.1:1080 https://example.com
```

## Behavioral Notes

- This is not a packet tunnel like OpenVPN-over-UDP.
- The carrier is unordered at the chunk layer, but the session layer still reorders per-session envelopes so TCP streams stay valid.
- If a session develops a missing envelope sequence that does not heal within the adaptive grace window, only that session is closed.
- Old chunk files from previous epochs are cleaned up by a background orphan sweeper.

## Troubleshooting

### Slow Throughput

- try `--adaptive`
- add another backend
- verify the selected Google fronting profile actually works on your network

### Frequent Session Drops

- check for repeated upload/download/manifest errors in logs
- increase manual timeouts only if you are running without `--adaptive`
- remember that native UDP/QUIC traffic is still out of scope

### Drive `403` Or `429`

- adaptive mode will back off automatically
- in non-adaptive mode, lower poll and upload aggressiveness
- reduce configured request rates or add another backend

### Missing Or Delayed Traffic

- this transport is best-effort
- process crashes can drop queued chunks
- sustained chunk loss will eventually close only the affected TCP session

## Notes

- This project is for your own infrastructure and accounts.
- Keep TLS verification enabled unless you knowingly accept the risk.
