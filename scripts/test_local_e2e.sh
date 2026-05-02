#!/bin/bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

if [ -x "./go1.26/go/bin/go" ]; then
  GO_BIN="./go1.26/go/bin/go"
else
  GO_BIN="${GO_BIN:-go}"
fi

echo "Building binaries with $GO_BIN..."
"$GO_BIN" build -o bin/client ./cmd/client
"$GO_BIN" build -o bin/server ./cmd/server

TMP_DIR=$(mktemp -d)
BACKEND_DIR="$TMP_DIR/backend"
TMP_CONFIG="$TMP_DIR/local.json"

cat > "$TMP_CONFIG" <<EOF
{
  "listen_addr": "127.0.0.1:10800",
  "storage_type": "local",
  "local_dir": "$BACKEND_DIR",
  "client_id": "local-e2e",
  "refresh_rate_ms": 100,
  "flush_rate_ms": 100,
  "block_private_ips": false
}
EOF

mkdir -p "$BACKEND_DIR"

cleanup() {
  echo "Cleaning up..."
  kill "${HTTP_PID:-}" "${SERVER_PID:-}" "${CLIENT_PID:-}" 2>/dev/null || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "Starting local upstream HTTP server..."
python3 -m http.server 18080 --bind 127.0.0.1 > "$TMP_DIR/http.log" 2>&1 &
HTTP_PID=$!

echo "Starting tunnel server..."
./bin/server -c "$TMP_CONFIG" > "$TMP_DIR/server.log" 2>&1 &
SERVER_PID=$!

echo "Starting tunnel client..."
./bin/client -c "$TMP_CONFIG" > "$TMP_DIR/client.log" 2>&1 &
CLIENT_PID=$!

sleep 5

echo "Running local backend SOCKS latency checks..."
for run in 1 2 3; do
  if ! curl -sS -o "$TMP_DIR/body-$run.out" \
    --max-time 20 \
    -w "run=$run code=%{http_code} connect=%{time_connect} start=%{time_starttransfer} total=%{time_total}\n" \
    -x socks5h://127.0.0.1:10800 \
    http://127.0.0.1:18080/; then
    echo "FAILED on run $run. Logs follow:"
    echo "--- SERVER LOG ---"
    cat "$TMP_DIR/server.log"
    echo "--- CLIENT LOG ---"
    cat "$TMP_DIR/client.log"
    echo "--- HTTP LOG ---"
    cat "$TMP_DIR/http.log"
    exit 1
  fi
done

echo "SUCCESS"
