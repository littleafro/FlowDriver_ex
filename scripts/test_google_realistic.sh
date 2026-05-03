#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${ROOT_DIR}/.tmp-google-realistic-$(date +%s)"
CLIENT_PORT="${CLIENT_PORT:-19090}"
UPSTREAM_PORT="${UPSTREAM_PORT:-18183}"

mkdir -p "${TMP_DIR}/client-data" "${TMP_DIR}/server-data"
cp "${ROOT_DIR}/client_config.json" "${TMP_DIR}/client.json"
cp "${ROOT_DIR}/server_config.json" "${TMP_DIR}/server.json"

python3 - <<'PY' "${TMP_DIR}/client.json" "${TMP_DIR}/server.json" "${TMP_DIR}" "${CLIENT_PORT}"
import json, sys

client_path, server_path, tmp_dir, client_port = sys.argv[1:]

with open(client_path) as f:
    client = json.load(f)
with open(server_path) as f:
    server = json.load(f)

client["data_dir"] = f"{tmp_dir}/client-data"
client["listen_addr"] = f"127.0.0.1:{client_port}"

server["data_dir"] = f"{tmp_dir}/server-data"
server["block_private_ips"] = False

with open(client_path, "w") as f:
    json.dump(client, f, indent=2)
    f.write("\n")
with open(server_path, "w") as f:
    json.dump(server, f, indent=2)
    f.write("\n")
PY

cleanup() {
  code=$?
  for pid in ${UPSTREAM_PID:-} ${SERVER_PID:-} ${CLIENT_PID:-}; do
    if [[ -n "${pid:-}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
  done
  wait ${UPSTREAM_PID:-} ${SERVER_PID:-} ${CLIENT_PID:-} 2>/dev/null || true
  exit $code
}
trap cleanup EXIT

python3 - <<'PY' "${UPSTREAM_PORT}" > "${TMP_DIR}/upstream.log" 2>&1 &
import http.server
import json
import socketserver
import sys
import threading
import time

PORT = int(sys.argv[1])

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.startswith("/small"):
            body = json.dumps({"ok": True, "path": self.path}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith("/large"):
            body = (b"0123456789abcdef" * 65536)[:1024 * 1024]
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith("/stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for idx in range(6):
                chunk = f"line-{idx}\n".encode()
                self.wfile.write(f"{len(chunk):X}\r\n".encode())
                self.wfile.write(chunk)
                self.wfile.write(b"\r\n")
                self.wfile.flush()
                time.sleep(0.3)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            return
        body = b"root"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return

class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

server = ThreadingServer(("127.0.0.1", PORT), Handler)
server.serve_forever()
PY
UPSTREAM_PID=$!

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  FLOWDRIVER_GOOGLE_VERBOSE=1 \
  "${ROOT_DIR}/bin/server" -c "${TMP_DIR}/server.json" --adaptive > "${TMP_DIR}/server.log" 2>&1 &
SERVER_PID=$!

sleep 6

env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  FLOWDRIVER_GOOGLE_VERBOSE=1 \
  "${ROOT_DIR}/bin/client" -c "${TMP_DIR}/client.json" --adaptive > "${TMP_DIR}/client.log" 2>&1 &
CLIENT_PID=$!

sleep 14

run_curl() {
  local path="$1"
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
    curl -sS --socks5-hostname "127.0.0.1:${CLIENT_PORT}" \
    -o /dev/null \
    -w "path=${path} code=%{http_code} connect=%{time_connect} start=%{time_starttransfer} total=%{time_total}\n" \
    "http://127.0.0.1:${UPSTREAM_PORT}${path}" \
    --max-time 90
}

echo "== sequential =="
run_curl "/small?i=1"
run_curl "/small?i=2"
run_curl "/small?i=3"

echo "== concurrent =="
export CLIENT_PORT UPSTREAM_PORT
seq 1 4 | xargs -I{} -P 4 bash -lc '
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
    curl -sS --socks5-hostname "127.0.0.1:${CLIENT_PORT}" \
    -o /dev/null \
    -w "path=/small?c={} code=%{http_code} connect=%{time_connect} start=%{time_starttransfer} total=%{time_total}\n" \
    "http://127.0.0.1:${UPSTREAM_PORT}/small?c={}" \
    --max-time 90
'

echo "== large =="
run_curl "/large"

echo "== stream =="
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  curl -sS --socks5-hostname "127.0.0.1:${CLIENT_PORT}" \
  -o /dev/null \
  -w "path=/stream code=%{http_code} connect=%{time_connect} start=%{time_starttransfer} total=%{time_total}\n" \
  "http://127.0.0.1:${UPSTREAM_PORT}/stream" \
  --max-time 90

echo
echo "--- server tail ---"
tail -n 120 "${TMP_DIR}/server.log"
echo
echo "--- client tail ---"
tail -n 120 "${TMP_DIR}/client.log"
