#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-./bin/mini-redis-server}"
IDLE_CONNECTIONS="${IDLE_CONNECTIONS:-500}"
PIPELINE_COMMANDS="${PIPELINE_COMMANDS:-20000}"
HOST="127.0.0.1"
BASE_PORT="${BASE_PORT:-16479}"

if [[ ! -x "$BIN" ]]; then
  echo "missing binary: $BIN (run make build first)" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for the educational benchmark harness" >&2
  exit 1
fi

cleanup_pids=()
cleanup() {
  for pid in "${cleanup_pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

wait_ready() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket, sys, time
port=int(sys.argv[1])
for _ in range(200):
    try:
        with socket.create_connection(("127.0.0.1",port),timeout=.1):
            raise SystemExit(0)
    except OSError:
        time.sleep(.01)
raise SystemExit("server did not become ready")
PY
}

run_engine() {
  local engine="$1"
  local port="$2"
  local log="/tmp/mini-redis-bench-${engine}-${port}.log"

  "$BIN" -addr "$HOST:$port" -network-engine="$engine" -appendonly=false >"$log" 2>&1 &
  local server_pid=$!
  cleanup_pids+=("$server_pid")
  wait_ready "$port"

  python3 - "$port" "$IDLE_CONNECTIONS" <<'PY' &
import socket, sys, time
port, n = int(sys.argv[1]), int(sys.argv[2])
socks=[]
for _ in range(n):
    s=socket.create_connection(("127.0.0.1",port),timeout=2)
    socks.append(s)
time.sleep(30)
PY
  local holder_pid=$!
  cleanup_pids+=("$holder_pid")
  sleep .5
  local rss_kb
  rss_kb="$(ps -o rss= -p "$server_pid" | tr -d ' ')"
  kill "$holder_pid" 2>/dev/null || true
  wait "$holder_pid" 2>/dev/null || true

  local throughput
  throughput="$(python3 - "$port" "$PIPELINE_COMMANDS" <<'PY'
import socket, sys, time
port, n = int(sys.argv[1]), int(sys.argv[2])
cmd=b"*1\r\n$4\r\nPING\r\n"
want=n*7
with socket.create_connection(("127.0.0.1",port),timeout=5) as s:
    t0=time.perf_counter()
    s.sendall(cmd*n)
    got=0
    while got < want:
        b=s.recv(1<<20)
        if not b: raise SystemExit("connection closed during benchmark")
        got += len(b)
    dt=time.perf_counter()-t0
print(f"{n/dt:.0f}")
PY
)"

  local isolation_us
  isolation_us="$(python3 - "$port" <<'PY'
import socket, sys, time
port=int(sys.argv[1])
slow=socket.create_connection(("127.0.0.1",port),timeout=2)
slow.sendall(b"*2\r\n$3\r\nGET\r\n$5\r\nhel")
fast=socket.create_connection(("127.0.0.1",port),timeout=2)
t0=time.perf_counter_ns()
fast.sendall(b"*1\r\n$4\r\nPING\r\n")
r=fast.recv(64)
us=(time.perf_counter_ns()-t0)/1000
assert r == b"+PONG\r\n", r
print(f"{us:.0f}")
slow.close(); fast.close()
PY
)"

  printf '%-8s %12s %18s %18s\n' "$engine" "$rss_kb" "$throughput" "$isolation_us"
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
}

printf '%-8s %12s %18s %18s\n' "engine" "RSS_KiB" "pipeline_req/s" "isolated_PING_us"
printf '%-8s %12s %18s %18s\n' "--------" "------------" "------------------" "------------------"
run_engine legacy "$BASE_PORT"
run_engine reactor "$((BASE_PORT+1))"

echo
echo "Notes:"
echo "- RSS is sampled with $IDLE_CONNECTIONS idle TCP clients."
echo "- Throughput is a deliberately small Python pipelined PING benchmark ($PIPELINE_COMMANDS commands)."
echo "- Numbers are machine/kernel dependent; compare mechanisms, not absolute Redis performance."
echo "- For heavier load, run redis-benchmark against each -network-engine separately."
