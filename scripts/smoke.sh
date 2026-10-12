#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-16379}"
AOF="${AOF:-/tmp/mini-redis-smoke-${PORT}.aof}"
LOG="${LOG:-/tmp/mini-redis-smoke-${PORT}.log}"
rm -f "$AOF" "$LOG"

./bin/mini-redis-server -addr "127.0.0.1:${PORT}" -appendfilename "$AOF" >"$LOG" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; rm -f "$AOF"' EXIT
sleep 0.2

if command -v redis-cli >/dev/null 2>&1; then
  redis-cli -p "$PORT" SET hello world >/dev/null
  test "$(redis-cli -p "$PORT" --raw GET hello)" = "world"
  test "$(redis-cli -p "$PORT" INCR counter)" = "1"
  redis-cli -p "$PORT" SET temp yes EX 1 >/dev/null
  echo "smoke test passed with redis-cli"
else
  python3 - "$PORT" <<'PY'
import socket, sys
port = int(sys.argv[1])

def command(*parts):
    payload = f"*{len(parts)}\r\n".encode()
    for part in parts:
        b = str(part).encode()
        payload += f"${len(b)}\r\n".encode() + b + b"\r\n"
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall(payload)
        return s.recv(4096)

assert command("PING") == b"+PONG\r\n"
assert command("SET", "hello", "world") == b"+OK\r\n"
assert command("GET", "hello") == b"$5\r\nworld\r\n"
assert command("INCR", "counter") == b":1\r\n"
print("smoke test passed with Python RESP client")
PY
fi
