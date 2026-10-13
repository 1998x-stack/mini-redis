#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-16379}"
NETWORK_ENGINE="${NETWORK_ENGINE:-auto}"
AOF="${AOF:-/tmp/mini-redis-smoke-${PORT}.aof}"
LOG="${LOG:-/tmp/mini-redis-smoke-${PORT}.log}"
rm -f "$AOF" "$LOG"

./bin/mini-redis-server -addr "127.0.0.1:${PORT}" -network-engine "$NETWORK_ENGINE" -appendfilename "$AOF" >"$LOG" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; rm -f "$AOF"' EXIT
sleep 0.2

if command -v redis-cli >/dev/null 2>&1; then
  redis-cli -p "$PORT" SET hello world >/dev/null
  test "$(redis-cli -p "$PORT" --raw GET hello)" = "world"
  test "$(redis-cli -p "$PORT" INCR counter)" = "1"

  redis-cli -p "$PORT" SET encoded 42 >/dev/null
  test "$(redis-cli -p "$PORT" --raw OBJECT ENCODING encoded)" = "int"
  redis-cli -p "$PORT" SET padded 001 >/dev/null
  test "$(redis-cli -p "$PORT" --raw OBJECT ENCODING padded)" = "raw"

  test "$(redis-cli -p "$PORT" RPUSH list a b c d e f g h i)" = "9"
  test "$(redis-cli -p "$PORT" LLEN list)" = "9"
  test "$(redis-cli -p "$PORT" --raw OBJECT ENCODING list)" = "quicklist"

  test "$(redis-cli -p "$PORT" HSET hash f1 v1 f2 v2)" = "2"
  test "$(redis-cli -p "$PORT" --raw HGET hash f2)" = "v2"

  test "$(redis-cli -p "$PORT" SADD set 1 2 3)" = "3"
  test "$(redis-cli -p "$PORT" --raw OBJECT ENCODING set)" = "intset"
  test "$(redis-cli -p "$PORT" SADD set 01)" = "1"
  test "$(redis-cli -p "$PORT" --raw OBJECT ENCODING set)" = "hashtable"

  test "$(redis-cli -p "$PORT" ZADD zset 1 a 2 b 3 c 4 d 5 e 6 f 7 g 8 h 9 i)" = "9"
  test "$(redis-cli -p "$PORT" --raw OBJECT ENCODING zset)" = "skiplist"
  test "$(redis-cli -p "$PORT" ZRANK zset e)" = "4"

  redis-cli -p "$PORT" SET temp yes EX 1 >/dev/null
  echo "smoke test passed with redis-cli (engine=$NETWORK_ENGINE)"
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
assert command("SET", "encoded", "42") == b"+OK\r\n"
assert command("OBJECT", "ENCODING", "encoded") == b"$3\r\nint\r\n"
assert command("SET", "padded", "001") == b"+OK\r\n"
assert command("OBJECT", "ENCODING", "padded") == b"$3\r\nraw\r\n"

assert command("RPUSH", "list", "a", "b", "c", "d", "e", "f", "g", "h", "i") == b":9\r\n"
assert command("LLEN", "list") == b":9\r\n"
assert command("OBJECT", "ENCODING", "list") == b"$9\r\nquicklist\r\n"

assert command("HSET", "hash", "f1", "v1", "f2", "v2") == b":2\r\n"
assert command("HGET", "hash", "f2") == b"$2\r\nv2\r\n"

assert command("SADD", "set", "1", "2", "3") == b":3\r\n"
assert command("OBJECT", "ENCODING", "set") == b"$6\r\nintset\r\n"
assert command("SADD", "set", "01") == b":1\r\n"
assert command("OBJECT", "ENCODING", "set") == b"$9\r\nhashtable\r\n"

assert command("ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f", "7", "g", "8", "h", "9", "i") == b":9\r\n"
assert command("OBJECT", "ENCODING", "zset") == b"$8\r\nskiplist\r\n"
assert command("ZRANK", "zset", "e") == b":4\r\n"
print("smoke test passed with Python RESP client")
PY
fi
