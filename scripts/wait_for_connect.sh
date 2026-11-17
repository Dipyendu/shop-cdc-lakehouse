#!/usr/bin/env bash
# Wait up to 120 s for the Kafka Connect REST API on 127.0.0.1:8083.
for _ in $(seq 40); do
  curl -fs --max-time 3 127.0.0.1:8083/ >/dev/null && exit 0
  sleep 3
done
echo "Kafka Connect did not answer on 127.0.0.1:8083 within 120 s" >&2
exit 1
