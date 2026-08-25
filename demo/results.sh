#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic instruments.out \
  --from-beginning \
  --timeout-ms 5000 || true
