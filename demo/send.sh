#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

scenario="${1:-allianz}"
message="demo/messages/${scenario}.json"

if [ ! -f "$message" ]; then
  echo "Unknown scenario: $scenario" >&2
  echo "Choose one of: allianz, totalenergies, philips" >&2
  exit 2
fi

docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic instruments.in < "$message"

echo "Sent $scenario ($(sed -n 's/.*\"messageId\":\"\([^\"]*\)\".*/\1/p' "$message"))"
