#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

docker compose up -d --wait kafka refdata
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic instruments.in \
  --partitions 1 \
  --replication-factor 1
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic instruments.out \
  --partitions 1 \
  --replication-factor 1

echo "Demo services are ready. Start the app with:"
echo "mvn spring-boot:run -Dspring-boot.run.profiles=demo"
