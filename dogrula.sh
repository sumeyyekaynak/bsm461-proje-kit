#!/usr/bin/env bash
# Kitin ayakta olup olmadığını denetler.
#   ./dogrula.sh          → Kafka, S3, DuckDB (ve açıksa Elasticsearch)   ~10 sn
#   ./dogrula.sh --tam    → ayrıca Spark → S3 / Delta / Kafka              ilk seferde birkaç dk
# Windows: Git Bash ya da WSL içinde çalıştırın (README.md).
set -u
cd "$(dirname "$0")"

if ! docker info > /dev/null 2>&1; then
  echo "❌ Docker çalışmıyor. Docker Desktop'ı açın, simgesi 'running' olunca yeniden deneyin."
  exit 1
fi

echo "── Kapsayıcılar ─────────────────────────────────────────────"
docker compose ps
echo

for s in bsm461-kafka bsm461-s3 bsm461-araclar bsm461-spark; do
  if [ -z "$(docker ps -q -f name=^/${s}$)" ]; then
    echo "❌ $s çalışmıyor. Önce: docker compose up -d"
    exit 1
  fi
done

echo "── Servis denemeleri ────────────────────────────────────────"
docker compose exec -T araclar python araclar/dogrula.py
durum=$?

if [ "${1:-}" = "--tam" ]; then
  echo
  echo "── Spark denemesi (ilk seferde kütüphaneler indirilir) ──────"
  docker compose exec -T spark spark-submit is/dogrula_spark.py 2>&1 | grep -E '✅|🎉|Error|Exception' || durum=1
fi
exit $durum
