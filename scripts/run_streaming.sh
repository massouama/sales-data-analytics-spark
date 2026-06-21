#!/usr/bin/env bash
#
# run_streaming.sh — Extension : lance l'application Spark Structured Streaming
# ----------------------------------------------------------------------------
# Démarre src/streaming.py qui surveille streaming_input/. Dans un AUTRE
# terminal, lancez scripts/stream_producer.sh pour alimenter le flux.
#
# Le checkpoint est réinitialisé à chaque lancement pour repartir d'un état propre.
#
set -euo pipefail

echo "[stream] Réinitialisation du checkpoint..."
docker exec spark-master rm -rf /tmp/streaming_checkpoint 2>/dev/null || true

echo "[stream] Démarrage de l'application de streaming (Ctrl+C pour arrêter)..."
echo "[stream] >>> Dans un autre terminal : scripts/stream_producer.sh"
echo ""

docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name SalesDataAnalyticsStreaming \
  /app/src/streaming.py \
  --input /app/streaming_input
