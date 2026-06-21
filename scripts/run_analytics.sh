#!/usr/bin/env bash
#
# run_analytics.sh — PARTIE 1 : exécute l'application PySpark batch
# ----------------------------------------------------------------
# Soumet src/analytics.py au cluster Spark. Lit le dataset depuis HDFS et
# écrit les résultats dans output/ (monté dans les conteneurs Spark).
#
set -euo pipefail

INPUT="${1:-hdfs://namenode:9000/data/online_retail.csv}"

echo "[run] Soumission de analytics.py au cluster Spark..."
echo "[run] Source : ${INPUT}"

docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name SalesDataAnalytics \
  /app/src/analytics.py \
  --input "${INPUT}" \
  --output /app/output

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo ""
echo "[run] Génération du tableau de bord HTML..."
python3 "${PROJECT_DIR}/scripts/build_dashboard.py" || \
  echo "[run] (dashboard non généré : python3 requis sur l'hôte)"

echo ""
echo "[run] Terminé. Résultats CSV disponibles dans ./output/"
ls -1 "${PROJECT_DIR}/output" 2>/dev/null || true
echo ""
echo "[run] Tableau de bord visuel : ouvrez  output/dashboard.html"
echo "[run]   (ex. :  open output/dashboard.html )"
