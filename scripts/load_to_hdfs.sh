#!/usr/bin/env bash
#
# load_to_hdfs.sh — PARTIE 2 : stockage du dataset dans Hadoop HDFS
# -----------------------------------------------------------------
# Copie data/online_retail.csv dans le namenode puis l'enregistre dans HDFS
# sous /data/online_retail.csv, et vérifie le résultat.
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV_FILE="${PROJECT_DIR}/data/online_retail.csv"
HDFS_DIR="/data"
HDFS_PATH="${HDFS_DIR}/online_retail.csv"

if [[ ! -f "${CSV_FILE}" ]]; then
  echo "[hdfs] ERREUR : ${CSV_FILE} introuvable. Lancez d'abord scripts/download_data.sh"
  exit 1
fi

echo "[hdfs] Attente de la disponibilité du namenode (sortie du safe mode)..."
for i in $(seq 1 30); do
  if docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; then
    echo "[hdfs] Namenode prêt."
    break
  fi
  echo "  ... tentative ${i}/30"
  sleep 5
done

echo "[hdfs] Copie du CSV dans le conteneur namenode..."
docker cp "${CSV_FILE}" namenode:/tmp/online_retail.csv

echo "[hdfs] Création du répertoire HDFS ${HDFS_DIR}..."
docker exec namenode hdfs dfs -mkdir -p "${HDFS_DIR}"

echo "[hdfs] Téléversement dans HDFS -> ${HDFS_PATH}..."
docker exec namenode hdfs dfs -put -f /tmp/online_retail.csv "${HDFS_PATH}"

echo ""
echo "[hdfs] Contenu de ${HDFS_DIR} :"
docker exec namenode hdfs dfs -ls -h "${HDFS_DIR}"

echo ""
echo "[hdfs] Aperçu (5 premières lignes depuis HDFS) :"
docker exec namenode bash -c "hdfs dfs -cat ${HDFS_PATH} | head -5"

echo ""
echo "[hdfs] OK — dataset stocké dans HDFS : hdfs://namenode:9000${HDFS_PATH}"
echo "[hdfs] Interface web HDFS : http://localhost:9871"
