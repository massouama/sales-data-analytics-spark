#!/usr/bin/env bash
#
# setup_all.sh — Mise en route complète, de bout en bout
# ------------------------------------------------------
# 1. Télécharge et prépare le dataset (si nécessaire).
# 2. Démarre la stack Docker (HDFS + Spark).
# 3. Stocke le dataset dans HDFS (Partie 2).
# 4. Exécute l'application d'analyse PySpark (Partie 1).
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

echo "############################################################"
echo "#  Sales Data Analytics — Mise en route complète           #"
echo "############################################################"

echo ""
echo "### Étape 1/4 — Préparation du dataset"
bash scripts/download_data.sh

echo ""
echo "### Étape 2/4 — Démarrage de la stack Docker (HDFS + Spark)"
docker compose up -d

echo "[setup] Attente du démarrage des services (~30 s)..."
sleep 20

echo ""
echo "### Étape 3/4 — Stockage du dataset dans HDFS"
bash scripts/load_to_hdfs.sh

echo ""
echo "### Étape 4/4 — Exécution des analyses Spark"
bash scripts/run_analytics.sh

echo ""
echo "############################################################"
echo "#  Terminé. Interfaces web :                               #"
echo "#   - HDFS  (Namenode)   : http://localhost:9871           #"
echo "#   - Spark (Master)     : http://localhost:8082           #"
echo "#  Résultats : ./output/                                   #"
echo "############################################################"
