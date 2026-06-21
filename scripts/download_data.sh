#!/usr/bin/env bash
#
# download_data.sh
# -----------------
# Télécharge le jeu de données "Online Retail" depuis le UCI Machine Learning
# Repository, l'extrait, puis le convertit en CSV propre exploitable par Spark.
#
# Source : https://archive.ics.uci.edu/dataset/352/online+retail
# Résultat : data/online_retail.csv  (~45 Mo, 541 909 transactions)
#
# Le script est idempotent : si le CSV existe déjà il ne refait rien.
#
set -euo pipefail

# Répertoire racine du projet (le parent de scripts/)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${PROJECT_DIR}/data"
ZIP_URL="https://archive.ics.uci.edu/static/public/352/online+retail.zip"
ZIP_FILE="${DATA_DIR}/online_retail.zip"
XLSX_FILE="${DATA_DIR}/Online Retail.xlsx"
CSV_FILE="${DATA_DIR}/online_retail.csv"

mkdir -p "${DATA_DIR}"

if [[ -f "${CSV_FILE}" ]]; then
  echo "[download_data] Le CSV existe déjà : ${CSV_FILE}"
  echo "[download_data] (supprimez-le pour forcer un nouveau téléchargement)"
  exit 0
fi

echo "[download_data] Téléchargement depuis UCI..."
curl -fSL --retry 3 --max-time 300 -o "${ZIP_FILE}" "${ZIP_URL}"

echo "[download_data] Décompression..."
unzip -o "${ZIP_FILE}" -d "${DATA_DIR}"

echo "[download_data] Vérification des dépendances Python (pandas, openpyxl)..."
python3 - <<'PY' || pip3 install --quiet pandas openpyxl
import pandas, openpyxl  # noqa: F401
PY

echo "[download_data] Conversion xlsx -> CSV (peut prendre ~30 s)..."
python3 - "${XLSX_FILE}" "${CSV_FILE}" <<'PY'
import sys
import pandas as pd

xlsx, csv = sys.argv[1], sys.argv[2]
# On force les types pour préserver les codes (StockCode alphanumérique,
# InvoiceNo avec préfixe 'C' pour les annulations) et CustomerID en entier.
df = pd.read_excel(
    xlsx,
    dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": "Int64"},
)
df.to_csv(csv, index=False, date_format="%Y-%m-%d %H:%M:%S")
print(f"[download_data] CSV écrit : {csv} ({len(df):,} lignes)")
PY

echo "[download_data] Terminé."
ls -lh "${CSV_FILE}"
