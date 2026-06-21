#!/usr/bin/env bash
#
# stream_producer.sh — Producteur de flux pour l'extension streaming
# ------------------------------------------------------------------
# Découpe data/online_retail.csv en micro-lots et les dépose un par un dans
# streaming_input/ (avec un délai), pour simuler un flux de ventes en
# temps réel consommé par src/streaming.py.
#
# Écriture atomique : chaque lot est d'abord écrit sous un nom préfixé par "_"
# (ignoré par Spark) puis renommé, afin d'éviter la lecture de fichiers partiels.
#
# Usage : scripts/stream_producer.sh [chunk_size] [delay_sec] [max_chunks]
#   chunk_size  : lignes par lot          (défaut 20000)
#   delay_sec   : délai entre deux lots   (défaut 5)
#   max_chunks  : nombre de lots à émettre (défaut 8, 0 = tous)
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV_FILE="${PROJECT_DIR}/data/online_retail.csv"
STREAM_DIR="${PROJECT_DIR}/streaming_input"

CHUNK_SIZE="${1:-20000}"
DELAY="${2:-5}"
MAX_CHUNKS="${3:-8}"

if [[ ! -f "${CSV_FILE}" ]]; then
  echo "[producer] ERREUR : ${CSV_FILE} introuvable. Lancez scripts/download_data.sh"
  exit 1
fi

mkdir -p "${STREAM_DIR}"
echo "[producer] Nettoyage de ${STREAM_DIR}..."
rm -f "${STREAM_DIR}"/*.csv "${STREAM_DIR}"/_*.csv 2>/dev/null || true

HEADER="$(head -1 "${CSV_FILE}")"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

echo "[producer] Découpage en lots de ${CHUNK_SIZE} lignes..."
tail -n +2 "${CSV_FILE}" | split -l "${CHUNK_SIZE}" - "${WORK}/chunk_"

emitted=0
for f in "${WORK}"/chunk_*; do
  if [[ "${MAX_CHUNKS}" -ne 0 && "${emitted}" -ge "${MAX_CHUNKS}" ]]; then
    echo "[producer] Limite de ${MAX_CHUNKS} lots atteinte."
    break
  fi
  emitted=$((emitted + 1))
  tmp="${STREAM_DIR}/_part_$(printf '%03d' "${emitted}").csv"
  dst="${STREAM_DIR}/part_$(printf '%03d' "${emitted}").csv"
  { echo "${HEADER}"; cat "${f}"; } > "${tmp}"
  mv "${tmp}" "${dst}"
  echo "[producer] Lot ${emitted} déposé : $(basename "${dst}") ($(wc -l < "${dst}") lignes)"
  sleep "${DELAY}"
done

echo "[producer] Flux terminé (${emitted} lots émis)."
