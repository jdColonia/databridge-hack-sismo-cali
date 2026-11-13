#!/usr/bin/env bash
# Sube las referencias oficiales (descargadas con scripts/download_referencias.py)
# al volumen de Unity Catalog. Requiere haber corrido antes el setup (setup_volume).
#   uv run --no-project --with requests python -I scripts/download_referencias.py
#   DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/upload_referencias.sh
set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
CATALOG="${CATALOG:-workspace}"
SCHEMA="${SCHEMA:-default}"
VOLUME="${VOLUME:-el_equipo}"

SRC="data/external/referencias"
DEST="dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/externos/referencias"

databricks fs mkdir "${DEST}" -p "$PROFILE" || true

for f in comunas.geojson barrios_divipola.geojson catastro_terrenos_predios.json \
         catastro_barrios.json manifest.json; do
  databricks fs cp "${SRC}/${f}" "${DEST}/${f}" --overwrite -p "$PROFILE"
done
