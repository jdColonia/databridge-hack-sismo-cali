#!/usr/bin/env bash
# Descarga los GeoJSON del mapa desde el volumen de Unity Catalog a map/data/.
# Uso:  DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/download_map_data.sh
# (en desarrollo: SCHEMA=dev_<usuario> DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/download_map_data.sh)
set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
CATALOG="${CATALOG:-workspace}"
SCHEMA="${SCHEMA:-default}"
VOLUME="${VOLUME:-el_equipo}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/map/data"

for f in gold_el_equipo_map_buildings gold_el_equipo_map_seismic_zones gold_el_equipo_map_facilities; do
  databricks fs cp \
    "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/export/${f}.geojson" \
    "$ROOT/map/data/${f}.geojson" \
    --overwrite -p "$PROFILE"
done
echo "Listo: $ROOT/map/data"
