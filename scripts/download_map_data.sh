#!/usr/bin/env bash
# Descarga los GeoJSON del mapa desde el volumen de Unity Catalog a map/data/.
# Uso local:  DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/download_map_data.sh
# Desarrollo: SCHEMA=dev_<usuario> DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/download_map_data.sh
# En CI:      DATABRICKS_HOST y DATABRICKS_TOKEN como variables de entorno (sin perfil):
#             bash scripts/download_map_data.sh
set -euo pipefail

CATALOG="${CATALOG:-workspace}"
SCHEMA="${SCHEMA:-default}"
VOLUME="${VOLUME:-el_equipo}"

# El perfil solo se pasa si la variable esta definida (en CI no hay ~/.databrickscfg).
PROFILE_ARGS=()
if [ -n "${DATABRICKS_CONFIG_PROFILE:-}" ]; then
  PROFILE_ARGS=(-p "$DATABRICKS_CONFIG_PROFILE")
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/map/data"

FILES=(gold_el_equipo_map_buildings gold_el_equipo_map_seismic_zones gold_el_equipo_map_facilities)

for f in "${FILES[@]}"; do
  databricks fs cp \
    "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/export/${f}.geojson" \
    "$ROOT/map/data/${f}.geojson" \
    --overwrite ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}
done

for f in "${FILES[@]}"; do
  if [ ! -s "$ROOT/map/data/${f}.geojson" ]; then
    echo "ERROR: falta o esta vacio $ROOT/map/data/${f}.geojson" >&2
    exit 1
  fi
done
echo "Listo: $ROOT/map/data"
