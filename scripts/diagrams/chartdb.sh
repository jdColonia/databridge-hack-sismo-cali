#!/usr/bin/env bash
# Levanta ChartDB local (self-hosted) para importar y exportar los diagramas DBML.
# Uso: bash scripts/diagrams/chartdb.sh [up|down|status]
# Abre http://localhost:${CHARTDB_PORT:-8080}. Sin cuentas: los diagramas viven en el
# navegador (IndexedDB); la fuente de verdad es el .dbml en docs/diagrams/.
set -euo pipefail

IMAGE="ghcr.io/chartdb/chartdb:${CHARTDB_VERSION:-1.21.0}"
NAME="chartdb-el-equipo"
PORT="${CHARTDB_PORT:-8080}"

case "${1:-up}" in
  up)
    if docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
      echo "ChartDB ya corre en http://localhost:${PORT}"
      exit 0
    fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    # DISABLE_ANALYTICS: sin telemetria. Sin OPENAI_API_KEY: las funciones de IA quedan
    # apagadas, no hacen falta para importar DBML ni exportar imagenes.
    docker run -d --name "$NAME" -p "${PORT}:80" -e DISABLE_ANALYTICS=true "$IMAGE" >/dev/null
    echo "ChartDB corriendo en http://localhost:${PORT} (${IMAGE})"
    ;;
  down)
    docker rm -f "$NAME" >/dev/null && echo "ChartDB detenido"
    ;;
  status)
    docker ps --filter "name=^${NAME}$" --format '{{.Names}} {{.Image}} {{.Ports}}'
    ;;
  *)
    echo "Uso: $0 [up|down|status]" >&2
    exit 1
    ;;
esac
