#!/usr/bin/env bash
# Sube Datosprediales.json al volumen de Unity Catalog.
# IMPORTANTE: antes hay que correr el setup (tarea setup_volumen) o crear el
# volumen a mano: CREATE VOLUME IF NOT EXISTS workspace.default.el_equipo
set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
CATALOG="${CATALOG:-workspace}"
SCHEMA="${SCHEMA:-default}"
VOLUME="${VOLUME:-el_equipo}"

databricks fs cp data/raw/Datosprediales.json \
  "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/Datosprediales.json" \
  --overwrite -p "$PROFILE"
