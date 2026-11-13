# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: contexto sísmico del USGS y cifras oficiales de impacto
# MAGIC **Fuentes:**
# MAGIC - **USGS, evento `us6000tjl2`** (M7,4, 10 ago 2026, 116 km de profundidad): API FDSN `event/1/query`.
# MAGIC - **Secuencia de réplicas:** misma API, con el círculo oficial de la secuencia (centro 4,4791 / −76,5739, radio 125,4 km), M ≥ 3,5 desde el 10 ago 2026.
# MAGIC   La M5,4 del 3 nov (Rioblanco, Tolima) queda fuera del círculo y **no es réplica**.
# MAGIC - **Pronóstico oficial de réplicas** (producto `oaf`, `forecast.json`, modelo ETAS): probabilidad y número esperado de réplicas por ventana de tiempo y magnitud mínima.
# MAGIC - **Cifras oficiales de impacto** (Alcaldía de Cali y UNGRD): CSV curado a mano en `reference/official_impact.csv`, con fuente, fecha de corte y estado de verificación.
# MAGIC
# MAGIC **Decisiones:**
# MAGIC - Bronze guarda los datos sin interpretar: una fila por evento y una fila por (ventana, magnitud mínima) del pronóstico. Solo se aplanan las estructuras anidadas y se pasan los milisegundos epoch a `TIMESTAMP` UTC.
# MAGIC - El crudo se guarda primero en el volumen de trabajo (`externos/usgs_sequence.geojson`, `externos/usgs_forecast.json`); las tablas se arman desde esos archivos.
# MAGIC - **Respaldo:** si la API no responde, se lee la copia del volumen compartido (`/Volumes/<catalog>/<raw_schema>/<volume>/externos/`), que solo se lee.
# MAGIC - Metadatos: `_source_url`, `_queried_at` (UTC) y `_load_origin` (`api` o `volume_copy`).
# MAGIC - Las cifras oficiales conservan **ambas** definiciones de colapso (Alcaldía: 24 colapsos totales; UNGRD: 308 edificaciones colapsadas) y su estado de verificación.
# MAGIC
# MAGIC Solo usa librerías ya disponibles en el entorno (estándar de Python, `requests` y `pandas`): no hace falta `%pip install`.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType,
                               BooleanType, TimestampType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("raw_schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw_schema = dbutils.widgets.get("raw_schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

MAINSHOCK_ID = "us6000tjl2"
EVENT_URL = f"https://earthquake.usgs.gov/fdsnws/event/1/query?eventid={MAINSHOCK_ID}&format=geojson"
SEQUENCE_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-08-10"
    "&latitude=4.4791&longitude=-76.5739&maxradiuskm=125.4&minmagnitude=3.5"
)
SEQUENCE_FILE = "usgs_sequence.geojson"
FORECAST_FILE = "usgs_forecast.json"

ext_dir = f"/Volumes/{catalog}/{schema}/{volume}/externos"                 # volumen de trabajo (se escribe)
fallback_dir = f"/Volumes/{catalog}/{raw_schema}/{volume}/externos"        # copia manual (solo lectura)
t_events = f"{catalog}.{schema}.bronze_{team}_usgs_earthquakes"
t_forecast = f"{catalog}.{schema}.bronze_{team}_usgs_aftershock_forecast"
t_impact = f"{catalog}.{schema}.bronze_{team}_manual_official_impact"
os.makedirs(ext_dir, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descarga del crudo
# MAGIC Primero la API; si falla, la copia del volumen compartido. Se valida la forma de la respuesta antes de guardarla.
# MAGIC La URL del pronóstico no es fija (lleva la fecha de creación): se lee del producto `oaf` del evento.

# COMMAND ----------

def save_raw(content: bytes, name: str) -> str:
    path = f"{ext_dir}/{name}"
    with open(path, "wb") as fh:
        fh.write(content)
    return path

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

# --- Secuencia de sismos ---
seq_origin, seq_queried = None, None
seq_path = f"{ext_dir}/{SEQUENCE_FILE}"
try:
    r = requests.get(SEQUENCE_URL, timeout=120)
    r.raise_for_status()
    assert r.json().get("type") == "FeatureCollection", "la respuesta no es un FeatureCollection"
    seq_path = save_raw(r.content, SEQUENCE_FILE)
    seq_origin, seq_queried = "api", datetime.now(timezone.utc)
    print(f"Secuencia descargada: {seq_path} ({len(r.content) / 1024:.0f} KB)")
except Exception as e:
    print(f"AVISO: la API del USGS no respondió para la secuencia ({e}). Se intenta la copia del volumen compartido.")
    seq_path = f"{fallback_dir}/{SEQUENCE_FILE}"
    if not os.path.exists(seq_path):
        raise FileNotFoundError(f"No hay copia de respaldo en {seq_path}. Descarga {SEQUENCE_URL} y súbela a esa ruta.")
    seq_origin = "volume_copy"
    seq_queried = datetime.fromtimestamp(os.path.getmtime(seq_path), tz=timezone.utc)

# --- Pronóstico de réplicas ---
fc_url, fc_origin, fc_queried = None, None, None
fc_path = f"{ext_dir}/{FORECAST_FILE}"
try:
    ev_resp = requests.get(EVENT_URL, timeout=120)
    ev_resp.raise_for_status()
    fc_url = ev_resp.json()["properties"]["products"]["oaf"][0]["contents"]["forecast.json"]["url"]
    r = requests.get(fc_url, timeout=120)
    r.raise_for_status()
    assert "forecast" in r.json(), "forecast.json sin la clave 'forecast'"
    fc_path = save_raw(r.content, FORECAST_FILE)
    fc_origin, fc_queried = "api", datetime.now(timezone.utc)
    print(f"Pronóstico descargado: {fc_path} ({fc_url})")
except Exception as e:
    print(f"AVISO: no se pudo obtener el pronóstico del USGS ({e}). Se intenta la copia del volumen compartido.")
    fc_path = f"{fallback_dir}/{FORECAST_FILE}"
    if not os.path.exists(fc_path):
        raise FileNotFoundError(f"No hay copia de respaldo en {fc_path}. Descarga el forecast.json del evento {MAINSHOCK_ID} y súbelo a esa ruta.")
    fc_url = fc_url or EVENT_URL
    fc_origin = "volume_copy"
    fc_queried = datetime.fromtimestamp(os.path.getmtime(fc_path), tz=timezone.utc)

seq = load_json(seq_path)
fc = load_json(fc_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de sismos
# MAGIC Una fila por evento (sismo principal + réplicas). `event_time` sale de milisegundos epoch UTC. La profundidad es el tercer valor de la coordenada.
# MAGIC `is_mainshock` marca el sismo principal (`us6000tjl2`).

# COMMAND ----------

def ms_to_ts(ms):
    """Milisegundos epoch -> datetime UTC sin zona (Spark TimestampType)."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)

def to_float(v):
    return None if v is None else float(v)

def to_long(v):
    return None if v is None else int(v)

events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("magnitude", DoubleType()),
    StructField("magnitude_type", StringType()),
    StructField("place", StringType()),
    StructField("event_time", TimestampType()),
    StructField("depth_km", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("event_type", StringType()),
    StructField("status", StringType()),
    StructField("url", StringType()),
    StructField("is_mainshock", BooleanType()),
    StructField("_source_url", StringType()),
    StructField("_queried_at", TimestampType()),
    StructField("_load_origin", StringType()),
])

q_naive = seq_queried.replace(tzinfo=None)
event_rows = []
for f in seq["features"]:
    p = f.get("properties") or {}
    lon, lat, depth = (list(f["geometry"]["coordinates"]) + [None, None, None])[:3]
    event_rows.append((
        f["id"], to_float(p.get("mag")), p.get("magType"), p.get("place"), ms_to_ts(p.get("time")),
        to_float(depth), to_float(lon), to_float(lat), p.get("type"), p.get("status"), p.get("url"),
        f["id"] == MAINSHOCK_ID, SEQUENCE_URL, q_naive, seq_origin,
    ))

df_events = spark.createDataFrame(event_rows, schema=events_schema)
(df_events.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_events))
print(f"Guardada {t_events} (origen: {seq_origin})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla del pronóstico de réplicas
# MAGIC El `forecast.json` trae una lista de ventanas (`1 Day`, `1 Week`, `1 Month`, `1 Year`) y, en cada una, una entrada por magnitud mínima (3 a 7) con:
# MAGIC `probability` (0 a 1, tal como la publica el USGS), `median` y el rango del 95 % (`p95minimum`–`p95maximum`) del número esperado de réplicas.
# MAGIC Se aplana a una fila por (ventana, magnitud mínima); `forecast_created_at` y `advisory_time_frame` se repiten en cada fila.
# MAGIC Se descartan los arreglos auxiliares de gráficos (`fractileValues`, `barPercentages`).

# COMMAND ----------

forecast_schema = StructType([
    StructField("forecast_created_at", TimestampType()),
    StructField("advisory_time_frame", StringType()),
    StructField("window_label", StringType()),
    StructField("window_start_at", TimestampType()),
    StructField("window_end_at", TimestampType()),
    StructField("min_magnitude", DoubleType()),
    StructField("probability", DoubleType()),
    StructField("expected_count_median", LongType()),
    StructField("expected_count_p95_min", LongType()),
    StructField("expected_count_p95_max", LongType()),
    StructField("_source_url", StringType()),
    StructField("_queried_at", TimestampType()),
    StructField("_load_origin", StringType()),
])

fq_naive = fc_queried.replace(tzinfo=None)
fc_rows = []
for w in fc["forecast"]:
    for b in w["bins"]:
        fc_rows.append((
            ms_to_ts(fc.get("creationTime")), fc.get("advisoryTimeFrame"), w["label"],
            ms_to_ts(w.get("timeStart")), ms_to_ts(w.get("timeEnd")),
            to_float(b["magnitude"]), to_float(b.get("probability")),
            to_long(b.get("median")), to_long(b.get("p95minimum")), to_long(b.get("p95maximum")),
            fc_url, fq_naive, fc_origin,
        ))

df_fc = spark.createDataFrame(fc_rows, schema=forecast_schema)
(df_fc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_forecast))
print(f"Guardada {t_forecast} (origen: {fc_origin})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cifras oficiales de impacto (CSV versionado en el repo)
# MAGIC `reference/official_impact.csv` se despliega junto con el notebook. Se lee relativo a la carpeta del notebook (`../../reference/`);
# MAGIC si el directorio de trabajo no es esa carpeta se prueba con la ruta del notebook bajo `/Workspace`.
# MAGIC Todas las columnas quedan como texto salvo `value` (`double`). Las cifras sin fuente confirmada quedan como `por verificar` y sin URL: no se inventan.

# COMMAND ----------

rel = os.path.join("..", "..", "reference", "official_impact.csv")
candidates = [os.path.abspath(rel)]
try:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    candidates.append(os.path.abspath(os.path.join("/Workspace" + os.path.dirname(nb), rel)))
except Exception:
    pass
csv_path = next((p for p in candidates if os.path.exists(p)), None)
if csv_path is None:
    raise FileNotFoundError(
        f"No se encontró official_impact.csv; se probó: {candidates}. Ejecuta `databricks bundle deploy` para subir `reference/`."
    )

pdf = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
pdf["value"] = pd.to_numeric(pdf["value"]).astype(float)
pdf["_source_url"] = "reference/official_impact.csv"
impact_queried = datetime.now(timezone.utc).replace(tzinfo=None)  # se agrega por fila: un Timestamp de pandas sin zona rompe createDataFrame

text_cols = ["unit", "scope", "source_name", "source_url", "cutoff_date", "verification_status", "note"]
impact_schema = StructType(
    [StructField("metric_code", StringType()), StructField("metric_name", StringType()), StructField("value", DoubleType())]
    + [StructField(c, StringType()) for c in text_cols]
    + [StructField("_source_url", StringType()), StructField("_queried_at", TimestampType())]
)
cols = [f.name for f in impact_schema.fields]
rows = [tuple(rec) + (impact_queried,) for rec in pdf[cols[:-1]].itertuples(index=False, name=None)]
df_impact = spark.createDataFrame(rows, schema=impact_schema)
(df_impact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_impact))
print(f"Guardada {t_impact}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación
# MAGIC - 61 eventos al 13 nov (sismo principal + 60 réplicas M ≥ 3,5) con exactamente 1 sismo principal; si hubo réplicas nuevas se acepta `>= 61` y se imprime el conteo.
# MAGIC - La mayor réplica es M5,0 (`us6000tjm4`, 10 ago). Coordenadas dentro del rango esperado.
# MAGIC - Pronóstico: 4 ventanas × 5 magnitudes = 20 filas. Se imprime P(M ≥ 5) para la ventana de 1 mes (≈ 31,5 %).
# MAGIC - Cifras oficiales: 6 filas, una por métrica, con `value` numérico.

# COMMAND ----------

ev = spark.table(t_events)
n = ev.count()
assert n >= 61, n
if n != 61:
    print(f"AVISO: la API devolvió {n} eventos (se esperaban 61); probablemente hay réplicas nuevas.")
assert ev.filter("is_mainshock").count() == 1
assert ev.filter(f"is_mainshock AND event_id = '{MAINSHOCK_ID}'").count() == 1
assert ev.filter("event_id IS NULL OR magnitude IS NULL OR event_time IS NULL").count() == 0
max_aftershock = ev.filter("NOT is_mainshock").agg({"magnitude": "max"}).collect()[0][0]
assert max_aftershock == 5.0, max_aftershock
bad = ev.filter("lon NOT BETWEEN -78 AND -75 OR lat NOT BETWEEN 3 AND 6").count()
assert bad == 0, f"{bad} eventos fuera del rango esperado (¿ejes invertidos?)"
print(f"Eventos: {n} | mayor réplica: M{max_aftershock}")

fcs = spark.table(t_forecast)
assert fcs.count() == 20, fcs.count()
assert fcs.filter("probability < 0 OR probability > 1").count() == 0
p5 = fcs.filter("window_label = '1 Month' AND min_magnitude = 5.0").select("probability").collect()[0][0]
print(f"P(M>=5) en la ventana de 1 mes: {p5:.1%} (esperada ~31,5 %)")

imp = spark.table(t_impact)
assert imp.count() == 6, imp.count()
assert imp.filter("value IS NULL").count() == 0
display(ev.orderBy("event_time").limit(5))
display(fcs.filter("window_label = '1 Month'").orderBy("min_magnitude"))
display(imp.select("metric_code", "value", "scope", "source_name", "cutoff_date", "verification_status"))
