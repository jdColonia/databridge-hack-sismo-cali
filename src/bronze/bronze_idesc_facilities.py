# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: equipamientos de Cali (IDESC)
# MAGIC **Fuente:** capas oficiales del GeoServer del IDESC (Alcaldía de Santiago de Cali), servicio WFS `https://ws-idesc.cali.gov.co/geoserver/ows`.
# MAGIC
# MAGIC | Clave | Capa | Contenido |
# MAGIC | --- | --- | --- |
# MAGIC | `health` | `pot_2014:eqp_uco_salud` | Equipamientos de salud (POT 2014, sin fecha propia) |
# MAGIC | `security` | `pot_2014:eqp_uba_seguridad_ciudadana` | CAI, estaciones de policía, bomberos, Defensa Civil (POT 2014) |
# MAGIC | `schools` | `educacion:esf_establecimiento_educativo` | Sedes educativas (actualizada 2023–2024) |
# MAGIC | `public_spaces` | `dapm:pdt_pmep_encuentro` | Espacio público de encuentro, PMEP 2021–2023 |
# MAGIC
# MAGIC **Decisiones:**
# MAGIC - Bronze guarda el dato **tal cual llega**: una fila por elemento, todas las propiedades originales como texto (nombres originales, `None` queda `null`) más `geometry` como texto GeoJSON.
# MAGIC   La limpieza y los tipos son trabajo de silver.
# MAGIC - Se descarga la **capa completa**, sin filtro `bbox` en la URL: con WFS 2.0 y `EPSG:4326` el orden de ejes hace que el filtro devuelva 0 elementos. El recorte se hace en silver.
# MAGIC - Trazabilidad: `feature_id` (id del WFS), `_source_url`, `_queried_at` y `_load_origin` (`wfs` o `volume_copy`).
# MAGIC - El crudo se guarda primero en el volumen de trabajo (`externos/idesc_<clave>.geojson`) y la tabla se arma desde ese archivo.
# MAGIC - **Respaldo por capa:** si el GeoServer no responde, se lee la copia subida a mano en `/Volumes/<catalog>/<raw_schema>/<volume>/externos/idesc_<clave>.geojson` (solo lectura).
# MAGIC
# MAGIC Solo usa librerías estándar de Python y `requests`: no hace falta `%pip install`.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

import requests
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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

WFS_URL = "https://ws-idesc.cali.gov.co/geoserver/ows"
USER_AGENT = "el-equipo-databridge-hack"

# clave -> (capa WFS, sufijo de la tabla)
LAYERS = {
    "health": ("pot_2014:eqp_uco_salud", "idesc_health_facilities"),
    "security": ("pot_2014:eqp_uba_seguridad_ciudadana", "idesc_security_facilities"),
    "schools": ("educacion:esf_establecimiento_educativo", "idesc_schools"),
    "public_spaces": ("dapm:pdt_pmep_encuentro", "idesc_public_spaces"),
}

ext_dir = f"/Volumes/{catalog}/{schema}/{volume}/externos"           # volumen de trabajo (se escribe)
raw_ext_dir = f"/Volumes/{catalog}/{raw_schema}/{volume}/externos"   # copia manual (solo lectura)
os.makedirs(ext_dir, exist_ok=True)

def source_url(layer):
    """URL completa de la capa (sin bbox: el orden de ejes lo rompe)."""
    return (f"{WFS_URL}?service=WFS&version=2.0.0&request=GetFeature"
            f"&typeNames={layer}&outputFormat=application/json&srsName=EPSG:4326")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descarga del crudo
# MAGIC Por capa: primero el WFS; si falla, la copia del volumen compartido. Se valida que la respuesta sea un `FeatureCollection`
# MAGIC antes de guardarla (un GeoServer caído puede responder 200 con una página HTML o un XML de error).

# COMMAND ----------

def is_feature_collection(content: bytes) -> bool:
    try:
        return json.loads(content).get("type") == "FeatureCollection"
    except (ValueError, AttributeError):
        return False

def fetch_layer(key, layer):
    """Devuelve (geojson, load_origin, queried_at, raw_path) para una capa."""
    filename = f"idesc_{key}.geojson"
    raw_path = f"{ext_dir}/{filename}"
    backup_path = f"{raw_ext_dir}/{filename}"
    url = source_url(layer)
    try:
        resp = requests.get(url, timeout=120, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        if not is_feature_collection(resp.content):
            raise ValueError(f"respuesta no es GeoJSON ({resp.headers.get('content-type')})")
        with open(raw_path, "wb") as fh:
            fh.write(resp.content)
        origin, queried_at = "wfs", datetime.now(timezone.utc)
        print(f"[{key}] descargado del WFS: {raw_path} ({len(resp.content) / 1024:.0f} KB)")
    except Exception as e:
        print(f"AVISO [{key}]: el WFS del IDESC no respondió ({e}). Se intenta la copia del volumen compartido.")
        if not os.path.exists(backup_path):
            raise FileNotFoundError(
                f"No hay copia de respaldo en {backup_path}. Descarga la capa localmente desde {url} y súbela con "
                f"`databricks fs cp <archivo> dbfs:{backup_path} -p el-equipo`."
            )
        raw_path, origin = backup_path, "volume_copy"
        queried_at = datetime.fromtimestamp(os.path.getmtime(backup_path), tz=timezone.utc)
        print(f"[{key}] usando copia: {raw_path} (modificada {queried_at:%Y-%m-%d %H:%M} UTC)")
    with open(raw_path, "r", encoding="utf-8") as fh:
        gj = json.load(fh)
    return gj, origin, queried_at, raw_path

raw = {key: fetch_layer(key, layer) for key, (layer, _) in LAYERS.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas bronze
# MAGIC Una tabla por capa. Columnas: `feature_id`, todas las propiedades originales (texto, en el orden en que aparecen), `geometry` (GeoJSON texto)
# MAGIC y la trazabilidad (`_source_url`, `_queried_at`, `_load_origin`).

# COMMAND ----------

def prop_columns(features):
    """Unión ordenada (por aparición) de las propiedades de todos los elementos."""
    cols = []
    seen = set()
    for f in features:
        for k in (f.get("properties") or {}):
            if k not in seen:
                seen.add(k)
                cols.append(k)
    return cols

def to_text(v):
    return None if v is None else (v if isinstance(v, str) else str(v))

def build_table(key, layer, suffix):
    gj, origin, queried_at, _ = raw[key]
    feats = gj["features"]
    cols = prop_columns(feats)
    url = source_url(layer)
    q = queried_at.replace(tzinfo=None)
    sch = StructType(
        [StructField("feature_id", StringType())]
        + [StructField(c, StringType()) for c in cols]
        + [StructField("geometry", StringType()), StructField("_source_url", StringType()),
           StructField("_queried_at", TimestampType()), StructField("_load_origin", StringType())]
    )
    rows = []
    for f in feats:
        props = f.get("properties") or {}
        rows.append(tuple(
            [f.get("id")]
            + [to_text(props.get(c)) for c in cols]
            + [json.dumps(f.get("geometry")), url, q, origin]
        ))
    table = f"{catalog}.{schema}.bronze_{team}_{suffix}"
    (spark.createDataFrame(rows, schema=sch).write.format("delta")
          .mode("overwrite").option("overwriteSchema", "true").saveAsTable(table))
    print(f"Guardada {table}: {len(rows)} filas, {len(cols)} propiedades (origen: {origin})")
    return table

tables = {key: build_table(key, layer, suffix) for key, (layer, suffix) in LAYERS.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación
# MAGIC - El conteo de cada tabla coincide con `totalFeatures` del WFS (cuando viene en la respuesta).
# MAGIC - `feature_id` es único y no nulo; toda fila tiene geometría.
# MAGIC - Las coordenadas están en el rango de Cali y en orden `[lon, lat]` (lon −76,7 a −76,4; lat 3,2 a 3,6).

# COMMAND ----------

def coords(c):
    if isinstance(c[0], (int, float)):
        yield c
    else:
        for k in c:
            yield from coords(k)

for key, table in tables.items():
    gj = raw[key][0]
    df = spark.table(table)
    n = df.count()
    if gj.get("totalFeatures") is not None:
        assert n == gj["totalFeatures"], (key, n, gj["totalFeatures"])
    assert n == len(gj["features"]), (key, n, len(gj["features"]))
    assert df.filter("feature_id IS NULL").count() == 0, f"{key}: feature_id nulo"
    assert df.select("feature_id").distinct().count() == n, f"{key}: feature_id repetido"
    assert df.filter("geometry IS NULL OR geometry = 'null'").count() == 0, f"{key}: geometría nula"

    lons, lats = [], []
    for f in gj["features"]:
        for lon, lat, *_ in coords(f["geometry"]["coordinates"]):
            lons.append(lon)
            lats.append(lat)
    print(f"[{key}] {n} filas | lon {min(lons):.4f} a {max(lons):.4f} | lat {min(lats):.4f} a {max(lats):.4f} "
          f"| origen {raw[key][1]}")
    assert -76.7 <= min(lons) and max(lons) <= -76.4, f"{key}: longitudes fuera de Cali (¿ejes invertidos?)"
    assert 3.2 <= min(lats) and max(lats) <= 3.6, f"{key}: latitudes fuera de Cali (¿ejes invertidos?)"

display(spark.table(tables["security"]).groupBy("tipo").count().orderBy("tipo"))
