# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: microzonificación sísmica de Cali (IDESC)
# MAGIC **Fuente:** capa oficial `idesc:mc_microzonificacion_sismica` del GeoServer del IDESC (Alcaldía de Santiago de Cali).
# MAGIC Estudio de microzonificación INGEOMINAS–DAGMA (2005), adoptado por el Decreto 411.0.20.0158 de 2014. La capa no trae fecha propia:
# MAGIC en la lista de fuentes se cita el estudio y el decreto, no la fecha de descarga.
# MAGIC
# MAGIC **Decisiones:**
# MAGIC - Bronze guarda el dato **tal cual llega**: una fila por polígono (16), las 20 propiedades sin limpiar más `geometry` como texto GeoJSON.
# MAGIC   No se convierten los ceros de las zonas "de estudio particular" ni se descartan columnas basura: eso es trabajo de silver.
# MAGIC - El único ajuste es de **tipo**: el WFS mezcla enteros y decimales en un mismo campo (`sm_etc` = `1` y `0.45`), así que las medidas se guardan como `double`
# MAGIC   para que el esquema no dependa de qué valor llegó primero. Las columnas `*_etl` llegan como texto (`'0.40'`) y se dejan como texto.
# MAGIC - Se agregan columnas de **trazabilidad**: `feature_id` (id del WFS, porque `zona_mzsc` se repite), `_source_url`, `_load_origin` (`wfs` o `volume_copy`) y `_queried_at`.
# MAGIC - El crudo se guarda primero en el volumen de trabajo (`externos/`), y la tabla se arma desde ese archivo.
# MAGIC - **Respaldo:** si el GeoServer no responde (el IDESC anunció suspensión de servicios), se lee una copia subida a mano en
# MAGIC   `/Volumes/<catalog>/<raw_schema>/<volume>/externos/microzonificacion_sismica.geojson`. Ese volumen compartido solo se lee, nunca se escribe.
# MAGIC
# MAGIC Solo usa librerías estándar de Python: no hace falta `%pip install`.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

import requests
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

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

SOURCE_URL = (
    "https://ws-idesc.cali.gov.co/geoserver/ows?service=WFS&version=2.0.0&request=GetFeature"
    "&typeNames=idesc:mc_microzonificacion_sismica&outputFormat=application/json&srsName=EPSG:4326"
)
FILE_NAME = "microzonificacion_sismica.geojson"

ext_dir = f"/Volumes/{catalog}/{schema}/{volume}/externos"           # volumen de trabajo (se escribe)
backup_path = f"/Volumes/{catalog}/{raw_schema}/{volume}/externos/{FILE_NAME}"  # copia manual (solo lectura)
bronze_table = f"{catalog}.{schema}.bronze_{team}_idesc_seismic_zones"
os.makedirs(ext_dir, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descarga del crudo
# MAGIC Primero el WFS; si falla, la copia del volumen compartido. Se valida que la respuesta sea un `FeatureCollection` antes de guardarla
# MAGIC (un GeoServer caído puede responder 200 con una página HTML o un XML de error).

# COMMAND ----------

def is_feature_collection(content: bytes) -> bool:
    try:
        return json.loads(content).get("type") == "FeatureCollection"
    except (ValueError, AttributeError):
        return False

load_origin, queried_at, raw_path = None, None, f"{ext_dir}/{FILE_NAME}"
try:
    resp = requests.get(SOURCE_URL, timeout=120)
    resp.raise_for_status()
    if not is_feature_collection(resp.content):
        raise ValueError(f"respuesta no es GeoJSON ({resp.headers.get('content-type')})")
    with open(raw_path, "wb") as fh:
        fh.write(resp.content)
    load_origin, queried_at = "wfs", datetime.now(timezone.utc)
    print(f"Descargado del WFS: {raw_path} ({len(resp.content) / 1024:.0f} KB)")
except Exception as e:
    print(f"AVISO: el WFS del IDESC no respondió ({e}). Se intenta la copia del volumen compartido.")
    if not os.path.exists(backup_path):
        raise FileNotFoundError(
            f"No hay copia de respaldo en {backup_path}. Descarga la capa localmente desde SOURCE_URL y súbela con "
            f"`databricks fs cp <archivo> dbfs:{backup_path} -p el-equipo`."
        )
    raw_path = backup_path
    load_origin = "volume_copy"
    queried_at = datetime.fromtimestamp(os.path.getmtime(backup_path), tz=timezone.utc)
    print(f"Usando copia: {raw_path} (modificada {queried_at:%Y-%m-%d %H:%M} UTC)")

with open(raw_path, "r", encoding="utf-8") as fh:
    gj = json.load(fh)

# COMMAND ----------

# Esquema explícito: 20 propiedades tal como las publica el WFS + geometry + trazabilidad
DOUBLES = ["area", "perimeter", "mcooiiai_1", "mcooiiai_2", "am_etc", "sm_etc", "to_etc", "tc_etc", "tl_etc"]
LONGS = ["mcooiiain_", "mcooiiain1", "sucep_licu", "corrim_lat"]
STRINGS = ["zona_mzsc", "label", "am_etl", "sm_etl", "to_etl", "tc_etl", "tl_etl"]
PROP_COLS = [
    "area", "perimeter", "mcooiiain_", "mcooiiain1", "mcooiiai_1", "mcooiiai_2", "zona_mzsc", "label",
    "am_etc", "sm_etc", "to_etc", "tc_etc", "tl_etc", "am_etl", "sm_etl", "to_etl", "tc_etl", "tl_etl",
    "sucep_licu", "corrim_lat",
]
col_type = {**{c: DoubleType() for c in DOUBLES}, **{c: LongType() for c in LONGS}, **{c: StringType() for c in STRINGS}}

bronze_schema = StructType(
    [StructField("feature_id", StringType())]
    + [StructField(c, col_type[c]) for c in PROP_COLS]
    + [
        StructField("geometry", StringType()),
        StructField("_source_url", StringType()),
        StructField("_load_origin", StringType()),
        StructField("_queried_at", TimestampType()),
    ]
)

# Columnas nuevas en el WFS harían perder datos en silencio: mejor fallar
raw_props = {k for f in gj["features"] for k in (f.get("properties") or {})}
assert raw_props == set(PROP_COLS), f"El esquema del WFS cambió: {sorted(raw_props ^ set(PROP_COLS))}"

def cast_value(col, value):
    if value is None:
        return None
    if col in DOUBLES:
        return float(value)
    if col in LONGS:
        return int(value)
    return str(value)

def to_row(feature):
    props = feature.get("properties") or {}
    return tuple(
        [feature.get("id")]
        + [cast_value(c, props.get(c)) for c in PROP_COLS]
        + [json.dumps(feature.get("geometry")), SOURCE_URL, load_origin, queried_at.replace(tzinfo=None)]
    )

df = spark.createDataFrame([to_row(f) for f in gj["features"]], schema=bronze_schema)

# COMMAND ----------

(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(bronze_table))
print(f"Guardada {bronze_table} (origen: {load_origin})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación
# MAGIC - 16 polígonos y 25 columnas (id + 20 propiedades + geometry + 3 de trazabilidad), y el conteo coincide con `totalFeatures` del WFS.
# MAGIC - Todas las geometrías son `Polygon`/`MultiPolygon` y sus coordenadas están en el rango de Cali en orden `[lon, lat]`.
# MAGIC - La capa cubre el bbox de los edificios (lon −76,563 a −76,524; lat 3,389 a 3,445).
# MAGIC - Conteo por zona, para ver las repetidas (`Zona 6`, `Cauce Activo o Terra`) que silver no debe deduplicar por nombre.

# COMMAND ----------

bronze = spark.table(bronze_table)
n = bronze.count()
assert n == 16, n
assert len(bronze.columns) == 25, len(bronze.columns)
if gj.get("totalFeatures") is not None:
    assert n == gj["totalFeatures"], (n, gj["totalFeatures"])
assert bronze.filter("feature_id IS NULL OR geometry IS NULL").count() == 0
assert bronze.select("feature_id").distinct().count() == n

def coords(c):
    if isinstance(c[0], (int, float)):
        yield c
    else:
        for k in c:
            yield from coords(k)

lons, lats = [], []
for f in gj["features"]:
    g = f["geometry"]
    assert g["type"] in ("Polygon", "MultiPolygon"), g["type"]
    for lon, lat, *_ in coords(g["coordinates"]):
        lons.append(lon)
        lats.append(lat)

print(f"bbox capa: lon {min(lons):.4f} a {max(lons):.4f} | lat {min(lats):.4f} a {max(lats):.4f}")
# Rango ampliado frente al de predios: la capa cubre todo el casco urbano de Cali
assert -76.7 <= min(lons) and max(lons) <= -76.4, "longitudes fuera de Cali (¿ejes invertidos?)"
assert 3.2 <= min(lats) and max(lats) <= 3.6, "latitudes fuera de Cali (¿ejes invertidos?)"
assert min(lons) <= -76.563 and max(lons) >= -76.524 and min(lats) <= 3.389 and max(lats) >= 3.445, \
    "la capa no cubre el bbox de los edificios"

display(bronze.groupBy("zona_mzsc").count().orderBy("zona_mzsc"))
