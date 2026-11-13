# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Datos adicionales (plantilla para fuentes externas)
# MAGIC Estamos investigando qué fuentes externas enriquecen el análisis. Este notebook deja el **patrón** y un ejemplo funcional.
# MAGIC
# MAGIC **Patrón para cualquier fuente nueva:**
# MAGIC 1. Descargar la fuente (API o archivo).
# MAGIC 2. Guardar el **crudo** en el volumen: `/Volumes/<catalog>/<schema>/<volume>/externos/`.
# MAGIC 3. Tabla `bronze_{team}_<fuente>` (tal cual llega).
# MAGIC 4. Limpieza -> `silver_{team}_<fuente>`.
# MAGIC 5. Cruce espacial con `silver_{team}_predios` con geopandas (`sjoin` / `sjoin_nearest`) **en EPSG:9377** (metros).
# MAGIC
# MAGIC **Ejemplo incluido:** equipamientos de OpenStreetMap (Overpass API): hospitales/clínicas, colegios y estaciones de bomberos dentro del bbox de los predios (+0,01° de margen).
# MAGIC Resultado: `silver_{team}_equipamientos_osm` y `silver_{team}_predios_distancia_equipamientos` (distancia al más cercano de cada tipo).
# MAGIC
# MAGIC Si Overpass falla, el notebook avisa y termina sin romper el job (la gold usa estas distancias solo si la tabla existe).

# COMMAND ----------

import os
import json
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

vol_root = f"/Volumes/{catalog}/{schema}/{volume}"
ext_dir = f"{vol_root}/externos"
os.makedirs(ext_dir, exist_ok=True)

predios_table = f"{catalog}.{schema}.silver_{team}_predios"
equip_table = f"{catalog}.{schema}.silver_{team}_equipamientos_osm"
dist_table = f"{catalog}.{schema}.silver_{team}_predios_distancia_equipamientos"

# COMMAND ----------

def load_geojson_url_to_volume(url, filename, params=None, timeout=120):
    """Descarga una URL (GeoJSON/JSON) y guarda el crudo en el volumen /externos/. Devuelve la ruta."""
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    path = f"{ext_dir}/{filename}"
    with open(path, "wb") as fh:
        fh.write(resp.content)
    print(f"Guardado {path} ({len(resp.content) / 1024:.0f} KB)")
    return path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejemplo: OpenStreetMap (Overpass)
# MAGIC El bbox sale de `silver_{team}_predios` (min/max de `lon`/`lat` + 0,01°). Se descarga el JSON crudo al volumen antes de procesarlo.

# COMMAND ----------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
elements = []

try:
    b = spark.table(predios_table).agg(
        F.min("lon").alias("min_lon"), F.max("lon").alias("max_lon"),
        F.min("lat").alias("min_lat"), F.max("lat").alias("max_lat"),
    ).collect()[0]
    m = 0.01
    south, west = b["min_lat"] - m, b["min_lon"] - m
    north, east = b["max_lat"] + m, b["max_lon"] + m
    bbox = f"{south},{west},{north},{east}"  # orden Overpass: sur, oeste, norte, este
    print("bbox:", bbox)

    query = f"""
    [out:json][timeout:60];
    (
      nwr[amenity~"hospital|clinic"]({bbox});
      nwr[amenity~"school"]({bbox});
      nwr[amenity="fire_station"]({bbox});
    );
    out center;
    """
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=120)
    resp.raise_for_status()
    raw_path = f"{ext_dir}/osm_equipamientos.json"
    with open(raw_path, "wb") as fh:
        fh.write(resp.content)
    elements = resp.json().get("elements", [])
    print(f"Elementos OSM: {len(elements)}")
    osm_ok = len(elements) > 0
except Exception as e:
    osm_ok = False
    print(f"AVISO: no se pudo consultar Overpass ({e}). Se omite el ejemplo OSM.")

# COMMAND ----------

if not osm_ok:
    dbutils.notebook.exit("Overpass no disponible o sin resultados: se omite silver_equipamientos_osm (el job continúa).")

# COMMAND ----------

# Limpieza: un punto por elemento (nodos: lat/lon; ways/relations: center)
def tipo_amenity(tags):
    a = tags.get("amenity")
    if a in ("hospital", "clinic"):
        return "hospital"
    if a == "school":
        return "colegio"
    if a == "fire_station":
        return "bomberos"
    return None

recs = []
for el in elements:
    lat = el.get("lat", (el.get("center") or {}).get("lat"))
    lon = el.get("lon", (el.get("center") or {}).get("lon"))
    tags = el.get("tags", {})
    tipo = tipo_amenity(tags)
    if lat is None or lon is None or tipo is None:
        continue
    recs.append({
        "osm_id": f"{el['type']}/{el['id']}",
        "tipo": tipo,
        "nombre": tags.get("name"),
        "lon": float(lon),
        "lat": float(lat),
        "geometry_wkt": Point(lon, lat).wkt,
    })

equip = pd.DataFrame(recs).drop_duplicates("osm_id")
print(equip["tipo"].value_counts().to_string())
equip_schema = "osm_id string, tipo string, nombre string, lon double, lat double, geometry_wkt string"
equip_rows = equip.astype(object).where(equip.notna(), None)[["osm_id", "tipo", "nombre", "lon", "lat", "geometry_wkt"]].values.tolist()
equip_sdf = spark.createDataFrame(equip_rows, schema=equip_schema)
(equip_sdf.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(equip_table))
print(f"Guardada {equip_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cruce espacial
# MAGIC Para cada predio (centroide) se busca el equipamiento **más cercano de cada tipo** con `sjoin_nearest` en **EPSG:9377**, para que la distancia salga en metros.
# MAGIC Si un tipo no tiene ningún elemento en el bbox, su distancia queda `null`.

# COMMAND ----------

pred = spark.table(predios_table).select("OBJECTID", "lon", "lat").toPandas()
pred_gdf = gpd.GeoDataFrame(
    pred[["OBJECTID"]],
    geometry=gpd.points_from_xy(pred["lon"], pred["lat"]),
    crs="EPSG:4326",
).to_crs("EPSG:9377")

result = pred[["OBJECTID"]].copy()
for tipo in ["hospital", "colegio", "bomberos"]:
    sub = equip[equip["tipo"] == tipo]
    col = f"distancia_m_{tipo}"
    if sub.empty:
        result[col] = float("nan")
        continue
    eq_gdf = gpd.GeoDataFrame(
        sub[["osm_id"]],
        geometry=gpd.points_from_xy(sub["lon"], sub["lat"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:9377")
    j = gpd.sjoin_nearest(pred_gdf, eq_gdf, how="left", distance_col=col)
    # si hay empates sjoin_nearest duplica filas: nos quedamos con la mínima
    j = j.groupby("OBJECTID", as_index=False)[col].min()
    result = result.merge(j, on="OBJECTID", how="left")

dist_cols = [c for c in result.columns if c != "OBJECTID"]
dist_schema = "OBJECTID long, " + ", ".join(f"{c} double" for c in dist_cols)
rows = result[["OBJECTID"] + dist_cols].astype(object)
rows = rows.where(rows.notna(), None).values.tolist()
dist_sdf = spark.createDataFrame(rows, schema=dist_schema)
(dist_sdf.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(dist_table))
print(f"Guardada {dist_table}")
display(dist_sdf.summary())
