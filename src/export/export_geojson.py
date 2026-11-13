# Databricks notebook source
# MAGIC %pip install geopandas shapely

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Export a GeoJSON
# MAGIC Exporta `gold_{team}_map_buildings` como GeoJSON en el volumen (`export/`) para llevarlo a Kepler.gl, QGIS o cualquier visor.
# MAGIC **Decisión:** CRS EPSG:4326 con coordenadas `[lon, lat]`, que es lo que esperan GeoJSON y las herramientas web.
# MAGIC Los asserts validan conteo (5.146) y que las coordenadas caigan en Cali.

# COMMAND ----------

import os
import geopandas as gpd
import shapely.wkt

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

gold_map = f"{catalog}.{schema}.gold_{team}_map_buildings"
out_dir = f"/Volumes/{catalog}/{schema}/{volume}/export"
out_path = f"{out_dir}/gold_{team}_map_buildings.geojson"
os.makedirs(out_dir, exist_ok=True)

# COMMAND ----------

pdf = spark.table(gold_map).toPandas()
geom = pdf["geom_wkt"].map(shapely.wkt.loads)
gdf = gpd.GeoDataFrame(pdf.drop(columns=["geom_wkt"]), geometry=list(geom), crs="EPSG:4326")

assert len(gdf) == 5146, len(gdf)
minx, miny, maxx, maxy = gdf.total_bounds
assert -77 < minx and maxx < -76, (minx, maxx)  # lon
assert 3 < miny and maxy < 4, (miny, maxy)      # lat

if os.path.exists(out_path):
    os.remove(out_path)
gdf.to_file(out_path, driver="GeoJSON")
print(f"Exportado {out_path} ({os.path.getsize(out_path) / 1e6:.1f} MB, {len(gdf)} features)")

# COMMAND ----------

print(
    "Para descargar el archivo:\n"
    f"  1. Abre Catalog > {catalog} > {schema} > Volumes > {volume} > export\n"
    f"  2. Haz clic en gold_{team}_map_buildings.geojson > Download\n"
    "  3. Kepler.gl (kepler.gl/demo) o QGIS: arrastra el archivo; colorea por `color_hex` o `structural_status_code`."
)
