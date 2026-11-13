# Databricks notebook source
# MAGIC %pip install geopandas shapely

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Export a GeoJSON
# MAGIC Exporta tres capas gold como GeoJSON en el volumen (`export/`) para llevarlas a Kepler.gl, QGIS o cualquier visor:
# MAGIC
# MAGIC - `gold_{team}_map_buildings.geojson`: 5.146 edificios con estado, zona sísmica, distancias a equipamientos y la prioridad ante réplicas (`aftershock_priority_code`, `has_aggravating_factor`, `priority_color_hex`).
# MAGIC - `gold_{team}_map_seismic_zones.geojson`: polígonos de microzonificación que tocan el área de los edificios, con espectros ETC, banderas de suelo, número de edificios y % de daño.
# MAGIC - `gold_{team}_map_facilities.geojson`: equipamientos del área de estudio (salud, emergencia, policía, colegios, espacio público).
# MAGIC
# MAGIC **Decisión:** CRS EPSG:4326 con coordenadas `[lon, lat]`, que es lo que esperan GeoJSON y las herramientas web. `geom_wkt` se convierte en la geometría y el resto de columnas pasa como propiedades.
# MAGIC Distancias a metros enteros, `etc_*` a 2 decimales y porcentajes a 1 decimal para que los tooltips sean legibles y el archivo pese menos.
# MAGIC Los asserts validan el conteo de cada archivo y que las coordenadas caigan en Cali (lon −76,7 a −76,4; lat 3,2 a 3,6).

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

out_dir = f"/Volumes/{catalog}/{schema}/{volume}/export"
os.makedirs(out_dir, exist_ok=True)

LON_RANGE = (-76.7, -76.4)
LAT_RANGE = (3.2, 3.6)

# COMMAND ----------

def export_layer(table_suffix, round_int=(), round_2=(), round_1=(), int_cols=(), required=(), expected_count=None, min_count=1):
    """Lee una tabla gold, redondea columnas, convierte geom_wkt en geometría y escribe el GeoJSON (EPSG:4326)."""
    table = f"{catalog}.{schema}.gold_{team}_{table_suffix}"
    out_path = f"{out_dir}/gold_{team}_{table_suffix}.geojson"
    pdf = spark.table(table).toPandas()
    for c in round_int:  # metros enteros (Int64 admite nulos)
        pdf[c] = pdf[c].round(0).astype("Int64")
    for c in int_cols:
        pdf[c] = pdf[c].astype("Int64")
    for c in round_2:
        pdf[c] = pdf[c].round(2)
    for c in round_1:
        pdf[c] = pdf[c].round(1)
    geom = pdf["geom_wkt"].map(shapely.wkt.loads)
    gdf = gpd.GeoDataFrame(pdf.drop(columns=["geom_wkt"]), geometry=list(geom), crs="EPSG:4326")
    for c in required:
        assert c in gdf.columns, f"falta {c} en {table_suffix}"
    if expected_count is not None:
        assert len(gdf) == expected_count, (table_suffix, len(gdf))
    else:
        assert len(gdf) >= min_count, (table_suffix, len(gdf))
    assert gdf.geometry.notna().all() and not gdf.geometry.is_empty.any(), f"geometrías vacías en {table_suffix}"
    minx, miny, maxx, maxy = gdf.total_bounds
    assert LON_RANGE[0] < minx and maxx < LON_RANGE[1], (table_suffix, minx, maxx)  # lon
    assert LAT_RANGE[0] < miny and maxy < LAT_RANGE[1], (table_suffix, miny, maxy)  # lat
    if os.path.exists(out_path):
        os.remove(out_path)
    gdf.to_file(out_path, driver="GeoJSON")
    print(f"Exportado {out_path} ({os.path.getsize(out_path) / 1e6:.1f} MB, {len(gdf)} features)")
    return gdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Edificios
# MAGIC Se espera 5.146 features. Las propiedades incluyen zona sísmica, distancias (enteras) y la prioridad ante réplicas para colorear con `priority_color_hex` o `aftershock_priority_code`.

# COMMAND ----------

gdf_buildings = export_layer(
    "map_buildings",
    round_int=["nearest_health_m", "nearest_hospital_m", "nearest_emergency_m", "nearest_police_m",
               "nearest_school_m", "nearest_open_space_m"],
    int_cols=["open_space_count_500m"],
    required=["seismic_zone_name", "etc_sm_g", "requires_site_study", "is_liquefaction_susceptible",
              "nearest_hospital_m", "nearest_emergency_m", "nearest_open_space_m", "open_space_count_500m",
              "aftershock_priority_code", "has_aggravating_factor", "priority_color_hex"],
    expected_count=5146,
)
assert gdf_buildings["aftershock_priority_code"].notna().all(), "aftershock_priority_code nulo"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Zonas sísmicas
# MAGIC El conteo esperado es el de la tabla gold (los polígonos que tocan el bbox de los edificios). `etc_*` a 2 decimales y `damaged_building_pct` a 1.

# COMMAND ----------

n_zones_gold = spark.table(f"{catalog}.{schema}.gold_{team}_map_seismic_zones").count()
gdf_zones = export_layer(
    "map_seismic_zones",
    round_2=["etc_am_g", "etc_sm_g", "etc_tc_s"],
    round_1=["damaged_building_pct"],
    int_cols=["building_count"],
    required=["seismic_zone_id", "seismic_zone_name", "building_count", "damaged_building_pct"],
    expected_count=n_zones_gold,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Equipamientos
# MAGIC El conteo esperado es el de la tabla gold (equipamientos con `is_in_study_area`). Las columnas `care_level` y `enrollment_count` se exportan como enteros con nulos.

# COMMAND ----------

n_fac_gold = spark.table(f"{catalog}.{schema}.gold_{team}_map_facilities").count()
gdf_fac = export_layer(
    "map_facilities",
    int_cols=["care_level", "enrollment_count"],
    required=["facility_id", "facility_type_code", "is_hospital", "lon", "lat"],
    expected_count=n_fac_gold,
)

# COMMAND ----------

print(
    "Para descargar los archivos:\n"
    f"  1. Abre Catalog > {catalog} > {schema} > Volumes > {volume} > export\n"
    f"  2. Haz clic en gold_{team}_map_buildings.geojson, gold_{team}_map_seismic_zones.geojson y gold_{team}_map_facilities.geojson > Download\n"
    "  3. Kepler.gl (kepler.gl/demo) o QGIS: arrastra los archivos; colorea los edificios por `priority_color_hex`, `aftershock_priority_code` o `color_hex`."
)
