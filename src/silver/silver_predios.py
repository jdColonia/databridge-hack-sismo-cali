# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: predios limpios y con geometría métrica
# MAGIC Decisiones (todas reproducibles y con flags para no perder información):
# MAGIC
# MAGIC 1. **Texto:** `trim` de todos los strings; vacíos o solo espacios pasan a `null` (p. ej. `COMUNA = " "`, `EDIFPRED`, `NUMEPRED`).
# MAGIC 2. **Llaves de barrio:** `comuna_barrio_id = COMUNA-BARRIO` y `codigo_manzana = COMUNA+BARRIO+MANZANA` (null si falta algún componente).
# MAGIC 3. **Pisos:** `NPISOS = 0` no es un dato real; `npisos_valido` es `NPISOS` si > 0, si no `null`, y `npisos_imputable` marca esos casos.
# MAGIC 4. **Geometría:** se valida (`geom_valida`), se repara con `make_valid` (`geom_reparada`) y se marca `poligono_dudoso` si el área es < 5 m², > 50.000 m² o la geometría está vacía. **No se eliminan**: el filtro es decisión del análisis.
# MAGIC 5. **Área en metros:** `Shape__Area` viene en grados² (EPSG:4326) y no sirve. Se reproyecta a **EPSG:9377 (MAGNA-SIRGAS / Origen Nacional)** para `area_m2` y `perimetro_m`.
# MAGIC 6. **Área construida estimada:** `area_m2 * coalesce(npisos_valido, 1)`; con `area_construida_estimada = true` cuando no hay pisos válidos (se asume 1 piso, es un piso mínimo conservador).
# MAGIC 7. **Centroide:** se calcula en 9377 (en grados el centroide se distorsiona) y se vuelve a 4326 para `lon`, `lat`.
# MAGIC 8. **Estado estructural:** código corto (`sin_cambio`, `techo`, `escombros`, `sin_datos`), `nivel_dano` ordinal (0, 1, 2; `null` para sin datos) y `tiene_dato_satelital`.
# MAGIC    Ojo: `sin_cambio` **no** significa "sin daño": es lo que el satélite no alcanza a ver.
# MAGIC 9. **Descartadas:** `OBJECTID_1` (100% nulo) y `Shape__Area`/`Shape__Length` (en grados, reemplazadas por las métricas). `OBJECTID` se conserva como llave única.

# COMMAND ----------

import json
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import shape
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               DoubleType, BooleanType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

bronze_table = f"{catalog}.{schema}.bronze_{team}_predios"
silver_table = f"{catalog}.{schema}.silver_{team}_predios"

CRS_GEO = "EPSG:4326"
CRS_M = "EPSG:9377"

# COMMAND ----------

# Bronze -> pandas (5 mil filas, cabe sin problema en el driver)
pdf = spark.table(bronze_table).toPandas()

def clean_str(v):
    """trim; vacío o solo espacios -> None"""
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return None if pd.isna(v) else v

str_cols = [c for c in pdf.columns if pdf[c].dtype == object]
for c in str_cols:
    pdf[c] = pdf[c].map(clean_str)

# COMMAND ----------

# Geometría: GeoJSON texto -> shapely -> GeoDataFrame en 4326
geoms = pdf["geometry"].map(lambda g: shape(json.loads(g)))
gdf = gpd.GeoDataFrame(pdf.drop(columns=["geometry"]), geometry=list(geoms), crs=CRS_GEO)

def only_polygons(g):
    """make_valid puede devolver GeometryCollection; nos quedamos con lo poligonal"""
    if g.geom_type in ("Polygon", "MultiPolygon"):
        return g
    parts = [p for p in shapely.get_parts(g) if p.geom_type in ("Polygon", "MultiPolygon")]
    return shapely.union_all(parts) if parts else shapely.Polygon()

gdf["geom_valida"] = gdf.geometry.is_valid
gdf["geom_reparada"] = ~gdf["geom_valida"]
fixed = gdf.geometry.map(lambda g: g if g.is_valid else only_polygons(shapely.make_valid(g)))
gdf = gdf.set_geometry(fixed)

# COMMAND ----------

# Medidas en metros (EPSG:9377)
gm = gdf.to_crs(CRS_M)
gdf["area_m2"] = gm.geometry.area
gdf["perimetro_m"] = gm.geometry.length
gdf["poligono_dudoso"] = (gm.geometry.is_empty | (gdf["area_m2"] < 5) | (gdf["area_m2"] > 50000))

# Centroide calculado en métrico y devuelto a 4326
cent = gpd.GeoSeries(gm.geometry.centroid, crs=CRS_M).to_crs(CRS_GEO)
gdf["lon"] = cent.x
gdf["lat"] = cent.y

# COMMAND ----------

# Llaves y pisos
comuna, barrio, manzana = gdf["COMUNA"], gdf["BARRIO"], gdf["MANZANA"]
gdf["comuna_barrio_id"] = comuna + "-" + barrio  # null si alguno es null
gdf["codigo_manzana"] = comuna + barrio + manzana

npisos = gdf["NPISOS"].astype("Int64")
gdf["npisos_valido"] = npisos.where(npisos > 0)
gdf["npisos_imputable"] = (npisos == 0).fillna(False).astype(bool)
gdf["area_construida_m2"] = gdf["area_m2"] * gdf["npisos_valido"].fillna(1).astype(float)
gdf["area_construida_estimada"] = gdf["npisos_valido"].isna()

# Estado estructural (match por prefijo, robusto a tildes/encoding)
def estado_codigo(s):
    if s is None:
        return "sin_datos"
    t = s.lower()
    if t.startswith("sin cambio"):
        return "sin_cambio"
    if t.startswith("posible"):
        return "techo"
    if t.startswith("alta probabilidad"):
        return "escombros"
    return "sin_datos"

NIVEL = {"sin_cambio": 0, "techo": 1, "escombros": 2}
gdf["estado_codigo"] = gdf["Estado_Estructural"].map(estado_codigo)
gdf["nivel_dano"] = gdf["estado_codigo"].map(NIVEL).astype("Int64")
gdf["tiene_dato_satelital"] = gdf["estado_codigo"] != "sin_datos"

gdf["geometry_wkt"] = gdf.geometry.to_wkt()  # 4326

assert gdf["OBJECTID"].is_unique, "OBJECTID debe ser llave única"

# COMMAND ----------

# Selección final y tipos explícitos
out_cols = [
    ("OBJECTID", LongType()), ("DEPAPRED", StringType()), ("MUNIPRED", StringType()),
    ("TIPO_AVALU", StringType()), ("SECTOR", StringType()), ("COMUNA", StringType()),
    ("BARRIO", StringType()), ("MANZANA", StringType()), ("TERRENO", StringType()),
    ("COND", StringType()), ("EDIFPRED", StringType()), ("PREDIO", StringType()),
    ("NUMEPRED", StringType()), ("NPISOS", LongType()), ("ID_PREDIO", StringType()),
    ("Indice_Escombros", DoubleType()), ("Estado_Estructural", StringType()),
    ("comuna_barrio_id", StringType()), ("codigo_manzana", StringType()),
    ("npisos_valido", LongType()), ("npisos_imputable", BooleanType()),
    ("geom_valida", BooleanType()), ("geom_reparada", BooleanType()),
    ("poligono_dudoso", BooleanType()),
    ("area_m2", DoubleType()), ("perimetro_m", DoubleType()),
    ("area_construida_m2", DoubleType()), ("area_construida_estimada", BooleanType()),
    ("lon", DoubleType()), ("lat", DoubleType()),
    ("estado_codigo", StringType()), ("nivel_dano", LongType()),
    ("tiene_dato_satelital", BooleanType()), ("geometry_wkt", StringType()),
]
sch = StructType([StructField(n, t) for n, t in out_cols])
names = [n for n, _ in out_cols]

flat = pd.DataFrame(gdf[names]).astype(object)
flat = flat.where(flat.notna(), None)  # NaN / pd.NA -> None (null en Spark)
rows = [tuple(r) for r in flat.itertuples(index=False, name=None)]
silver = spark.createDataFrame(rows, schema=sch)

(silver.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(silver_table))
print(f"Guardada {silver_table}: {silver.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calidad
# MAGIC Nulos por columna, polígonos dudosos, reparados y qué parte del área construida no tiene dato satelital.

# COMMAND ----------

from pyspark.sql import functions as F

s = spark.table(silver_table)
nulls = s.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in s.columns])
display(nulls)

# COMMAND ----------

resumen = s.agg(
    F.count("*").alias("n"),
    F.sum(F.col("poligono_dudoso").cast("int")).alias("poligonos_dudosos"),
    F.sum(F.col("geom_reparada").cast("int")).alias("geometrias_reparadas"),
    F.sum(F.when(~F.col("tiene_dato_satelital"), F.col("area_construida_m2")).otherwise(0)).alias("area_sin_datos"),
    F.sum("area_construida_m2").alias("area_total"),
).collect()[0]
print(f"Polígonos dudosos: {resumen['poligonos_dudosos']}")
print(f"Geometrías reparadas: {resumen['geometrias_reparadas']}")
print(f"% área construida sin datos satelitales: {100 * resumen['area_sin_datos'] / resumen['area_total']:.1f}%")
display(s.select("area_m2", "perimetro_m", "area_construida_m2").summary())
