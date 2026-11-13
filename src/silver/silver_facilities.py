# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: equipamientos de Cali y distancias desde cada edificio
# MAGIC Dos tablas a partir de las 4 capas oficiales del IDESC (bronze `idesc_*`):
# MAGIC
# MAGIC 1. `silver_{team}_facilities`: **un equipamiento por fila** (3.727 en toda Cali), con tipo unificado, nombre, dirección, atributos útiles y geometría.
# MAGIC 2. `silver_{team}_building_facility_distances`: **una fila por edificio** (5.146) con la distancia en metros al equipamiento más cercano de cada tipo.
# MAGIC
# MAGIC **Decisiones:**
# MAGIC - **Tipo unificado** `facility_type_code`: `health` (salud), `emergency` (bomberos y Defensa Civil), `police` (el resto de seguridad ciudadana, incluidos los de `tipo` nulo),
# MAGIC   `school` (sedes educativas) y `open_space` (espacio público de encuentro: parques, zonas verdes, plazas; sirven como punto de reunión ante réplicas).
# MAGIC   El hospital se marca aparte con `is_hospital` (salud con categoría "Hospitales - Hospitalización"): una IPS ambulatoria no sustituye a un hospital.
# MAGIC - **Llave** `facility_id` = `<capa>.<id del WFS>` (las capas no comparten identificadores).
# MAGIC - **Texto:** `trim`; vacíos o solo espacios pasan a `null`. Los nombres propios se dejan como vienen.
# MAGIC - **Geometría:** las capas de salud, seguridad y espacio público son polígonos (huella del predio) y las sedes educativas son puntos. Se repara con `make_valid` si es inválida.
# MAGIC   `lon`/`lat` es un `representative_point` (siempre cae dentro del polígono). `area_m2` se calcula en **EPSG:9377** (nulo para puntos).
# MAGIC - **Zona de estudio:** `is_in_study_area` es verdadero si el equipamiento intersecta el bbox de los edificios ampliado 2.000 m (en EPSG:9377). La tabla conserva **toda Cali**.
# MAGIC - **Distancias:** se miden en EPSG:9377 desde el centroide del edificio hasta la **geometría** del equipamiento (el borde del polígono; 0 si el centroide cae dentro),
# MAGIC   usando **todos** los equipamientos de Cali y no solo los de la zona de estudio, para no sesgar a los edificios de borde.
# MAGIC   Si hay empates, se conserva la distancia mínima y una sola fila por edificio (el nombre sale del equipamiento con menor `facility_id`).
# MAGIC - `open_space_count_500m`: cuántos espacios públicos tienen alguna parte a 500 m o menos del centroide del edificio.
# MAGIC
# MAGIC **Fechas del dato:** las capas `pot_2014` (salud y seguridad) no traen fecha, probablemente son de 2014; las sedes educativas se actualizaron entre 2023 y 2024;
# MAGIC el espacio público viene del PMEP 2021–2023. Puede haber equipamientos que ya no existan o estén desactualizados.

# COMMAND ----------

import json
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import shape, box
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               DoubleType, BooleanType, IntegerType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

CRS_GEO = "EPSG:4326"
CRS_M = "EPSG:9377"
STUDY_BUFFER_M = 2000
OPEN_SPACE_RADIUS_M = 500

buildings_table = f"{catalog}.{schema}.silver_{team}_buildings"
facilities_table = f"{catalog}.{schema}.silver_{team}_facilities"
dist_table = f"{catalog}.{schema}.silver_{team}_building_facility_distances"

bronze_tables = {
    "health": f"{catalog}.{schema}.bronze_{team}_idesc_health_facilities",
    "security": f"{catalog}.{schema}.bronze_{team}_idesc_security_facilities",
    "schools": f"{catalog}.{schema}.bronze_{team}_idesc_schools",
    "public_spaces": f"{catalog}.{schema}.bronze_{team}_idesc_public_spaces",
}

needed = [buildings_table] + list(bronze_tables.values())
missing = [t for t in needed if not spark.catalog.tableExists(t)]
assert not missing, f"Faltan tablas {missing}: corre antes bronze_idesc_facilities y silver_buildings."

# COMMAND ----------

def apply_comments(table, table_comment, column_comments):
    """Aplica COMMENT de tabla y de cada columna; exige que el diccionario cubra TODAS las columnas."""
    esc = lambda s: s.replace("'", "''")
    cols = spark.table(table).columns
    assert set(cols) == set(column_comments), (
        f"{table}: sin comentario {set(cols) - set(column_comments)}, "
        f"sobran {set(column_comments) - set(cols)}")
    spark.sql(f"COMMENT ON TABLE {table} IS '{esc(table_comment)}'")
    for c in cols:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} COMMENT '{esc(column_comments[c])}'")

SRC = "capas oficiales del IDESC (Alcaldía de Santiago de Cali)"
DOCS = {
    "facilities": {
        "table": (f"Equipamientos de toda Cali (salud, bomberos y Defensa Civil, policía, sedes educativas y espacio público de encuentro), un equipamiento por fila. "
                  f"Fuente: {SRC}; las capas pot_2014 (salud y seguridad) no traen fecha, probablemente 2014; las sedes educativas se actualizaron en 2023–2024; "
                  f"el espacio público viene del PMEP 2021–2023. Pueden incluir equipamientos que ya no existen."),
        "columns": {
            "facility_id": "Identificador del equipamiento, formato '<capa>.<id del WFS>' (ej. 'health.eqp_uco_salud.484'). Origen: bronze idesc, feature_id.",
            "source_layer": "Capa de origen: health, security, schools o public_spaces. Origen: tabla bronze idesc de la que sale.",
            "facility_type_code": "Tipo unificado: health, emergency (bomberos y Defensa Civil), police (resto de seguridad ciudadana, incluye tipo nulo), school u open_space. Origen: derivado de la capa y de tipo.",
            "facility_subtype": "Subtipo original: categoria (salud), tipo (seguridad), eeestado (sedes educativas) o ecrpotele (espacio público); nulo si venía vacío. Origen: bronze idesc.",
            "facility_name": "Nombre del equipamiento (nombre, eenomsede o ecrnomcom); nulo si venía vacío. Origen: bronze idesc.",
            "address": "Dirección o localización (localizaci, eedirecc o ecrdirecci); nula si venía vacía. Origen: bronze idesc.",
            "is_hospital": "Verdadero si es de salud con categoría 'Hospitales - Hospitalización'; falso para el resto. Origen: derivado de categoria.",
            "care_level": "Nivel de atención de salud (1, 2 o 3); nulo si no es de salud o no tiene nivel. Origen: bronze idesc, nivel.",
            "is_public": "Verdadero si es público (propiedad PUB o estado Oficial); falso si es privado (PRIV) o no oficial; nulo si no aplica o no se sabe. Origen: propiedad (salud) o eeestado (colegios).",
            "enrollment_count": "Matrícula total de la sede educativa, en estudiantes; nulo si no es colegio. Origen: bronze idesc, ieomatrtot.",
            "area_m2": "Área de la huella en m² calculada en EPSG:9377; nula para puntos (sedes educativas). Origen: geometría reproyectada.",
            "is_in_study_area": "Verdadero si el equipamiento intersecta el bbox de los edificios ampliado 2.000 m (EPSG:9377). Origen: cruce espacial con silver_el_equipo_buildings.",
            "lon": "Longitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. Origen: representative_point calculado en EPSG:4326.",
            "lat": "Latitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. Origen: representative_point calculado en EPSG:4326.",
            "geom_wkt": "Geometría del equipamiento en WKT, EPSG:4326 (lon, lat), ya reparada. Origen: bronze idesc, geometry.",
        },
    },
    "building_facility_distances": {
        "table": (f"Distancia en metros de cada edificio al equipamiento más cercano de cada tipo, una fila por edificio (5.146). Medida en EPSG:9377 desde el centroide del edificio hasta la geometría "
                  f"del equipamiento (0 si está dentro), con todos los equipamientos de Cali. Fuente: {SRC}; salud y seguridad sin fecha (probablemente 2014), colegios 2023–2024, espacio público PMEP 2021–2023."),
        "columns": {
            "building_id": "Identificador del edificio. Origen: silver_el_equipo_buildings.",
            "nearest_health_m": "Distancia en m al equipamiento de salud más cercano (hospital o no). Origen: sjoin_nearest con silver_el_equipo_facilities (tipo health), EPSG:9377.",
            "nearest_hospital_m": "Distancia en m al hospital más cercano (salud con is_hospital). Origen: sjoin_nearest con silver_el_equipo_facilities, EPSG:9377.",
            "nearest_emergency_m": "Distancia en m a la estación de bomberos o sede de Defensa Civil más cercana. Origen: sjoin_nearest con silver_el_equipo_facilities (tipo emergency), EPSG:9377.",
            "nearest_police_m": "Distancia en m al equipamiento de policía más cercano (CAI, estaciones, etc.). Origen: sjoin_nearest con silver_el_equipo_facilities (tipo police), EPSG:9377.",
            "nearest_school_m": "Distancia en m a la sede educativa más cercana. Origen: sjoin_nearest con silver_el_equipo_facilities (tipo school), EPSG:9377.",
            "nearest_open_space_m": "Distancia en m al espacio público de encuentro más cercano (borde del polígono; 0 si el edificio está dentro). Origen: sjoin_nearest con silver_el_equipo_facilities (tipo open_space), EPSG:9377.",
            "nearest_hospital_name": "Nombre del hospital más cercano; nulo si el hospital no tiene nombre. Origen: silver_el_equipo_facilities, facility_name.",
            "nearest_emergency_name": "Nombre de la estación de bomberos o sede de Defensa Civil más cercana; nulo si no tiene nombre. Origen: silver_el_equipo_facilities, facility_name.",
            "nearest_open_space_name": "Nombre del espacio público más cercano; nulo si no tiene nombre. Origen: silver_el_equipo_facilities, facility_name.",
            "open_space_count_500m": "Número de espacios públicos con alguna parte a 500 m o menos del centroide del edificio. Origen: cruce espacial con silver_el_equipo_facilities (tipo open_space), EPSG:9377.",
        },
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Equipamientos
# MAGIC Cada capa bronze se convierte a un `GeoDataFrame` con las columnas comunes (tipo, subtipo, nombre, dirección, atributos propios) y luego se apilan.

# COMMAND ----------

def clean_str(v):
    """trim; vacío o solo espacios -> None"""
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return None

def load_layer(key):
    pdf = spark.table(bronze_tables[key]).toPandas()
    for c in pdf.columns:
        if c not in ("geometry", "_queried_at"):
            pdf[c] = pdf[c].map(clean_str)
    geoms = [shape(json.loads(g)) for g in pdf["geometry"]]
    return pdf, geoms

def to_int(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")

frames = []

# salud
pdf, geoms = load_layer("health")
frames.append(gpd.GeoDataFrame({
    "feature_id": pdf["feature_id"], "source_layer": "health", "facility_type_code": "health",
    "facility_subtype": pdf["categoria"], "facility_name": pdf["nombre"], "address": pdf["localizaci"],
    "is_hospital": pdf["categoria"].map(lambda s: bool(s) and s.lower().startswith("hospitales")),
    "care_level": to_int(pdf["nivel"]),
    "is_public": pdf["propiedad"].map(lambda s: {"PUB": True, "PRIV": False}.get((s or "").upper())),
    "enrollment_count": pd.array([pd.NA] * len(pdf), dtype="Int64"),
}, geometry=geoms, crs=CRS_GEO))

# seguridad: emergency = bomberos y Defensa Civil; police = el resto (incluye tipo nulo)
pdf, geoms = load_layer("security")
frames.append(gpd.GeoDataFrame({
    "feature_id": pdf["feature_id"], "source_layer": "security",
    "facility_type_code": pdf["tipo"].map(lambda s: "emergency" if (s or "").upper() in ("BOMBEROS", "DEFENSACIVIL") else "police"),
    "facility_subtype": pdf["tipo"], "facility_name": pdf["nombre"], "address": pdf["localizaci"],
    "is_hospital": False, "care_level": pd.array([pd.NA] * len(pdf), dtype="Int64"),
    "is_public": pdf["propiedad"].map(lambda s: {"PUB": True, "PRIV": False}.get((s or "").upper())),
    "enrollment_count": pd.array([pd.NA] * len(pdf), dtype="Int64"),
}, geometry=geoms, crs=CRS_GEO))

# sedes educativas
pdf, geoms = load_layer("schools")
frames.append(gpd.GeoDataFrame({
    "feature_id": pdf["feature_id"], "source_layer": "schools", "facility_type_code": "school",
    "facility_subtype": pdf["eeestado"], "facility_name": pdf["eenomsede"], "address": pdf["eedirecc"],
    "is_hospital": False, "care_level": pd.array([pd.NA] * len(pdf), dtype="Int64"),
    "is_public": pdf["eeestado"].map(lambda s: None if s is None else s.lower() == "oficial"),
    "enrollment_count": to_int(pdf["ieomatrtot"]),
}, geometry=geoms, crs=CRS_GEO))

# espacio público de encuentro
pdf, geoms = load_layer("public_spaces")
frames.append(gpd.GeoDataFrame({
    "feature_id": pdf["feature_id"], "source_layer": "public_spaces", "facility_type_code": "open_space",
    "facility_subtype": pdf["ecrpotele"], "facility_name": pdf["ecrnomcom"], "address": pdf["ecrdirecci"],
    "is_hospital": False, "care_level": pd.array([pd.NA] * len(pdf), dtype="Int64"),
    "is_public": None, "enrollment_count": pd.array([pd.NA] * len(pdf), dtype="Int64"),
}, geometry=geoms, crs=CRS_GEO))

fac = pd.concat(frames, ignore_index=True)
fac = gpd.GeoDataFrame(fac, geometry="geometry", crs=CRS_GEO)
fac["facility_id"] = fac["source_layer"] + "." + fac["feature_id"]
fac["is_hospital"] = fac["is_hospital"].astype(bool)

# COMMAND ----------

# Geometría: reparar si es inválida, medidas en EPSG:9377 y punto representativo en 4326
def fix_geom(g):
    return g if g.is_valid else shapely.make_valid(g)

fac = fac.set_geometry(fac.geometry.map(fix_geom))
assert not fac.geometry.is_empty.any(), "hay geometrías vacías"

fac_m = fac.to_crs(CRS_M)
is_poly = fac.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
fac["area_m2"] = fac_m.geometry.area.where(is_poly)  # nulo para puntos

rep = gpd.GeoSeries(fac.geometry.representative_point(), crs=CRS_GEO)
fac["lon"] = rep.x
fac["lat"] = rep.y
fac["geom_wkt"] = fac.geometry.to_wkt()

# Zona de estudio: bbox de los edificios (EPSG:9377) + 2.000 m
bld = spark.table(buildings_table).select("building_id", "lon", "lat", "geom_wkt").toPandas()
bld_geom = gpd.GeoSeries.from_wkt(bld["geom_wkt"], crs=CRS_GEO).to_crs(CRS_M)
minx, miny, maxx, maxy = bld_geom.total_bounds
study_box = box(minx - STUDY_BUFFER_M, miny - STUDY_BUFFER_M, maxx + STUDY_BUFFER_M, maxy + STUDY_BUFFER_M)
fac["is_in_study_area"] = fac_m.geometry.intersects(study_box).astype(bool)
print(f"bbox edificios + {STUDY_BUFFER_M} m (EPSG:9377): {study_box.bounds}")

# COMMAND ----------

fac_cols = [
    ("facility_id", StringType()), ("source_layer", StringType()), ("facility_type_code", StringType()),
    ("facility_subtype", StringType()), ("facility_name", StringType()), ("address", StringType()),
    ("is_hospital", BooleanType()), ("care_level", IntegerType()), ("is_public", BooleanType()),
    ("enrollment_count", IntegerType()), ("area_m2", DoubleType()), ("is_in_study_area", BooleanType()),
    ("lon", DoubleType()), ("lat", DoubleType()), ("geom_wkt", StringType()),
]

def to_spark(df, cols):
    names = [n for n, _ in cols]
    flat = pd.DataFrame(df[names]).astype(object)
    flat = flat.where(flat.notna(), None)  # NaN / pd.NA -> None (null en Spark)
    rows = [tuple(r) for r in flat.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows, schema=StructType([StructField(n, t) for n, t in cols]))

fac_sdf = to_spark(fac, fac_cols)
(fac_sdf.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(facilities_table))
apply_comments(facilities_table, DOCS["facilities"]["table"], DOCS["facilities"]["columns"])
print(f"Guardada {facilities_table}: {len(fac)} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distancias edificio → equipamiento más cercano
# MAGIC Para cada tipo, `sjoin_nearest` del centroide del edificio (EPSG:9377) contra la geometría del equipamiento; la distancia es al borde del polígono (0 si está dentro).
# MAGIC Con empates `sjoin_nearest` duplica filas: se ordena por distancia y `facility_id` y se deja una por edificio. Las columnas `nearest_*` se calculan con **todos** los equipamientos de Cali.

# COMMAND ----------

pts = gpd.GeoDataFrame(
    bld[["building_id"]],
    geometry=gpd.points_from_xy(bld["lon"], bld["lat"]),
    crs=CRS_GEO,
).to_crs(CRS_M)

fac_m = fac_m.assign(facility_id=fac["facility_id"], facility_name=fac["facility_name"])

def nearest(sub, dist_col, name_col=None):
    """Distancia (y nombre) del equipamiento más cercano; una fila por edificio."""
    assert len(sub) > 0, f"no hay equipamientos para {dist_col}"
    j = gpd.sjoin_nearest(pts, sub[["facility_id", "facility_name", "geometry"]], how="left", distance_col=dist_col)
    j = j.sort_values(["building_id", dist_col, "facility_id"]).drop_duplicates("building_id")
    keep = ["building_id", dist_col] + ([name_col] if name_col else [])
    if name_col:
        j = j.rename(columns={"facility_name": name_col})
    return j[keep]

is_type = lambda t: fac_m["facility_type_code"] == t
subsets = [
    (fac_m[is_type("health")], "nearest_health_m", None),
    (fac_m[fac["is_hospital"]], "nearest_hospital_m", "nearest_hospital_name"),
    (fac_m[is_type("emergency")], "nearest_emergency_m", "nearest_emergency_name"),
    (fac_m[is_type("police")], "nearest_police_m", None),
    (fac_m[is_type("school")], "nearest_school_m", None),
    (fac_m[is_type("open_space")], "nearest_open_space_m", "nearest_open_space_name"),
]
result = pts[["building_id"]].copy()
for sub, dist_col, name_col in subsets:
    result = result.merge(nearest(sub, dist_col, name_col), on="building_id", how="left")

# Espacios públicos a 500 m o menos: círculo de 500 m alrededor del centroide que intersecta el polígono
buf = gpd.GeoDataFrame(pts[["building_id"]], geometry=pts.geometry.buffer(OPEN_SPACE_RADIUS_M), crs=CRS_M)
osp = fac_m[is_type("open_space")][["facility_id", "geometry"]]
cnt = gpd.sjoin(buf, osp, how="inner", predicate="intersects").groupby("building_id").size()
result["open_space_count_500m"] = result["building_id"].map(cnt).fillna(0).astype(int)

dist_cols = [
    ("building_id", LongType()), ("nearest_health_m", DoubleType()), ("nearest_hospital_m", DoubleType()),
    ("nearest_emergency_m", DoubleType()), ("nearest_police_m", DoubleType()), ("nearest_school_m", DoubleType()),
    ("nearest_open_space_m", DoubleType()), ("nearest_hospital_name", StringType()),
    ("nearest_emergency_name", StringType()), ("nearest_open_space_name", StringType()),
    ("open_space_count_500m", IntegerType()),
]
dist_sdf = to_spark(result, dist_cols)
(dist_sdf.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(dist_table))
apply_comments(dist_table, DOCS["building_facility_distances"]["table"], DOCS["building_facility_distances"]["columns"])
print(f"Guardada {dist_table}: {len(result)} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría
# MAGIC - Equipamientos: total igual a la suma de las 4 tablas bronze (482 + 89 + 356 + 2.800 = 3.727) y `facility_id` único.
# MAGIC - Distancias: 5.146 filas, `building_id` único, todas las distancias `nearest_*_m` no nulas y mayores o iguales a 0.
# MAGIC - Resumen: equipamientos por tipo (toda Cali y zona de estudio), mediana y percentil 90 de cada distancia, y distribución de `open_space_count_500m`.

# COMMAND ----------

f = spark.table(facilities_table)
d = spark.table(dist_table)

expected = sum(spark.table(t).count() for t in bronze_tables.values())
n_f = f.count()
assert n_f == expected, (n_f, expected)
assert f.select("facility_id").distinct().count() == n_f, "facility_id debe ser único"
assert f.filter("lon IS NULL OR lat IS NULL OR geom_wkt IS NULL").count() == 0

n_d = d.count()
assert n_d == 5146, n_d
assert d.select("building_id").distinct().count() == n_d, "building_id debe ser único"
dist_m_cols = [c for c in d.columns if c.startswith("nearest_") and c.endswith("_m")]
for c in dist_m_cols:
    assert d.filter(F.col(c).isNull() | (F.col(c) < 0)).count() == 0, f"{c}: hay nulos o negativos"
assert d.filter("open_space_count_500m IS NULL OR open_space_count_500m < 0").count() == 0

# COMMAND ----------

print("--- Equipamientos por tipo (toda Cali y zona de estudio)")
display(f.groupBy("facility_type_code").agg(
    F.count("*").alias("total"),
    F.sum(F.col("is_in_study_area").cast("int")).alias("in_study_area"),
).orderBy("facility_type_code"))

print("--- Distancias (m): mediana y percentil 90")
stats = d.select([F.percentile_approx(c, [0.5, 0.9]).alias(c) for c in dist_m_cols]).collect()[0]
for c in dist_m_cols:
    med, p90 = stats[c]
    print(f"{c:24s} mediana {med:8.0f} m | p90 {p90:8.0f} m")

print("--- Distribución de open_space_count_500m")
display(d.groupBy("open_space_count_500m").count().orderBy("open_space_count_500m"))
