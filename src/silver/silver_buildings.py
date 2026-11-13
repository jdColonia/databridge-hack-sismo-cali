# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: edificios limpios, con geometría métrica, barrio oficial y catastro
# MAGIC Una fila por edificio (`building_id`). Decisiones (todas reproducibles y con banderas para no perder información):
# MAGIC
# MAGIC 1. **Texto:** `trim` de todos los strings; vacíos o solo espacios pasan a `null` (p. ej. `COMUNA = " "`, `EDIFPRED`, `NUMEPRED`).
# MAGIC 2. **Nombres:** las columnas originales (`OBJECTID`, `COMUNA`, `NPISOS`...) se renombran a inglés según `docs/conventions.md`; el mapeo está en el diccionario `RENAME`.
# MAGIC 3. **Códigos de barrio:** `cadastral_neighborhood_code` = comuna + barrio del dataset en formato `CCBB` (sin guion), igual que el código oficial `neighborhood_code`.
# MAGIC    `cadastral_block_code` = `CCBBMMMM` (null si falta algún componente).
# MAGIC 4. **Pisos:** `NPISOS = 0` no es un dato real; `floor_count_raw` conserva `NPISOS`, `floor_count` es `NPISOS` si > 0 y si no `null`, e `is_floor_count_missing` marca esos casos.
# MAGIC 5. **Geometría:** se valida (`is_geometry_valid`), se repara con `make_valid` (`is_geometry_repaired`) y se marca `is_suspect_polygon` si el área es < 5 m², > 50.000 m² o la geometría está vacía. **No se eliminan**: el filtro es decisión del análisis.
# MAGIC 6. **Área en metros:** `Shape__Area` viene en grados² (EPSG:4326) y no sirve. Se reproyecta a **EPSG:9377 (MAGNA-SIRGAS / Origen Nacional)** para `area_m2` y `perimeter_m`.
# MAGIC 7. **Área construida estimada:** `area_m2 * coalesce(floor_count, 1)`; con `is_built_area_estimated = true` cuando no hay pisos válidos (se asume 1 piso, mínimo conservador).
# MAGIC 8. **Centroide:** se calcula en 9377 (en grados se distorsiona) y se vuelve a 4326 para `lon`, `lat`.
# MAGIC 9. **Estado estructural:** `structural_status` conserva el texto original; `structural_status_code` (`no_change`, `roof_loss`, `debris`, `no_data`), `damage_level` ordinal (0, 1, 2; `null` si no hay datos) y `has_satellite_data`.
# MAGIC    Ojo: `no_change` **no** significa "sin daño": es lo que el satélite no alcanza a ver.
# MAGIC 10. **Descartadas:** `OBJECTID_1` (100% nulo) y `Shape__Area`/`Shape__Length` (en grados, reemplazadas por las métricas).
# MAGIC 11. **`parcel_id`:** `ID_PREDIO` como long; `0` y nulo pasan a `null` (no son identificadores válidos).
# MAGIC
# MAGIC ### Barrio y comuna oficiales
# MAGIC `commune_code/commune_name/neighborhood_code/neighborhood_name` salen del cruce espacial del **centroide** con los polígonos DIVIPOLA 2026 (`silver_el_equipo_neighborhoods`), que ubica a los 5.146 edificios.
# MAGIC Son las columnas para el mapa, los textos y las agregaciones. El dataset trae el código **catastral**, que el Acuerdo 0636 de 2026 cambió u omitió en algunos barrios (ej. catastral `1994` "Pampalinda" → oficial "Pampa Linda"):
# MAGIC `cadastral_neighborhood_name` es el nombre por código catastral (DIVIPOLA si el código existe, si no el del catastro) e `is_neighborhood_code_current` dice si el código catastral coincide con el oficial.
# MAGIC La traducción código → barrio oficial queda en `silver_el_equipo_neighborhood_crosswalk`.
# MAGIC
# MAGIC ### Catastro
# MAGIC `cadastre_match_method`: primero por `parcel_id` válido (`parcel_id`; si hay varios registros, el de mayor `cadastre_edited_date` y luego menor `national_parcel_number`).
# MAGIC Para los que no unieron, por `connection_code` (COMUNA+BARRIO+MANZANA+PREDIO) **solo si esa conexión tiene exactamente un registro** (`connection`);
# MAGIC si tiene varios (varias unidades en el terreno) no se adivina y queda `ambiguous_connection`; sin ninguna coincidencia, `no_match`.

# COMMAND ----------

import json
import pandas as pd
import geopandas as gpd
import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import shape
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               DoubleType, BooleanType, IntegerType, DateType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

bronze_table = f"{catalog}.{schema}.bronze_{team}_hackathon_buildings"
silver_table = f"{catalog}.{schema}.silver_{team}_buildings"
neighborhoods_t = f"{catalog}.{schema}.silver_{team}_neighborhoods"
cad_neighborhoods_t = f"{catalog}.{schema}.silver_{team}_cadastre_neighborhoods"
parcels_t = f"{catalog}.{schema}.silver_{team}_cadastre_parcels"

CRS_GEO = "EPSG:4326"
CRS_M = "EPSG:9377"

# Nombres originales -> convención (docs/conventions.md)
RENAME = {
    "OBJECTID": "building_id", "DEPAPRED": "department_code", "MUNIPRED": "municipality_code",
    "TIPO_AVALU": "zone_type_code", "SECTOR": "sector_code", "COMUNA": "cadastral_commune_code",
    "MANZANA": "block_code", "TERRENO": "lot_code", "COND": "ownership_condition_code",
    "EDIFPRED": "building_code", "PREDIO": "parcel_code", "NUMEPRED": "parcel_number",
    "NPISOS": "floor_count_raw", "Indice_Escombros": "debris_index",
    "Estado_Estructural": "structural_status",
    # auxiliares: solo sirven para armar llaves y no se guardan
    "BARRIO": "neighborhood_part_code", "ID_PREDIO": "parcel_id_raw",
}
# Descartadas: 100% nulo (OBJECTID_1) o en grados, reemplazadas por las métricas en EPSG:9377
DROP = ["OBJECTID_1", "Shape__Area", "Shape__Length"]

missing = [t for t in [neighborhoods_t, cad_neighborhoods_t, parcels_t] if not spark.catalog.tableExists(t)]
assert not missing, f"Faltan tablas de referencias {missing}: corre antes silver_references."

# COMMAND ----------

# Bronze -> pandas (5 mil filas, cabe sin problema en el driver)
pdf = spark.table(bronze_table).toPandas()

def clean_str(v):
    """trim; vacío o solo espacios -> None"""
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return None if pd.isna(v) else v

str_cols = [c for c in pdf.columns if pd.api.types.is_string_dtype(pdf[c].dtype)]
for c in str_cols:
    pdf[c] = pdf[c].map(clean_str)

# COMMAND ----------

# Geometría: GeoJSON texto -> shapely -> GeoDataFrame en 4326
geoms = pdf["geometry"].map(lambda g: shape(json.loads(g)))
gdf = gpd.GeoDataFrame(pdf.drop(columns=["geometry"] + DROP),
                       geometry=list(geoms), crs=CRS_GEO)
gdf = gdf.rename(columns=RENAME)

def only_polygons(g):
    """make_valid puede devolver GeometryCollection; nos quedamos con lo poligonal"""
    if g.geom_type in ("Polygon", "MultiPolygon"):
        return g
    parts = [p for p in shapely.get_parts(g) if p.geom_type in ("Polygon", "MultiPolygon")]
    return shapely.union_all(parts) if parts else shapely.Polygon()

gdf["is_geometry_valid"] = gdf.geometry.is_valid
gdf["is_geometry_repaired"] = ~gdf["is_geometry_valid"]
fixed = gdf.geometry.map(lambda g: g if g.is_valid else only_polygons(shapely.make_valid(g)))
gdf = gdf.set_geometry(fixed)

# COMMAND ----------

# Medidas en metros (EPSG:9377)
gm = gdf.to_crs(CRS_M)
gdf["area_m2"] = gm.geometry.area
gdf["perimeter_m"] = gm.geometry.length
gdf["is_suspect_polygon"] = (gm.geometry.is_empty | (gdf["area_m2"] < 5) | (gdf["area_m2"] > 50000))

# Centroide calculado en métrico y devuelto a 4326
cent = gpd.GeoSeries(gm.geometry.centroid, crs=CRS_M).to_crs(CRS_GEO)
gdf["lon"] = cent.x
gdf["lat"] = cent.y

# COMMAND ----------

# Llaves, pisos y estado estructural
commune, neigh, block = gdf["cadastral_commune_code"], gdf["neighborhood_part_code"], gdf["block_code"]
gdf["cadastral_neighborhood_code"] = commune + neigh  # 'CCBB'; null si falta alguno
gdf["cadastral_block_code"] = commune + neigh + block  # 'CCBBMMMM'

pid = pd.to_numeric(gdf["parcel_id_raw"], errors="coerce")
gdf["parcel_id"] = pid.where(pid > 0).astype("Int64")  # null y 0 no son ID válidos

floors = gdf["floor_count_raw"].astype("Int64")
gdf["floor_count_raw"] = floors  # entero con nulos (evita 1.0 al pasar a Spark LongType)
gdf["floor_count"] = floors.where(floors > 0)
gdf["is_floor_count_missing"] = gdf["floor_count"].isna()
gdf["built_area_m2"] = gdf["area_m2"] * gdf["floor_count"].fillna(1).astype(float)
gdf["is_built_area_estimated"] = gdf["floor_count"].isna()

def status_code(s):
    """Match por prefijo, robusto a tildes/encoding"""
    if s is None:
        return "no_data"
    t = s.lower()
    if t.startswith("sin cambio"):
        return "no_change"
    if t.startswith("posible"):
        return "roof_loss"
    if t.startswith("alta probabilidad"):
        return "debris"
    return "no_data"

DAMAGE = {"no_change": 0, "roof_loss": 1, "debris": 2}
gdf["structural_status_code"] = gdf["structural_status"].map(status_code)
gdf["damage_level"] = gdf["structural_status_code"].map(DAMAGE).astype("Int64")
gdf["has_satellite_data"] = gdf["structural_status_code"] != "no_data"

gdf["geom_wkt"] = gdf.geometry.to_wkt()  # 4326

assert gdf["building_id"].is_unique, "building_id debe ser llave única"

# COMMAND ----------

# Barrio y comuna OFICIALES: centroide dentro de los polígonos DIVIPOLA 2026
nei = spark.table(neighborhoods_t).toPandas()
poly = nei.sort_values("neighborhood_code")
polys = gpd.GeoDataFrame(
    {"neighborhood_code": poly["neighborhood_code"].values},
    geometry=[shapely_wkt.loads(w) for w in poly["geom_wkt"]], crs=CRS_GEO)
pts = gpd.GeoDataFrame(geometry=gpd.points_from_xy(gdf["lon"], gdf["lat"]), index=gdf.index, crs=CRS_GEO)
# within en 4326 basta para punto-en-polígono; si cae en 2 polígonos se queda el de menor código
hit = gpd.sjoin(pts, polys, how="inner", predicate="within")
hit = hit.sort_values("neighborhood_code")
hit = hit[~hit.index.duplicated(keep="first")]
gdf["neighborhood_code"] = hit["neighborhood_code"].reindex(gdf.index)
gdf["neighborhood_name"] = gdf["neighborhood_code"].map(dict(zip(nei["neighborhood_code"], nei["neighborhood_name"])))
gdf["commune_code"] = gdf["neighborhood_code"].str[:2]
gdf["commune_name"] = gdf["neighborhood_code"].map(dict(zip(nei["neighborhood_code"], nei["commune_name"])))

# Nombre por código catastral: DIVIPOLA si el código existe, si no el del catastro
cad = spark.table(cad_neighborhoods_t).toPandas()
n_div = gdf["cadastral_neighborhood_code"].map(dict(zip(nei["neighborhood_code"], nei["neighborhood_name"])))
n_cad = gdf["cadastral_neighborhood_code"].map(dict(zip(cad["cadastral_neighborhood_code"], cad["cadastral_neighborhood_name"])))
gdf["cadastral_neighborhood_name"] = n_div.where(n_div.notna(), n_cad)
gdf["is_neighborhood_code_current"] = (
    (gdf["cadastral_neighborhood_code"] == gdf["neighborhood_code"]).astype(object)
    .where(gdf["cadastral_neighborhood_code"].notna() & gdf["neighborhood_code"].notna(), None))

# COMMAND ----------

# Catastro: por parcel_id, luego por connection_code (solo si es unívoco)
terr = spark.table(parcels_t).toPandas()
terr["_f"] = pd.to_datetime(terr["cadastre_edited_date"])
terr["cadastre_edited_date"] = terr["_f"].dt.date
terr = terr.sort_values(["_f", "national_parcel_number"], ascending=[False, True], na_position="last")
cat_cols = ["national_parcel_number", "land_use", "economic_destination_code", "cadastre_floor_count",
            "cadastre_built_area_m2", "address", "cadastre_edited_date"]

by_id = terr[terr["parcel_id"].notna()].drop_duplicates("parcel_id").set_index("parcel_id")
key_id = gdf["parcel_id"].map(lambda v: None if pd.isna(v) else int(v))
hit_id = key_id.isin(by_id.index)

cnt = terr[terr["connection_code"].notna()].groupby("connection_code").size()
uniq = terr[terr["connection_code"].map(cnt) == 1].set_index("connection_code")
parts = [gdf["cadastral_commune_code"], gdf["neighborhood_part_code"], gdf["block_code"], gdf["parcel_code"]]
ok = pd.Series(True, index=gdf.index)
for part, n_len in zip(parts, (2, 2, 4, 4)):
    ok &= (part.notna() & (part.str.len() == n_len) & part.str.isdigit()).fillna(False).astype(bool)
key_cx = (parts[0] + parts[1] + parts[2] + parts[3]).where(ok)
n_cx = key_cx.map(cnt)

gdf["cadastre_match_method"] = "no_match"
gdf.loc[n_cx > 1, "cadastre_match_method"] = "ambiguous_connection"
gdf.loc[n_cx == 1, "cadastre_match_method"] = "connection"
gdf.loc[hit_id, "cadastre_match_method"] = "parcel_id"
for c in cat_cols:
    v_id = key_id.map(by_id[c]).astype(object)
    v_cx = key_cx.map(uniq[c]).astype(object)
    gdf[c] = v_id.where(hit_id, v_cx.where(n_cx == 1, None))
gdf["cadastre_floor_count"] = pd.to_numeric(gdf["cadastre_floor_count"], errors="coerce").astype("Int64")
gdf["cadastre_built_area_m2"] = pd.to_numeric(gdf["cadastre_built_area_m2"], errors="coerce")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios de tabla y columna
# MAGIC Formato: descripción, unidad y `Origen:`. Se aplican después de guardar (`docs/data_dictionary.md` se mantiene idéntico a estos textos).

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

DOCS = {
    "buildings": {
        "table": "Edificios de Cali afectados por el sismo del 10 de agosto, una fila por edificio (5.146). Datos del hackathon limpios, geometría en EPSG:4326 con medidas en EPSG:9377, barrio y comuna oficiales por ubicación y datos del catastro.",
        "columns": {
            "building_id": "Identificador único del edificio (1 a 5.146). Origen: bronze, OBJECTID.",
            "department_code": "Código DANE del departamento (76 = Valle del Cauca). Origen: bronze, DEPAPRED.",
            "municipality_code": "Código DANE del municipio (001 = Santiago de Cali). Origen: bronze, MUNIPRED.",
            "zone_type_code": "Código del tipo de avalúo / zona (ej. '01' urbana). Origen: bronze, TIPO_AVALU.",
            "sector_code": "Código del sector catastral. Origen: bronze, SECTOR.",
            "cadastral_commune_code": "Código catastral de la comuna, 2 dígitos; nulo en 2 edificios. Origen: bronze, COMUNA.",
            "cadastral_neighborhood_code": "Código catastral del barrio, 4 dígitos 'CCBB' (comuna + barrio, sin guion); puede no coincidir con el oficial de 2026. Origen: bronze, COMUNA + BARRIO.",
            "block_code": "Código catastral de la manzana, 4 dígitos. Origen: bronze, MANZANA.",
            "cadastral_block_code": "Código catastral de manzana completo 'CCBBMMMM' (8 dígitos); nulo si falta un componente. Origen: bronze, COMUNA + BARRIO + MANZANA.",
            "lot_code": "Código del terreno (lote) dentro de la manzana; nulo si venía vacío. Origen: bronze, TERRENO.",
            "ownership_condition_code": "Código de condición de la propiedad (ej. propiedad horizontal); nulo si venía vacío. Origen: bronze, COND.",
            "building_code": "Código del edificio dentro del predio; nulo si venía vacío. Origen: bronze, EDIFPRED.",
            "parcel_code": "Código del predio dentro del terreno. Origen: bronze, PREDIO.",
            "parcel_number": "Número del predio; nulo si venía vacío. Origen: bronze, NUMEPRED.",
            "parcel_id": "Identificador del predio en el catastro; nulo si venía vacío o en 0. Origen: bronze, ID_PREDIO.",
            "floor_count_raw": "Número de pisos tal como venía, incluidos los ceros. Origen: bronze, NPISOS.",
            "floor_count": "Número de pisos válido (mayor que 0); nulo si el dato falta. Origen: derivado de NPISOS.",
            "is_floor_count_missing": "Verdadero si no hay número de pisos válido (floor_count nulo). Origen: derivado de NPISOS.",
            "debris_index": "Índice de escombros satelital. Origen: bronze, Indice_Escombros.",
            "structural_status": "Estado estructural inferido por satélite, texto original en español. Origen: bronze, Estado_Estructural.",
            "structural_status_code": "Estado estructural codificado: no_change, roof_loss, debris o no_data. Origen: derivado de structural_status.",
            "damage_level": "Nivel de daño ordinal: 0 no_change, 1 roof_loss, 2 debris; nulo si no hay datos. Origen: derivado de structural_status_code.",
            "has_satellite_data": "Verdadero si el edificio tiene datos satelitales (estado distinto de no_data). Origen: derivado de structural_status_code.",
            "area_m2": "Área de la huella en m² calculada en EPSG:9377. Origen: geometría reproyectada.",
            "perimeter_m": "Perímetro de la huella en m calculado en EPSG:9377. Origen: geometría reproyectada.",
            "built_area_m2": "Área construida estimada en m²: area_m2 por floor_count (1 piso si falta). Origen: derivado de area_m2 y floor_count.",
            "is_built_area_estimated": "Verdadero si built_area_m2 asumió 1 piso por falta de floor_count. Origen: derivado de floor_count.",
            "is_geometry_valid": "Verdadero si la geometría original era válida. Origen: validación con shapely.",
            "is_geometry_repaired": "Verdadero si la geometría original era inválida y se reparó con make_valid. Origen: validación con shapely.",
            "is_suspect_polygon": "Verdadero si el polígono es dudoso: área menor de 5 m², mayor de 50.000 m² o vacío. No se elimina. Origen: derivado de area_m2.",
            "lon": "Longitud del centroide en grados, EPSG:4326. Origen: centroide calculado en EPSG:9377.",
            "lat": "Latitud del centroide en grados, EPSG:4326. Origen: centroide calculado en EPSG:9377.",
            "geom_wkt": "Geometría del edificio en WKT, EPSG:4326 (lon, lat), ya reparada. Origen: bronze, geometry.",
            "commune_code": "Código oficial de la comuna (2 dígitos) según la ubicación del centroide. Origen: cruce espacial con silver_el_equipo_neighborhoods (DIVIPOLA 2026).",
            "commune_name": "Nombre oficial de la comuna según la ubicación. Origen: silver_el_equipo_neighborhoods.",
            "neighborhood_code": "Código oficial del barrio (4 dígitos 'CCBB') según la ubicación del centroide. Origen: cruce espacial con silver_el_equipo_neighborhoods (DIVIPOLA 2026).",
            "neighborhood_name": "Nombre oficial del barrio según la ubicación. Origen: silver_el_equipo_neighborhoods.",
            "cadastral_neighborhood_name": "Nombre del barrio según el código catastral (DIVIPOLA si el código existe, si no el del catastro); nulo si no hay código. Origen: silver_el_equipo_neighborhoods y silver_el_equipo_cadastre_neighborhoods.",
            "is_neighborhood_code_current": "Verdadero si el código catastral del barrio coincide con el oficial; nulo si falta alguno. Origen: cadastral_neighborhood_code = neighborhood_code.",
            "national_parcel_number": "Número Predial Nacional (30 dígitos) del catastro; nulo si no hay match. Origen: silver_el_equipo_cadastre_parcels.",
            "land_use": "Uso principal del predio según el catastro (ej. 'RESIDENCIAL'). Origen: silver_el_equipo_cadastre_parcels.",
            "economic_destination_code": "Código de destinación económica del predio según el catastro. Origen: silver_el_equipo_cadastre_parcels.",
            "cadastre_floor_count": "Número de pisos según el catastro. Origen: silver_el_equipo_cadastre_parcels.",
            "cadastre_built_area_m2": "Área construida según el catastro, en m². Origen: silver_el_equipo_cadastre_parcels.",
            "address": "Dirección del predio según el catastro. Origen: silver_el_equipo_cadastre_parcels.",
            "cadastre_edited_date": "Fecha de última edición del registro catastral. Origen: silver_el_equipo_cadastre_parcels.",
            "cadastre_match_method": "Cómo se unió con el catastro: parcel_id, connection (conexión única), ambiguous_connection (varias unidades, no se adivina) o no_match. Origen: derivado del cruce con silver_el_equipo_cadastre_parcels.",
        },
    },
}

# COMMAND ----------

# Selección final (orden de la convención) y tipos explícitos
out_cols = [
    ("building_id", LongType()), ("department_code", StringType()), ("municipality_code", StringType()),
    ("zone_type_code", StringType()), ("sector_code", StringType()), ("cadastral_commune_code", StringType()),
    ("cadastral_neighborhood_code", StringType()), ("block_code", StringType()),
    ("cadastral_block_code", StringType()), ("lot_code", StringType()),
    ("ownership_condition_code", StringType()), ("building_code", StringType()),
    ("parcel_code", StringType()), ("parcel_number", StringType()), ("parcel_id", LongType()),
    ("floor_count_raw", LongType()), ("floor_count", LongType()), ("is_floor_count_missing", BooleanType()),
    ("debris_index", DoubleType()), ("structural_status", StringType()),
    ("structural_status_code", StringType()), ("damage_level", LongType()), ("has_satellite_data", BooleanType()),
    ("area_m2", DoubleType()), ("perimeter_m", DoubleType()), ("built_area_m2", DoubleType()),
    ("is_built_area_estimated", BooleanType()), ("is_geometry_valid", BooleanType()),
    ("is_geometry_repaired", BooleanType()), ("is_suspect_polygon", BooleanType()),
    ("lon", DoubleType()), ("lat", DoubleType()), ("geom_wkt", StringType()),
    ("commune_code", StringType()), ("commune_name", StringType()), ("neighborhood_code", StringType()),
    ("neighborhood_name", StringType()), ("cadastral_neighborhood_name", StringType()),
    ("is_neighborhood_code_current", BooleanType()),
    ("national_parcel_number", StringType()), ("land_use", StringType()),
    ("economic_destination_code", StringType()), ("cadastre_floor_count", IntegerType()),
    ("cadastre_built_area_m2", DoubleType()), ("address", StringType()),
    ("cadastre_edited_date", DateType()), ("cadastre_match_method", StringType()),
]
sch = StructType([StructField(n, t) for n, t in out_cols])
names = [n for n, _ in out_cols]

flat = pd.DataFrame(gdf[names]).astype(object)
flat = flat.where(flat.notna(), None)  # NaN / pd.NA -> None (null en Spark)
rows = [tuple(r) for r in flat.itertuples(index=False, name=None)]
silver = spark.createDataFrame(rows, schema=sch)

(silver.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(silver_table))
apply_comments(silver_table, DOCS["buildings"]["table"], DOCS["buildings"]["columns"])
print(f"Guardada {silver_table}: {silver.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calidad
# MAGIC Nulos por columna, polígonos dudosos, reparados y qué parte del área construida no tiene dato satelital.

# COMMAND ----------

s = spark.table(silver_table)
nulls = s.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in s.columns])
display(nulls)

# COMMAND ----------

summary = s.agg(
    F.count("*").alias("n"),
    F.sum(F.col("is_suspect_polygon").cast("int")).alias("suspect_polygons"),
    F.sum(F.col("is_geometry_repaired").cast("int")).alias("repaired_geometries"),
    F.sum(F.when(~F.col("has_satellite_data"), F.col("built_area_m2")).otherwise(0)).alias("no_data_area"),
    F.sum("built_area_m2").alias("total_area"),
).collect()[0]
print(f"Polígonos dudosos: {summary['suspect_polygons']}")
print(f"Geometrías reparadas: {summary['repaired_geometries']}")
print(f"% área construida sin datos satelitales: {100 * summary['no_data_area'] / summary['total_area']:.1f}%")
display(s.select("area_m2", "perimeter_m", "built_area_m2").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría
# MAGIC 5.146 filas con `building_id` único; `neighborhood_code` oficial presente en todas las filas y con 4 dígitos; % por `cadastre_match_method`;
# MAGIC % de códigos catastrales vigentes; y comparación de pisos del dataset con el catastro.

# COMMAND ----------

n = s.count()
assert n == 5146, n
assert s.select("building_id").distinct().count() == 5146, "building_id debe ser único"
assert s.filter("neighborhood_code IS NULL OR length(neighborhood_code) <> 4").count() == 0, "neighborhood_code debe tener 4 dígitos"
print("--- % por cadastre_match_method")
display(s.groupBy("cadastre_match_method").count().withColumn("pct", F.round(100 * F.col("count") / n, 1)).orderBy(F.desc("count")))
cur = s.agg(
    F.sum(F.col("is_neighborhood_code_current").cast("int")).alias("current"),
    F.count("is_neighborhood_code_current").alias("with_code")).collect()[0]
print(f"Código catastral vigente: {cur['current']} de {cur['with_code']} "
      f"({100 * cur['current'] / max(cur['with_code'], 1):.1f}%)")
cmp = s.filter("cadastre_floor_count is not null").agg(
    F.count("*").alias("with_cadastre_floors"),
    F.sum((F.col("floor_count_raw") == F.col("cadastre_floor_count")).cast("int")).alias("same_floors"),
    F.sum(((F.col("floor_count_raw") == 0) & (F.col("cadastre_floor_count") > 0)).cast("int")).alias("zero_with_cadastre"),
).collect()[0]
print(f"Con cadastre_floor_count: {cmp['with_cadastre_floors']} | floor_count_raw igual: {cmp['same_floors']} "
      f"| floor_count_raw=0 y cadastre_floor_count>0: {cmp['zero_with_cadastre']}")
