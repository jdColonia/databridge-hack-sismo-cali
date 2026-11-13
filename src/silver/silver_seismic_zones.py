# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: microzonificación sísmica y zona sísmica de cada edificio
# MAGIC Dos tablas:
# MAGIC
# MAGIC 1. `silver_el_equipo_seismic_zones`: **16 polígonos** de la microzonificación (INGEOMINAS–DAGMA 2005, Decreto 411.0.20.0158 de 2014), limpios y con nombres según `docs/conventions.md`.
# MAGIC 2. `silver_el_equipo_building_seismic_zones`: **una fila por edificio (5.146)** con la zona que le corresponde y cuánto de su huella cae dentro.
# MAGIC
# MAGIC ### Decisiones de la tabla de zonas
# MAGIC - **Llave:** `seismic_zone_id` = `feature_id` del WFS. **Nunca se deduplica por nombre**: `Zona 6` aparece 2 veces y `Cauce Activo o Terra` 3, con distinto `sucep_licu`/`corrim_lat`.
# MAGIC - **Los ceros no son cero:** en `Depositos Antropicos`, `Suelo Coluvion` y `Cauce Activo o Terra` todos los valores `*_etc` son `0`, que significa **"requiere estudio particular"**, no aceleración nula.
# MAGIC   Pasan a `NULL` y la bandera `requires_site_study` los identifica.
# MAGIC - **Dos espectros (ETC y ETL):** el estudio publica dos conjuntos de parámetros (`*_etc` y `*_etl`). Su significado exacto está **por verificar en el decreto**; los `etl` vienen como texto (`'0.40'`), solo existen en Zona 4B, 5 y 4C, y se castean a `double` (vacío → `NULL`).
# MAGIC    Notación del estudio: `am` aceleración máxima (g), `sm` aceleración de la meseta espectral (g), `t0`/`tc`/`tl` periodos (s).
# MAGIC - **Descartadas:** columnas basura `mcooiiain_`, `mcooiiain1`, `mcooiiai_1`, `mcooiiai_2`, `label` y `area`/`perimeter` originales (en el CRS de origen); el área se recalcula en **EPSG:9377**.
# MAGIC - **Geometría:** se guarda en EPSG:4326 como `geom_wkt`; si venía inválida se repara con `make_valid`.
# MAGIC
# MAGIC ### Decisiones del cruce edificio → zona
# MAGIC - Se reproyectan edificios y zonas a **EPSG:9377** y se cruzan con `intersects`; se calcula el área de intersección de cada par.
# MAGIC - A cada edificio se le asigna la zona con **mayor área de intersección** (empate: menor `seismic_zone_id`). Esto cubre edificios que cruzan el borde entre dos zonas.
# MAGIC - `seismic_zone_overlap_count` = cuántos polígonos toca la huella; `seismic_zone_overlap_pct` = porcentaje (0 a 100) de la huella dentro de la zona elegida. Un porcentaje bajo avisa que la asignación es dudosa.
# MAGIC - Un edificio que no toca ninguna zona conserva `NULL` en las columnas de zona y `overlap_count = 0`. **No se pierde ninguna fila.**
# MAGIC - Solo se llevan a la tabla del edificio las columnas que sirven para el análisis (aceleración máxima, meseta, periodo `tc` del espectro ETC y banderas); el resto está en la tabla de zonas.

# COMMAND ----------

import json
import pandas as pd
import geopandas as gpd
import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import shape
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType,
                               BooleanType, IntegerType)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

bronze_table = f"{catalog}.{schema}.bronze_{team}_idesc_seismic_zones"
buildings_t = f"{catalog}.{schema}.silver_{team}_buildings"
zones_table = f"{catalog}.{schema}.silver_{team}_seismic_zones"
building_zones_table = f"{catalog}.{schema}.silver_{team}_building_seismic_zones"

CRS_GEO = "EPSG:4326"
CRS_M = "EPSG:9377"

missing = [t for t in [bronze_table, buildings_t] if not spark.catalog.tableExists(t)]
assert not missing, f"Faltan tablas {missing}: corre antes bronze_idesc_seismic_zones y silver_buildings."

# COMMAND ----------

def only_polygons(g):
    """make_valid puede devolver GeometryCollection; nos quedamos con lo poligonal"""
    if g.geom_type in ("Polygon", "MultiPolygon"):
        return g
    parts = [p for p in shapely.get_parts(g) if p.geom_type in ("Polygon", "MultiPolygon")]
    return shapely.union_all(parts) if parts else shapely.Polygon()

def repair(g):
    return g if g.is_valid else only_polygons(shapely.make_valid(g))

def clean_str(v):
    """trim; vacío o solo espacios -> None"""
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return None if pd.isna(v) else v

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de zonas (16 polígonos)

# COMMAND ----------

raw = spark.table(bronze_table).toPandas()
assert len(raw) == 16, len(raw)

geoms = raw["geometry"].map(lambda g: repair(shape(json.loads(g))))
zones = gpd.GeoDataFrame(raw.drop(columns=["geometry"]), geometry=list(geoms), crs=CRS_GEO)

out = pd.DataFrame(index=zones.index)
out["seismic_zone_id"] = zones["feature_id"].astype(str)
out["seismic_zone_name"] = zones["zona_mzsc"].map(clean_str)

# ETC: 0 significa "requiere estudio particular" -> NULL
etc_map = {"etc_am_g": "am_etc", "etc_sm_g": "sm_etc", "etc_t0_s": "to_etc",
           "etc_tc_s": "tc_etc", "etc_tl_s": "tl_etc"}
for new, old in etc_map.items():
    v = pd.to_numeric(zones[old], errors="coerce")
    out[new] = v.where(v != 0)

# ETL: texto ('0.40') -> double; vacío -> NULL
etl_map = {"etl_am_g": "am_etl", "etl_sm_g": "sm_etl", "etl_t0_s": "to_etl",
           "etl_tc_s": "tc_etl", "etl_tl_s": "tl_etl"}
for new, old in etl_map.items():
    out[new] = pd.to_numeric(zones[old].map(clean_str), errors="coerce")

out["is_liquefaction_susceptible"] = zones["sucep_licu"] == 1
out["has_lateral_spread_risk"] = zones["corrim_lat"] == 1
out["requires_site_study"] = out[list(etc_map)].isna().all(axis=1)

zones_m = zones.to_crs(CRS_M)
out["area_m2"] = zones_m.geometry.area
out["geom_wkt"] = zones.geometry.to_wkt()  # 4326

out = out.sort_values("seismic_zone_id").reset_index(drop=True)
zone_geoms_m = zones_m.set_index(zones["feature_id"].astype(str).values).geometry  # para el cruce

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

ETC_ETL = ("ETC y ETL son los dos espectros de la microzonificación sísmica (estudio INGEOMINAS–DAGMA 2005, "
           "Decreto 411.0.20.0158 de 2014); su significado exacto está por verificar en el decreto.")

DOCS = {
    "seismic_zones": {
        "table": "Zonas de la microzonificación sísmica de Cali, una fila por polígono (16). " + ETC_ETL +
                 " Notación del estudio: am = aceleración máxima (g), sm = aceleración de la meseta espectral (g), t0/tc/tl = periodos (s). "
                 "Un nombre de zona puede repetirse con distintos riesgos: la llave es seismic_zone_id. "
                 "Origen: bronze_el_equipo_idesc_seismic_zones (IDESC, capa mc_microzonificacion_sismica).",
        "columns": {
            "seismic_zone_id": "Identificador del polígono (id del feature en el WFS del IDESC); es la llave, el nombre de zona se repite. Origen: bronze, feature_id.",
            "seismic_zone_name": "Nombre de la zona de microzonificación (ej. 'Zona 4C'); puede repetirse (Zona 6 x2, Cauce Activo o Terra x3). Origen: bronze, zona_mzsc.",
            "etc_am_g": "Espectro ETC, aceleración máxima (am) en g; nulo si la zona requiere estudio particular. Origen: bronze, am_etc (0 pasa a nulo).",
            "etc_sm_g": "Espectro ETC, aceleración de la meseta espectral (sm) en g; nulo si la zona requiere estudio particular. Origen: bronze, sm_etc (0 pasa a nulo).",
            "etc_t0_s": "Espectro ETC, periodo t0 en segundos; nulo si la zona requiere estudio particular. Origen: bronze, to_etc (0 pasa a nulo).",
            "etc_tc_s": "Espectro ETC, periodo tc en segundos; nulo si la zona requiere estudio particular. Origen: bronze, tc_etc (0 pasa a nulo).",
            "etc_tl_s": "Espectro ETC, periodo tl en segundos; nulo si la zona requiere estudio particular. Origen: bronze, tl_etc (0 pasa a nulo).",
            "etl_am_g": "Espectro ETL, aceleración máxima (am) en g; solo Zona 4B, 5 y 4C, el resto nulo. Origen: bronze, am_etl (texto a double).",
            "etl_sm_g": "Espectro ETL, aceleración de la meseta espectral (sm) en g; solo Zona 4B, 5 y 4C, el resto nulo. Origen: bronze, sm_etl (texto a double).",
            "etl_t0_s": "Espectro ETL, periodo t0 en segundos; solo Zona 4B, 5 y 4C, el resto nulo. Origen: bronze, to_etl (texto a double).",
            "etl_tc_s": "Espectro ETL, periodo tc en segundos; solo Zona 4B, 5 y 4C, el resto nulo. Origen: bronze, tc_etl (texto a double).",
            "etl_tl_s": "Espectro ETL, periodo tl en segundos; solo Zona 4B, 5 y 4C, el resto nulo. Origen: bronze, tl_etl (texto a double).",
            "is_liquefaction_susceptible": "Verdadero si el suelo de la zona es susceptible a licuación. Origen: bronze, sucep_licu = 1.",
            "has_lateral_spread_risk": "Verdadero si la zona tiene riesgo de corrimiento lateral. Origen: bronze, corrim_lat = 1.",
            "requires_site_study": "Verdadero si la zona requiere estudio particular de sitio: todos los valores ETC venían en 0, que no significa aceleración nula. Origen: derivado de los ETC.",
            "area_m2": "Área del polígono en m² recalculada en EPSG:9377. Origen: geometría reproyectada.",
            "geom_wkt": "Geometría de la zona en WKT, EPSG:4326 (lon, lat), reparada con make_valid si era inválida. Origen: bronze, geometry.",
        },
    },
    "building_seismic_zones": {
        "table": "Zona de microzonificación sísmica de cada edificio, una fila por edificio (5.146). Se asigna la zona con mayor área de intersección con la huella (cruce en EPSG:9377); "
                 "los edificios que no tocan ninguna zona quedan con nulos. " + ETC_ETL +
                 " Origen: silver_el_equipo_buildings y silver_el_equipo_seismic_zones.",
        "columns": {
            "building_id": "Identificador del edificio. Origen: silver_el_equipo_buildings.",
            "seismic_zone_id": "Identificador de la zona elegida (mayor área de intersección; empate, el menor id); nulo si el edificio no toca ninguna zona. Origen: silver_el_equipo_seismic_zones.",
            "seismic_zone_name": "Nombre de la zona elegida; nulo si no toca ninguna. Origen: silver_el_equipo_seismic_zones.",
            "etc_am_g": "Espectro ETC, aceleración máxima (am) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. Origen: silver_el_equipo_seismic_zones.",
            "etc_sm_g": "Espectro ETC, aceleración de la meseta espectral (sm) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. Origen: silver_el_equipo_seismic_zones.",
            "etc_tc_s": "Espectro ETC, periodo tc en segundos de la zona; nulo si la zona requiere estudio particular o no hay zona. Origen: silver_el_equipo_seismic_zones.",
            "is_liquefaction_susceptible": "Verdadero si la zona elegida es susceptible a licuación; nulo si no hay zona. Origen: silver_el_equipo_seismic_zones.",
            "has_lateral_spread_risk": "Verdadero si la zona elegida tiene riesgo de corrimiento lateral; nulo si no hay zona. Origen: silver_el_equipo_seismic_zones.",
            "requires_site_study": "Verdadero si la zona elegida requiere estudio particular de sitio (sin valores ETC); nulo si no hay zona. Origen: silver_el_equipo_seismic_zones.",
            "seismic_zone_overlap_count": "Número de polígonos de zona que toca la huella del edificio (0 si ninguno). Mayor que 1 indica un edificio sobre el borde entre zonas. Origen: cruce espacial en EPSG:9377.",
            "seismic_zone_overlap_pct": "Porcentaje (0 a 100) de la huella del edificio que cae dentro de la zona elegida; nulo si no hay zona. Origen: área de intersección sobre area_m2, en EPSG:9377.",
        },
    },
}

# COMMAND ----------

# Zonas: tipos explícitos y guardado
zone_cols = [
    ("seismic_zone_id", StringType()), ("seismic_zone_name", StringType()),
    ("etc_am_g", DoubleType()), ("etc_sm_g", DoubleType()), ("etc_t0_s", DoubleType()),
    ("etc_tc_s", DoubleType()), ("etc_tl_s", DoubleType()),
    ("etl_am_g", DoubleType()), ("etl_sm_g", DoubleType()), ("etl_t0_s", DoubleType()),
    ("etl_tc_s", DoubleType()), ("etl_tl_s", DoubleType()),
    ("is_liquefaction_susceptible", BooleanType()), ("has_lateral_spread_risk", BooleanType()),
    ("requires_site_study", BooleanType()), ("area_m2", DoubleType()), ("geom_wkt", StringType()),
]

def to_spark(pdf, cols):
    """pandas -> Spark con tipos explícitos (NaN / pd.NA -> null)"""
    flat = pdf[[n for n, _ in cols]].astype(object)
    flat = flat.where(flat.notna(), None)
    rows = [tuple(r) for r in flat.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows, schema=StructType([StructField(n, t) for n, t in cols]))

zones_sdf = to_spark(out, zone_cols)
(zones_sdf.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(zones_table))
apply_comments(zones_table, DOCS["seismic_zones"]["table"], DOCS["seismic_zones"]["columns"])
print(f"Guardada {zones_table}: {spark.table(zones_table).count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cruce edificio → zona
# MAGIC Edificios y zonas en EPSG:9377; `intersects` para ver todas las zonas que toca cada huella y área de intersección para elegir la principal.

# COMMAND ----------

b = spark.table(buildings_t).select("building_id", "geom_wkt").toPandas()
assert len(b) == 5146 and b["building_id"].is_unique, len(b)

bgeo = gpd.GeoDataFrame(
    {"building_id": b["building_id"].values},
    geometry=[shapely_wkt.loads(w) if w else shapely.Polygon() for w in b["geom_wkt"]], crs=CRS_GEO).to_crs(CRS_M)
bgeo["geometry"] = bgeo.geometry.map(repair)
bgeo["building_area_m2"] = bgeo.geometry.area

zgeo = gpd.GeoDataFrame({"seismic_zone_id": zone_geoms_m.index.values},
                        geometry=[repair(g) for g in zone_geoms_m.values], crs=CRS_M)

pairs = gpd.sjoin(bgeo[["building_id", "building_area_m2", "geometry"]], zgeo,
                  how="inner", predicate="intersects")
zone_by_pos = zgeo.set_index("seismic_zone_id").geometry
pairs["inter_area_m2"] = [
    g.intersection(zone_by_pos.loc[z]).area for g, z in zip(pairs.geometry, pairs["seismic_zone_id"])
]
pairs = pd.DataFrame(pairs[["building_id", "building_area_m2", "seismic_zone_id", "inter_area_m2"]])

# Solo cuentan las zonas con área compartida (> 0,01 m²): tocar el borde no es estar dentro
overlap_count = (pairs["inter_area_m2"] > 0.01).groupby(pairs["building_id"]).sum().rename("seismic_zone_overlap_count")
best = (pairs.sort_values(["building_id", "inter_area_m2", "seismic_zone_id"], ascending=[True, False, True])
             .drop_duplicates("building_id", keep="first"))
best["seismic_zone_overlap_pct"] = (100 * best["inter_area_m2"] / best["building_area_m2"].where(best["building_area_m2"] > 0)).clip(upper=100)

zone_attrs = out[["seismic_zone_id", "seismic_zone_name", "etc_am_g", "etc_sm_g", "etc_tc_s",
                  "is_liquefaction_susceptible", "has_lateral_spread_risk", "requires_site_study"]]
bz = (b[["building_id"]]
      .merge(best[["building_id", "seismic_zone_id", "seismic_zone_overlap_pct"]], on="building_id", how="left")
      .merge(zone_attrs, on="seismic_zone_id", how="left")
      .merge(overlap_count, left_on="building_id", right_index=True, how="left"))
bz["seismic_zone_overlap_count"] = bz["seismic_zone_overlap_count"].fillna(0).astype(int)
bz = bz.sort_values("building_id").reset_index(drop=True)

# COMMAND ----------

bz_cols = [
    ("building_id", LongType()),
    ("seismic_zone_id", StringType()), ("seismic_zone_name", StringType()),
    ("etc_am_g", DoubleType()), ("etc_sm_g", DoubleType()), ("etc_tc_s", DoubleType()),
    ("is_liquefaction_susceptible", BooleanType()), ("has_lateral_spread_risk", BooleanType()),
    ("requires_site_study", BooleanType()),
    ("seismic_zone_overlap_count", IntegerType()), ("seismic_zone_overlap_pct", DoubleType()),
]
bz_sdf = to_spark(bz, bz_cols)
(bz_sdf.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(building_zones_table))
apply_comments(building_zones_table, DOCS["building_seismic_zones"]["table"],
               DOCS["building_seismic_zones"]["columns"])
print(f"Guardada {building_zones_table}: {spark.table(building_zones_table).count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría
# MAGIC - Zonas: 16 filas, `seismic_zone_id` único y ningún `0` en las columnas `etc_*` (los ceros pasaron a `NULL`).
# MAGIC - Edificios: 5.146 filas con `building_id` único.
# MAGIC - Distribución de edificios por zona, cuántos requieren estudio particular, cuántos tocan más de una zona y cuántos no tocan ninguna.
# MAGIC - Daño estructural por zona (sin `no_data` en el denominador). Es descriptivo: no prueba causalidad.

# COMMAND ----------

z = spark.table(zones_table)
assert z.count() == 16, z.count()
assert z.select("seismic_zone_id").distinct().count() == 16, "seismic_zone_id debe ser único"
etc_cols = [c for c in z.columns if c.startswith("etc_")]
zeros = z.filter(" OR ".join(f"{c} = 0" for c in etc_cols)).count()
assert zeros == 0, "los ceros de etc_* deben ser NULL"
print(f"Zonas que requieren estudio particular: {z.filter('requires_site_study').count()}")

s = spark.table(building_zones_table)
n = s.count()
assert n == 5146, n
assert s.select("building_id").distinct().count() == 5146, "building_id debe ser único"

print("--- Edificios por seismic_zone_name")
display(s.groupBy("seismic_zone_name").count().withColumn("pct", F.round(100 * F.col("count") / n, 1)).orderBy(F.desc("count")))
print(f"Edificios que requieren estudio particular: {s.filter('requires_site_study').count()}")
print(f"Edificios que tocan más de una zona (overlap_count > 1): {s.filter('seismic_zone_overlap_count > 1').count()}")
print(f"Edificios sin zona (overlap_count = 0): {s.filter('seismic_zone_overlap_count = 0').count()}")
display(s.filter("seismic_zone_overlap_count > 1").select("seismic_zone_overlap_pct").summary())

# COMMAND ----------

# Daño por zona: no_data se reporta aparte y no entra al denominador del % de daño
dmg = (s.join(spark.table(buildings_t).select("building_id", "structural_status_code"), "building_id")
        .groupBy("seismic_zone_name")
        .agg(F.count("*").alias("buildings"),
             F.sum((F.col("structural_status_code") == "no_change").cast("int")).alias("no_change"),
             F.sum((F.col("structural_status_code") == "roof_loss").cast("int")).alias("roof_loss"),
             F.sum((F.col("structural_status_code") == "debris").cast("int")).alias("debris"),
             F.sum((F.col("structural_status_code") == "no_data").cast("int")).alias("no_data"))
        .withColumn("damaged_pct_of_with_data",
                    F.round(100 * (F.col("roof_loss") + F.col("debris"))
                            / F.when(F.col("buildings") > F.col("no_data"), F.col("buildings") - F.col("no_data")), 1))
        .orderBy(F.desc("buildings")))
display(dmg)
