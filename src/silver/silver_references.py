# Databricks notebook source
# MAGIC %pip install geopandas shapely pyproj

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: referencias oficiales limpias
# MAGIC Decisiones:
# MAGIC
# MAGIC 1. **Comunas** (`silver_el_equipo_communes`): `commune_code` (2 dígitos, `trim`), nombre, URL del acuerdo y geometría en WKT (EPSG:4326).
# MAGIC 2. **Barrios oficiales** (`silver_el_equipo_neighborhoods`): los 342 barrios y sectores de la DIVIPOLA 2026 (Acuerdo 0636), con `neighborhood_code` en formato **`CCBB`**
# MAGIC    (comuna de 2 dígitos + barrio de 2 dígitos, sin guion, igual que el código DIVIPOLA), su comuna, categoría, acuerdo y geometría WKT.
# MAGIC 3. **Nombres catastrales de barrio** (`silver_el_equipo_cadastre_neighborhoods`, auxiliar): nombre que el catastro da a cada código `CCBB` del dataset.
# MAGIC    Existe porque el Acuerdo 0636 cambió u omitió 13 códigos del catastro; para esos, el nombre catastral es la única traducción del código.
# MAGIC 4. **Predios del catastro** (`silver_el_equipo_cadastre_parcels`): strings con `trim` y vacío a `null`, `parcel_id` long, pisos y área construida del catastro, fecha de edición como `date`.
# MAGIC    **No se deduplica aquí:** una misma `connection_code` (comuna+barrio+manzana+predio) puede tener varias unidades; esa decisión se toma en `silver_buildings`, donde se conoce qué edificio busca el dato.
# MAGIC 5. **Catálogo de códigos** (`silver_el_equipo_code_catalog`): el CSV limpio, con `verification_status` (`verified` / `pending_verification`).
# MAGIC
# MAGIC Cada tabla recibe comentarios de tabla y de columna en Databricks (`COMMENT`), que usan el catálogo y Genie.

# COMMAND ----------

import pandas as pd
import json
from shapely.geometry import shape
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")

b_communes = f"{catalog}.{schema}.bronze_{team}_idesc_communes"
b_neighborhoods = f"{catalog}.{schema}.bronze_{team}_idesc_neighborhoods"
b_parcels = f"{catalog}.{schema}.bronze_{team}_idesc_cadastre_parcels"
b_cad_neighborhoods = f"{catalog}.{schema}.bronze_{team}_idesc_cadastre_neighborhoods"
b_codes = f"{catalog}.{schema}.bronze_{team}_manual_code_catalog"

s_communes = f"{catalog}.{schema}.silver_{team}_communes"
s_neighborhoods = f"{catalog}.{schema}.silver_{team}_neighborhoods"
s_cad_neighborhoods = f"{catalog}.{schema}.silver_{team}_cadastre_neighborhoods"
s_parcels = f"{catalog}.{schema}.silver_{team}_cadastre_parcels"
s_codes = f"{catalog}.{schema}.silver_{team}_code_catalog"

def clean(col):
    """trim; vacío o solo espacios -> null"""
    c = F.trim(F.col(col))
    return F.when(c == "", None).otherwise(c)

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

def save(df, table, key):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(table))
    apply_comments(table, DOCS[key]["table"], DOCS[key]["columns"])
    print(f"Guardada {table}: {spark.table(table).count()} filas")

def wkt_from_geojson(txt):
    if txt is None:
        return None
    g = json.loads(txt)
    return shape(g).wkt if g else None

def to_spark(pdf, cols):
    """pandas (solo strings) -> Spark con NaN como null"""
    flat = pdf[cols].astype(object)
    flat = flat.where(flat.notna(), None)
    schema_ = StructType([StructField(c, StringType()) for c in cols])
    return spark.createDataFrame([tuple(r) for r in flat.itertuples(index=False, name=None)], schema=schema_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios de tabla y columna
# MAGIC Se aplican después de guardar cada tabla. Formato: descripción, unidad y `Origen:` (el diccionario `docs/data_dictionary.md` se mantiene idéntico a estos textos).

# COMMAND ----------

DOCS = {
    "communes": {
        "table": "Las 22 comunas de Cali según la DIVIPOLA 2026 (Acuerdo 0636), una fila por comuna, con geometría. Fuente: IDESC.",
        "columns": {
            "commune_code": "Código de la comuna, 2 dígitos con cero a la izquierda (ej. '03'). Origen: IDESC dapm:pdt_dpa_comunas, campo comcodigo.",
            "commune_name": "Nombre oficial de la comuna (ej. 'Comuna 03'). Origen: IDESC, campo comnombre.",
            "agreement_url": "Enlace al acuerdo municipal que define la comuna (Acuerdo 0636 de 2026). Origen: IDESC, campo comacuerdo.",
            "geom_wkt": "Polígono de la comuna en WKT, EPSG:4326 (lon, lat). Origen: geometría de IDESC.",
        },
    },
    "neighborhoods": {
        "table": "Los 342 barrios y sectores oficiales de Cali (DIVIPOLA 2026, Acuerdo 0636), una fila por barrio, con su comuna y geometría. Fuente: IDESC.",
        "columns": {
            "neighborhood_code": "Código oficial del barrio, 4 dígitos 'CCBB' (comuna + barrio, sin guion, ej. '1931'). Origen: IDESC dapm:pdt_dpa_barrios_sectores, campo barcodigo.",
            "neighborhood_name": "Nombre oficial del barrio o sector. Origen: IDESC, campo barnombre.",
            "neighborhood_category": "Categoría del polígono (ej. 'Barrio', 'Sector'). Origen: IDESC, campo barcategor.",
            "commune_code": "Código de la comuna a la que pertenece el barrio, 2 dígitos. Origen: primeros 2 dígitos de neighborhood_code.",
            "commune_name": "Nombre oficial de la comuna. Origen: silver_el_equipo_communes por commune_code.",
            "agreement_url": "Enlace al acuerdo municipal que define el barrio (Acuerdo 0636 de 2026). Origen: IDESC, campo baracusan.",
            "geom_wkt": "Polígono del barrio en WKT, EPSG:4326 (lon, lat). Origen: geometría de IDESC.",
        },
    },
    "cadastre_neighborhoods": {
        "table": "Nombre que el catastro da a cada código de barrio 'CCBB' presente en el dataset de edificios (52 códigos). Auxiliar: sirve para los códigos que la DIVIPOLA 2026 cambió u omitió. Fuente: IDESC catastro.",
        "columns": {
            "cadastral_neighborhood_code": "Código catastral del barrio, 4 dígitos 'CCBB' (comuna + barrio, sin guion). Origen: IDESC catastro:cat_bas_terrenos, campos comuna y barrio.",
            "cadastral_neighborhood_name": "Nombre del barrio según el catastro. Origen: IDESC catastro, campo nom_barrio.",
        },
    },
    "cadastre_parcels": {
        "table": "Registros del catastro de Cali (terrenos) que corresponden a los edificios del dataset, buscados por id_predio y por conexion. Puede haber varios registros por parcel_id o connection_code (varias unidades por terreno). Fuente: IDESC catastro.",
        "columns": {
            "parcel_id": "Identificador del predio en el catastro. Origen: IDESC catastro:cat_bas_terrenos, campo id_predio.",
            "national_parcel_number": "Número Predial Nacional (30 dígitos). Origen: IDESC catastro, campo npn.",
            "connection_code": "Código de conexión del terreno: comuna + barrio + manzana + predio (12 dígitos). Origen: IDESC catastro, campo conexion.",
            "land_use": "Uso principal del predio (ej. 'RESIDENCIAL'). Origen: IDESC catastro, campo uso_princi.",
            "economic_destination_code": "Código de destinación económica del predio (ej. 'A'). Origen: IDESC catastro, campo destinacio.",
            "cadastre_floor_count": "Número de pisos según el catastro. Origen: IDESC catastro, campo total_pis1.",
            "cadastre_built_area_m2": "Área construida según el catastro, en m². Origen: IDESC catastro, campo area_cons1.",
            "address": "Dirección del predio según el catastro. Origen: IDESC catastro, campo direpred.",
            "cadastre_edited_date": "Fecha de última edición del registro catastral. Origen: IDESC catastro, campo last_edite (primeros 10 caracteres).",
            "construction_count": "Número de construcciones registradas en el predio. Origen: IDESC catastro, campo n_construc.",
        },
    },
    "code_catalog": {
        "table": "Significado de los códigos catastrales del dataset (DEPAPRED, MUNIPRED, TIPO_AVALU, SECTOR, COND) con su fuente y estado de verificación. Los significados no confirmados quedan vacíos. Fuente: curada a mano (reference/codigos_catastrales.csv).",
        "columns": {
            "domain": "Campo del dataset al que pertenece el código (ej. 'TIPO_AVALU'). Origen: reference/codigos_catastrales.csv, columna dominio.",
            "code": "Valor del código tal como aparece en el dataset (ej. '01'). Origen: CSV, columna codigo.",
            "meaning": "Significado del código; nulo si no está confirmado en una fuente. Origen: CSV, columna significado.",
            "source": "Nombre de la fuente que respalda el significado. Origen: CSV, columna fuente.",
            "source_url": "Enlace a la fuente que respalda el significado. Origen: CSV, columna enlace.",
            "verification_status": "Estado de verificación: 'verified' o 'pending_verification'. Origen: CSV, columna estado traducida.",
        },
    },
}

# COMMAND ----------

# Comunas
pc = spark.table(b_communes).toPandas()
pc["geom_wkt"] = pc["geometry"].map(wkt_from_geojson)
pc = pc.rename(columns={"comcodigo": "commune_code", "comnombre": "commune_name", "comacuerdo": "agreement_url"})
pc["commune_code"] = pc["commune_code"].str.strip()
pc["commune_name"] = pc["commune_name"].str.strip()
assert pc["commune_code"].is_unique and (pc["commune_code"].str.len() == 2).all()
save(to_spark(pc, ["commune_code", "commune_name", "agreement_url", "geom_wkt"]), s_communes, "communes")

# COMMAND ----------

# Barrios oficiales DIVIPOLA 2026 (con su comuna)
pn = spark.table(b_neighborhoods).toPandas()
pn["neighborhood_code"] = pn["barcodigo"].str.strip()
pn["neighborhood_name"] = pn["barnombre"].str.strip()
pn["neighborhood_category"] = pn["barcategor"].str.strip()
pn["agreement_url"] = pn["baracusan"].str.strip()
pn["geom_wkt"] = pn["geometry"].map(wkt_from_geojson)
pn["commune_code"] = pn["neighborhood_code"].str[:2]
pn = pn.merge(pc[["commune_code", "commune_name"]], on="commune_code", how="left")
assert pn["neighborhood_code"].is_unique, "neighborhood_code debe ser único"
assert (pn["neighborhood_code"].str.len() == 4).all() and pn["neighborhood_code"].str.isdigit().all()
assert pn["commune_name"].notna().all(), "todo barrio debe tener comuna"
cols_n = ["neighborhood_code", "neighborhood_name", "neighborhood_category", "commune_code",
          "commune_name", "agreement_url", "geom_wkt"]
save(to_spark(pn.sort_values("neighborhood_code"), cols_n), s_neighborhoods, "neighborhoods")

# COMMAND ----------

# Nombres catastrales de barrio (auxiliar)
pcb = spark.table(b_cad_neighborhoods).toPandas()
pcb["cadastral_neighborhood_code"] = pcb["comuna"].str.strip() + pcb["barrio"].str.strip()
pcb["cadastral_neighborhood_name"] = pcb["nom_barrio"].str.strip()
pcb = pcb.drop_duplicates("cadastral_neighborhood_code").sort_values("cadastral_neighborhood_code")
assert (pcb["cadastral_neighborhood_code"].str.len() == 4).all()
save(to_spark(pcb, ["cadastral_neighborhood_code", "cadastral_neighborhood_name"]),
     s_cad_neighborhoods, "cadastre_neighborhoods")

# COMMAND ----------

# Catastro de predios (varios registros por connection_code posibles: no se deduplica aquí)
t = spark.table(b_parcels)
parcels = t.select(
    F.col("id_predio").cast("long").alias("parcel_id"),
    clean("npn").alias("national_parcel_number"),
    clean("conexion").alias("connection_code"),
    clean("uso_princi").alias("land_use"),
    clean("destinacio").alias("economic_destination_code"),
    F.col("total_pis1").cast("int").alias("cadastre_floor_count"),
    F.col("area_cons1").cast("double").alias("cadastre_built_area_m2"),
    clean("direpred").alias("address"),
    # last_edite llega como '2025-09-02Z': se toman los 10 primeros caracteres
    F.to_date(F.substring(F.trim(F.col("last_edite")), 1, 10)).alias("cadastre_edited_date"),
    F.col("n_construc").cast("int").alias("construction_count"),
)
save(parcels, s_parcels, "cadastre_parcels")

# COMMAND ----------

# Catálogo de códigos (columnas del CSV en español -> inglés; estado -> categoría en inglés)
STATUS = {"verificado": "verified", "por verificar": "pending_verification"}
codes = spark.table(b_codes).select(
    clean("dominio").alias("domain"), clean("codigo").alias("code"),
    clean("significado").alias("meaning"), clean("fuente").alias("source"),
    clean("enlace").alias("source_url"),
    F.coalesce(F.create_map(*[x for kv in STATUS.items() for x in map(F.lit, kv)])[clean("estado")],
               clean("estado")).alias("verification_status"))
save(codes, s_codes, "code_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría
# MAGIC Se espera: 22 comunas, 342 barrios oficiales con `neighborhood_code` único de 4 dígitos, 52 códigos catastrales de barrio, y en el catastro `parcel_id` y `connection_code`
# MAGIC **repetidos** (varias unidades por terreno), que se resuelven en `silver_buildings`.

# COMMAND ----------

com = spark.table(s_communes); nei = spark.table(s_neighborhoods); par = spark.table(s_parcels)
assert com.count() == 22 and nei.count() == 342
print(f"Comunas: {com.count()} | barrios oficiales: {nei.count()} | "
      f"códigos catastrales de barrio: {spark.table(s_cad_neighborhoods).count()}")
print(f"Catastro predios: {par.count()} filas | parcel_id distintos: {par.select('parcel_id').distinct().count()} "
      f"| connection_code distintos: {par.select('connection_code').distinct().count()} "
      f"| national_parcel_number distintos: {par.select('national_parcel_number').distinct().count()}")
print("connection_code con más de un registro:",
      par.groupBy("connection_code").count().filter("count > 1").count())
display(spark.table(s_codes))
