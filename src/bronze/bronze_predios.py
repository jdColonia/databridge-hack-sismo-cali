# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: predios tal cual llegan
# MAGIC **Decisión:** bronze guarda el dato *sin limpiar*: sin `trim`, sin convertir vacíos a nulos, sin corregir tipos raros.
# MAGIC Una fila por feature del GeoJSON, una columna por propiedad (20) más `geometry` como texto GeoJSON = **21 columnas**.
# MAGIC
# MAGIC Se lee con `json` de Python porque el archivo pesa ~5 MB; no hace falta un lector distribuido.
# MAGIC Se define esquema explícito para que el resultado no dependa de la inferencia (códigos como string, conteos como long, medidas como double).

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

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

# El crudo vive en el volumen compartido (se sube una vez); la tabla va al esquema de trabajo
src_path = f"/Volumes/{catalog}/{raw_schema}/{volume}/Datosprediales.json"
bronze_table = f"{catalog}.{schema}.bronze_{team}_predios"

# COMMAND ----------

# Esquema explícito: 20 propiedades + geometry (texto GeoJSON)
schema_bronze = StructType([
    StructField("OBJECTID", LongType()),
    StructField("OBJECTID_1", LongType()),
    StructField("DEPAPRED", StringType()),
    StructField("MUNIPRED", StringType()),
    StructField("TIPO_AVALU", StringType()),
    StructField("SECTOR", StringType()),
    StructField("COMUNA", StringType()),
    StructField("BARRIO", StringType()),
    StructField("MANZANA", StringType()),
    StructField("TERRENO", StringType()),
    StructField("COND", StringType()),
    StructField("EDIFPRED", StringType()),
    StructField("PREDIO", StringType()),
    StructField("NUMEPRED", StringType()),
    StructField("NPISOS", LongType()),
    StructField("ID_PREDIO", StringType()),
    StructField("Indice_Escombros", DoubleType()),
    StructField("Estado_Estructural", StringType()),
    StructField("Shape__Area", DoubleType()),
    StructField("Shape__Length", DoubleType()),
    StructField("geometry", StringType()),
])
prop_cols = [f.name for f in schema_bronze.fields if f.name != "geometry"]

with open(src_path, "r", encoding="utf-8") as fh:
    gj = json.load(fh)

def to_row(feature):
    props = feature.get("properties", {})
    row = [props.get(c) for c in prop_cols]
    # ID_PREDIO llega como número en el JSON; se guarda como string (es un código, no una medida)
    i = prop_cols.index("ID_PREDIO")
    if row[i] is not None:
        row[i] = str(row[i])
    row.append(json.dumps(feature.get("geometry")))
    return tuple(row)

rows = [to_row(f) for f in gj["features"]]
df = spark.createDataFrame(rows, schema=schema_bronze)

# COMMAND ----------

(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(bronze_table))
print(f"Guardada {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación
# MAGIC Esperamos 5.146 features y 21 columnas. Conteo por estado estructural tal como llega.

# COMMAND ----------

bronze = spark.table(bronze_table)
assert bronze.count() == 5146, bronze.count()
assert len(bronze.columns) == 21, len(bronze.columns)
display(bronze.groupBy("Estado_Estructural").count().orderBy("count", ascending=False))
