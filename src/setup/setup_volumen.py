# Databricks notebook source
# MAGIC %md
# MAGIC # Setup del esquema y los volúmenes
# MAGIC Crea el esquema de trabajo y los volúmenes de Unity Catalog:
# MAGIC - **Volumen compartido de crudos** (`{catalog}.{raw_schema}.{volume}`): `Datosprediales.json` se sube una sola vez y todo el equipo lo lee.
# MAGIC - **Volumen del esquema de trabajo** (`{catalog}.{schema}.{volume}`): datos externos (`externos/`) y exportes (`export/`).
# MAGIC
# MAGIC **Decisión:** todos trabajamos en el mismo workspace. En el target `dev` cada integrante escribe en su propio esquema
# MAGIC (`dev_<usuario>`) para no pisar las tablas de los demás; el target `team` escribe en `default`, donde quedan las 3 capas finales.
# MAGIC Usamos *volúmenes* (no DBFS root) porque Free Edition solo trabaja con serverless + Unity Catalog.

# COMMAND ----------

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

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw_schema}.{volume}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
volume_path = f"/Volumes/{catalog}/{raw_schema}/{volume}/"
print(f"Esquema de trabajo: {catalog}.{schema}")
print(f"Volumen de crudos (compartido): {volume_path}")
print(f"Volumen de trabajo: /Volumes/{catalog}/{schema}/{volume}/")

# COMMAND ----------

# MAGIC %md
# MAGIC Verificamos si el archivo fuente ya fue subido (`bash scripts/upload_data.sh`).

# COMMAND ----------

try:
    files = [f.name for f in dbutils.fs.ls(volume_path)]
except Exception:
    files = []

if "Datosprediales.json" in files:
    print("OK: Datosprediales.json ya está en el volumen.")
else:
    print(
        "AVISO: Datosprediales.json aún NO está en el volumen.\n"
        "Súbelo con `bash scripts/upload_data.sh` (o desde Catalog > Volumes > Upload) "
        "antes de correr el notebook bronze/bronze_predios."
    )
