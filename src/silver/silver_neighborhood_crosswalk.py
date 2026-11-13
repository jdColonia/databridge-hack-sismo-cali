# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: crosswalk entre barrios catastrales y barrios oficiales
# MAGIC Traduce el código de barrio **catastral** del dataset (`cadastral_neighborhood_code`) al barrio **oficial** DIVIPOLA 2026 (`neighborhood_code`) por ubicación.
# MAGIC
# MAGIC | Tabla | Grano | Para qué |
# MAGIC | ----- | ----- | -------- |
# MAGIC | `silver_{team}_neighborhood_crosswalk` | Un par código catastral → barrio oficial | Muestra qué códigos cambió el Acuerdo 0636 de 2026 y cuántos edificios afecta |
# MAGIC
# MAGIC **Decisión:** el barrio oficial se asigna por **ubicación** (centroide del edificio dentro del polígono DIVIPOLA 2026), no por código,
# MAGIC porque el Acuerdo recodificó algunos barrios: el código catastral no siempre existe en la DIVIPOLA, pero el barrio sí (ej. catastral `1994` "Pampalinda" → oficial "Pampa Linda").
# MAGIC La tabla deja esa traducción a la vista. Los edificios sin código catastral (comuna o barrio vacíos en el origen) aparecen con `cadastral_neighborhood_code` nulo.
# MAGIC El barrio oficial (342 barrios, con comuna y geometría) está en `silver_el_equipo_neighborhoods`.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")

buildings_t = f"{catalog}.{schema}.silver_{team}_buildings"
crosswalk_t = f"{catalog}.{schema}.silver_{team}_neighborhood_crosswalk"

assert spark.catalog.tableExists(buildings_t), f"Falta {buildings_t}: corre antes silver_buildings."

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
    "neighborhood_crosswalk": {
        "table": "Equivalencia entre el código de barrio catastral de los edificios y el barrio oficial DIVIPOLA 2026 por ubicación, con el número de edificios de cada par. Los building_count suman los edificios con barrio oficial.",
        "columns": {
            "cadastral_neighborhood_code": "Código catastral del barrio, 4 dígitos 'CCBB'; nulo en los edificios sin comuna o barrio en el origen. Origen: silver_el_equipo_buildings.",
            "cadastral_neighborhood_name": "Nombre del barrio según el código catastral (DIVIPOLA si existe, si no el catastro). Origen: silver_el_equipo_buildings.",
            "neighborhood_code": "Código oficial del barrio (4 dígitos 'CCBB') donde cae el centroide. Origen: silver_el_equipo_buildings (DIVIPOLA 2026).",
            "neighborhood_name": "Nombre oficial del barrio donde cae el centroide. Origen: silver_el_equipo_buildings (DIVIPOLA 2026).",
            "is_same_code": "Verdadero si el código catastral coincide con el oficial; nulo si no hay código catastral. Origen: cadastral_neighborhood_code = neighborhood_code.",
            "building_count": "Número de edificios del dataset con ese par de códigos. Origen: conteo sobre silver_el_equipo_buildings.",
        },
    },
}

# COMMAND ----------

buildings = spark.table(buildings_t)
crosswalk = (buildings.filter("neighborhood_code IS NOT NULL")
             .groupBy("cadastral_neighborhood_code", "cadastral_neighborhood_name",
                      "neighborhood_code", "neighborhood_name")
             .agg(F.count("*").cast("long").alias("building_count"))
             .withColumn("is_same_code", F.col("cadastral_neighborhood_code") == F.col("neighborhood_code"))
             .select("cadastral_neighborhood_code", "cadastral_neighborhood_name",
                     "neighborhood_code", "neighborhood_name", "is_same_code", "building_count")
             .orderBy("cadastral_neighborhood_code", F.desc("building_count")))
(crosswalk.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(crosswalk_t))
apply_comments(crosswalk_t, DOCS["neighborhood_crosswalk"]["table"], DOCS["neighborhood_crosswalk"]["columns"])
print(f"Guardada {crosswalk_t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría
# MAGIC Los `building_count` suman 5.146 menos los edificios sin barrio oficial; y la lista de códigos catastrales distintos del oficial.

# COMMAND ----------

x = spark.table(crosswalk_t)
total = buildings.count()
without_neighborhood = buildings.filter("neighborhood_code IS NULL").count()
counted = x.agg(F.sum("building_count")).collect()[0][0]
print(f"Edificios contados en el crosswalk: {counted} | sin barrio oficial: {without_neighborhood} | total: {total}")
assert counted + without_neighborhood == total, "todo edificio debe quedar contado o marcado sin barrio"
print("Códigos catastrales distintos al oficial:")
display(x.filter("is_same_code = false").orderBy(F.desc("building_count")))
