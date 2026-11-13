# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: mapa de edificios y resúmenes por comuna y barrio
# MAGIC Tablas listas para consumir (mapa, tableros):
# MAGIC
# MAGIC - `gold_{team}_mapa_edificios`: una fila por edificio con color y geometría WKT. **Incluye los polígonos dudosos** (con su flag `poligono_dudoso`) para que quien consuma decida si filtrarlos.
# MAGIC - `gold_{team}_resumen_comuna` y `gold_{team}_resumen_barrio`: conteos, área construida y porcentajes de daño.
# MAGIC
# MAGIC **Por qué el denominador excluye `sin_datos`:** los 435 edificios sin datos satelitales no se pueden clasificar; contarlos como "sin daño" bajaría artificialmente el porcentaje, y contarlos como "dañados" lo inflaría.
# MAGIC Por eso `pct_edificios_dano` y `pct_area_dano` se calculan solo sobre edificios **con dato**, y la cobertura (`pct_area_sin_datos`) se reporta aparte para que se lea junto al porcentaje.
# MAGIC
# MAGIC **Por qué también se pondera por área:** un edificio grande pesa más en personas y reconstrucción que uno pequeño. `pct_area_dano` complementa el conteo simple.
# MAGIC "Daño" aquí = `techo` + `escombros` (inferencia satelital, no inspección en campo).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

silver_table = f"{catalog}.{schema}.silver_{team}_predios"
dist_table = f"{catalog}.{schema}.silver_{team}_predios_distancia_equipamientos"
gold_mapa = f"{catalog}.{schema}.gold_{team}_mapa_edificios"
gold_comuna = f"{catalog}.{schema}.gold_{team}_resumen_comuna"
gold_barrio = f"{catalog}.{schema}.gold_{team}_resumen_barrio"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapa de edificios
# MAGIC Paleta (de menor a mayor daño): azul `sin_cambio`, naranja `techo`, rojo `escombros`, gris `sin_datos`.

# COMMAND ----------

s = spark.table(silver_table)

color = (F.when(F.col("estado_codigo") == "sin_cambio", "#2b83ba")
          .when(F.col("estado_codigo") == "techo", "#fdae61")
          .when(F.col("estado_codigo") == "escombros", "#d7191c")
          .otherwise("#9e9e9e"))

mapa = s.select(
    "OBJECTID",
    F.col("COMUNA").alias("comuna"),
    F.col("BARRIO").alias("barrio"),
    "comuna_barrio_id",
    "npisos_valido",
    "area_m2",
    "area_construida_m2",
    "estado_codigo",
    F.col("Estado_Estructural").alias("estado_estructural"),
    "nivel_dano",
    F.col("Indice_Escombros").alias("indice_escombros"),
    color.alias("color_hex"),
    "poligono_dudoso",
    "geometry_wkt",
)

# Distancias a equipamientos: opcional, solo si el notebook 03 pudo crear la tabla
if spark.catalog.tableExists(dist_table):
    d = spark.table(dist_table)
    mapa = mapa.join(d, on="OBJECTID", how="left")
    print("Se agregaron distancias a equipamientos")
else:
    print("Tabla de distancias no existe: se omite (opcional)")

# geometry_wkt al final
cols = [c for c in mapa.columns if c != "geometry_wkt"] + ["geometry_wkt"]
mapa = mapa.select(cols)

mapa.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_mapa)
print(f"Guardada {gold_mapa}: {spark.table(gold_mapa).count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resúmenes por comuna y barrio
# MAGIC Misma función para ambos niveles. Porcentajes: `(techo + escombros) / (sin_cambio + techo + escombros)`, por conteo y por área construida.

# COMMAND ----------

def resumen(df, key):
    estados = ["sin_cambio", "techo", "escombros", "sin_datos"]
    aggs = [F.count("*").alias("n_edificios")]
    for e in estados:
        aggs.append(F.sum(F.when(F.col("estado_codigo") == e, 1).otherwise(0)).alias(f"n_{e}"))
    aggs.append(F.sum("area_construida_m2").alias("area_construida_total_m2"))
    for e in estados:
        aggs.append(F.sum(F.when(F.col("estado_codigo") == e, F.col("area_construida_m2")).otherwise(0.0))
                     .alias(f"area_construida_{e}_m2"))
    r = df.groupBy(key).agg(*aggs)

    con_dato_n = F.col("n_sin_cambio") + F.col("n_techo") + F.col("n_escombros")
    con_dato_a = (F.col("area_construida_sin_cambio_m2") + F.col("area_construida_techo_m2")
                  + F.col("area_construida_escombros_m2"))
    r = (r
         .withColumn("pct_edificios_dano",
                     100 * (F.col("n_techo") + F.col("n_escombros")) / F.when(con_dato_n > 0, con_dato_n))
         .withColumn("pct_area_dano",
                     100 * (F.col("area_construida_techo_m2") + F.col("area_construida_escombros_m2"))
                     / F.when(con_dato_a > 0, con_dato_a))
         .withColumn("pct_edificios_sin_datos", 100 * F.col("n_sin_datos") / F.col("n_edificios"))
         .withColumn("pct_area_sin_datos",
                     100 * F.col("area_construida_sin_datos_m2") / F.when(F.col("area_construida_total_m2") > 0,
                                                                          F.col("area_construida_total_m2"))))
    return r.orderBy(key)

res_comuna = resumen(s.withColumnRenamed("COMUNA", "comuna"), "comuna")
res_comuna.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_comuna)

res_barrio = resumen(s, "comuna_barrio_id")
res_barrio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_barrio)

print(f"Guardadas {gold_comuna} y {gold_barrio}")

# COMMAND ----------

display(spark.table(gold_comuna).orderBy(F.col("pct_area_dano").desc_nulls_last()))
