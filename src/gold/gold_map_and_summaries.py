# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: mapa de edificios y resúmenes por comuna y barrio
# MAGIC Tablas listas para consumir (mapa, tableros):
# MAGIC
# MAGIC - `gold_{team}_map_buildings`: una fila por edificio con color y geometría WKT. **Incluye los polígonos dudosos** (con su flag `is_suspect_polygon`) para que quien consuma decida si filtrarlos.
# MAGIC - `gold_{team}_commune_summary` y `gold_{team}_neighborhood_summary`: conteos, área construida y porcentajes de daño por comuna y por barrio **oficiales** (DIVIPOLA 2026, por ubicación).
# MAGIC
# MAGIC **Por qué el denominador excluye `no_data`:** los 435 edificios sin datos satelitales no se pueden clasificar; contarlos como "sin daño" bajaría artificialmente el porcentaje, y contarlos como "dañados" lo inflaría.
# MAGIC Por eso `damaged_building_pct` y `damaged_area_pct` se calculan solo sobre edificios **con dato**, y la cobertura (`no_data_building_pct`, `no_data_area_pct`) se reporta aparte para que se lea junto al porcentaje.
# MAGIC
# MAGIC **Por qué también se pondera por área:** un edificio grande pesa más en personas y reconstrucción que uno pequeño. `damaged_area_pct` complementa el conteo simple.
# MAGIC "Daño" aquí = `roof_loss` + `debris` (inferencia satelital, no inspección en campo).
# MAGIC
# MAGIC Cada tabla recibe comentarios de tabla y de columna en Databricks (`COMMENT`).

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

silver_table = f"{catalog}.{schema}.silver_{team}_buildings"
dist_table = f"{catalog}.{schema}.silver_{team}_building_facility_distances"
gold_map = f"{catalog}.{schema}.gold_{team}_map_buildings"
gold_commune = f"{catalog}.{schema}.gold_{team}_commune_summary"
gold_neighborhood = f"{catalog}.{schema}.gold_{team}_neighborhood_summary"

def apply_comments(table, table_comment, column_comments):
    """Aplica COMMENT de tabla y de cada columna; exige comentario para TODAS las columnas de la tabla.
    El diccionario puede traer columnas opcionales (distancias) que la tabla no siempre tiene."""
    esc = lambda s: s.replace("'", "''")
    cols = spark.table(table).columns
    missing = set(cols) - set(column_comments)
    assert not missing, f"{table}: columnas sin comentario {missing}"
    spark.sql(f"COMMENT ON TABLE {table} IS '{esc(table_comment)}'")
    for c in cols:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} COMMENT '{esc(column_comments[c])}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios de tabla y columna
# MAGIC Formato: descripción, unidad y `Origen:` (`docs/data_dictionary.md` se mantiene idéntico a estos textos).

# COMMAND ----------

DOCS = {
    "map_buildings": {
        "table": "Vista plana para el mapa: una fila por edificio (5.146) con estado estructural, color, comuna y barrio oficiales, área, distancias a equipamientos (si existen) y geometría WKT. Incluye polígonos dudosos con su bandera.",
        "columns": {
            "building_id": "Identificador único del edificio. Origen: silver_el_equipo_buildings.",
            "commune_code": "Código oficial de la comuna (2 dígitos) según la ubicación. Origen: silver_el_equipo_buildings.",
            "commune_name": "Nombre oficial de la comuna. Origen: silver_el_equipo_buildings.",
            "neighborhood_code": "Código oficial del barrio (4 dígitos 'CCBB') según la ubicación. Origen: silver_el_equipo_buildings.",
            "neighborhood_name": "Nombre oficial del barrio. Origen: silver_el_equipo_buildings.",
            "floor_count": "Número de pisos válido (mayor que 0); nulo si el dato falta. Origen: silver_el_equipo_buildings.",
            "area_m2": "Área de la huella en m² calculada en EPSG:9377. Origen: silver_el_equipo_buildings.",
            "built_area_m2": "Área construida estimada en m² (huella por pisos, 1 piso si falta el dato). Origen: silver_el_equipo_buildings.",
            "structural_status_code": "Estado estructural inferido por satélite: no_change, roof_loss, debris o no_data. Origen: silver_el_equipo_buildings.",
            "structural_status": "Estado estructural en el texto original en español. Origen: silver_el_equipo_buildings.",
            "damage_level": "Nivel de daño ordinal: 0 no_change, 1 roof_loss, 2 debris; nulo si no hay datos. Origen: silver_el_equipo_buildings.",
            "debris_index": "Índice de escombros satelital. Origen: silver_el_equipo_buildings.",
            "color_hex": "Color para el mapa según el estado: azul no_change, naranja roof_loss, rojo debris, gris no_data. Origen: derivado de structural_status_code.",
            "is_suspect_polygon": "Verdadero si el polígono es dudoso (área menor de 5 m², mayor de 50.000 m² o vacío). Origen: silver_el_equipo_buildings.",
            "nearest_hospital_m": "Distancia en m al hospital o clínica más cercano (OpenStreetMap); solo existe si corrió silver_osm_facilities. Origen: silver_el_equipo_building_facility_distances.",
            "nearest_school_m": "Distancia en m al colegio más cercano (OpenStreetMap); solo existe si corrió silver_osm_facilities. Origen: silver_el_equipo_building_facility_distances.",
            "nearest_fire_station_m": "Distancia en m a la estación de bomberos más cercana (OpenStreetMap); solo existe si corrió silver_osm_facilities. Origen: silver_el_equipo_building_facility_distances.",
            "geom_wkt": "Geometría del edificio en WKT, EPSG:4326 (lon, lat). Origen: silver_el_equipo_buildings.",
        },
    },
    "commune_summary": {
        "table": "Resumen del daño por comuna oficial: conteos y área construida por estado estructural, y porcentajes de daño y de cobertura sin datos. Una fila por comuna con edificios del dataset.",
        "columns": {
            "commune_code": "Código oficial de la comuna (2 dígitos). Origen: silver_el_equipo_buildings.",
            "commune_name": "Nombre oficial de la comuna. Origen: silver_el_equipo_buildings.",
            "building_count": "Número de edificios de la comuna. Origen: conteo sobre silver_el_equipo_buildings.",
            "no_change_building_count": "Edificios con estado no_change (sin cambio aparente). Origen: conteo por structural_status_code.",
            "roof_loss_building_count": "Edificios con estado roof_loss (posible pérdida de techo). Origen: conteo por structural_status_code.",
            "debris_building_count": "Edificios con estado debris (alta probabilidad de escombros). Origen: conteo por structural_status_code.",
            "no_data_building_count": "Edificios sin datos satelitales (no_data). Origen: conteo por structural_status_code.",
            "built_area_m2": "Área construida total en m². Origen: suma de built_area_m2 de silver_el_equipo_buildings.",
            "no_change_built_area_m2": "Área construida en m² de los edificios no_change. Origen: suma por structural_status_code.",
            "roof_loss_built_area_m2": "Área construida en m² de los edificios roof_loss. Origen: suma por structural_status_code.",
            "debris_built_area_m2": "Área construida en m² de los edificios debris. Origen: suma por structural_status_code.",
            "no_data_built_area_m2": "Área construida en m² de los edificios no_data. Origen: suma por structural_status_code.",
            "damaged_building_pct": "Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. Origen: derivado de los conteos.",
            "damaged_area_pct": "Porcentaje (0-100) del área construida con daño (roof_loss + debris) sobre la que tiene dato satelital; nulo si ninguna tiene dato. Origen: derivado de las áreas.",
            "no_data_building_pct": "Porcentaje (0-100) de edificios sin datos satelitales sobre el total de la comuna. Origen: derivado de los conteos.",
            "no_data_area_pct": "Porcentaje (0-100) del área construida sin datos satelitales sobre el total de la comuna. Origen: derivado de las áreas.",
        },
    },
    "neighborhood_summary": {
        "table": "Resumen del daño por barrio oficial (DIVIPOLA 2026): conteos y área construida por estado estructural, y porcentajes de daño y de cobertura sin datos. Una fila por barrio con edificios del dataset.",
        "columns": {
            "commune_code": "Código oficial de la comuna del barrio (2 dígitos). Origen: silver_el_equipo_buildings.",
            "commune_name": "Nombre oficial de la comuna del barrio. Origen: silver_el_equipo_buildings.",
            "neighborhood_code": "Código oficial del barrio (4 dígitos 'CCBB'). Origen: silver_el_equipo_buildings.",
            "neighborhood_name": "Nombre oficial del barrio. Origen: silver_el_equipo_buildings.",
            "building_count": "Número de edificios del barrio. Origen: conteo sobre silver_el_equipo_buildings.",
            "no_change_building_count": "Edificios con estado no_change (sin cambio aparente). Origen: conteo por structural_status_code.",
            "roof_loss_building_count": "Edificios con estado roof_loss (posible pérdida de techo). Origen: conteo por structural_status_code.",
            "debris_building_count": "Edificios con estado debris (alta probabilidad de escombros). Origen: conteo por structural_status_code.",
            "no_data_building_count": "Edificios sin datos satelitales (no_data). Origen: conteo por structural_status_code.",
            "built_area_m2": "Área construida total en m². Origen: suma de built_area_m2 de silver_el_equipo_buildings.",
            "no_change_built_area_m2": "Área construida en m² de los edificios no_change. Origen: suma por structural_status_code.",
            "roof_loss_built_area_m2": "Área construida en m² de los edificios roof_loss. Origen: suma por structural_status_code.",
            "debris_built_area_m2": "Área construida en m² de los edificios debris. Origen: suma por structural_status_code.",
            "no_data_built_area_m2": "Área construida en m² de los edificios no_data. Origen: suma por structural_status_code.",
            "damaged_building_pct": "Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. Origen: derivado de los conteos.",
            "damaged_area_pct": "Porcentaje (0-100) del área construida con daño (roof_loss + debris) sobre la que tiene dato satelital; nulo si ninguna tiene dato. Origen: derivado de las áreas.",
            "no_data_building_pct": "Porcentaje (0-100) de edificios sin datos satelitales sobre el total del barrio. Origen: derivado de los conteos.",
            "no_data_area_pct": "Porcentaje (0-100) del área construida sin datos satelitales sobre el total del barrio. Origen: derivado de las áreas.",
        },
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapa de edificios
# MAGIC Paleta (de menor a mayor daño): azul `no_change`, naranja `roof_loss`, rojo `debris`, gris `no_data`.

# COMMAND ----------

s = spark.table(silver_table)

color = (F.when(F.col("structural_status_code") == "no_change", "#2b83ba")
          .when(F.col("structural_status_code") == "roof_loss", "#fdae61")
          .when(F.col("structural_status_code") == "debris", "#d7191c")
          .otherwise("#9e9e9e"))

map_df = s.select(
    "building_id",
    "commune_code", "commune_name", "neighborhood_code", "neighborhood_name",
    "floor_count",
    "area_m2",
    "built_area_m2",
    "structural_status_code",
    "structural_status",
    "damage_level",
    "debris_index",
    color.alias("color_hex"),
    "is_suspect_polygon",
    "geom_wkt",
)

# Distancias a equipamientos: opcional, solo si silver_osm_facilities pudo crear la tabla
if spark.catalog.tableExists(dist_table):
    map_df = map_df.join(spark.table(dist_table), on="building_id", how="left")
    print("Se agregaron distancias a equipamientos")
else:
    print("Tabla de distancias no existe: se omite (opcional)")

# geom_wkt al final
cols = [c for c in map_df.columns if c != "geom_wkt"] + ["geom_wkt"]
map_df = map_df.select(cols)

map_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_map)
apply_comments(gold_map, DOCS["map_buildings"]["table"], DOCS["map_buildings"]["columns"])
print(f"Guardada {gold_map}: {spark.table(gold_map).count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resúmenes por comuna y barrio
# MAGIC Misma función para ambos niveles. Porcentajes: `(roof_loss + debris) / (no_change + roof_loss + debris)`, por conteo y por área construida.

# COMMAND ----------

def summarize(df, keys):
    statuses = ["no_change", "roof_loss", "debris", "no_data"]
    aggs = [F.count("*").alias("building_count")]
    for st in statuses:
        aggs.append(F.sum(F.when(F.col("structural_status_code") == st, 1).otherwise(0)).cast("long")
                     .alias(f"{st}_building_count"))
    aggs.append(F.sum("built_area_m2").alias("built_area_m2"))
    for st in statuses:
        aggs.append(F.sum(F.when(F.col("structural_status_code") == st, F.col("built_area_m2")).otherwise(0.0))
                     .alias(f"{st}_built_area_m2"))
    r = df.groupBy(*keys).agg(*aggs)

    with_data_n = F.col("no_change_building_count") + F.col("roof_loss_building_count") + F.col("debris_building_count")
    with_data_a = F.col("no_change_built_area_m2") + F.col("roof_loss_built_area_m2") + F.col("debris_built_area_m2")
    r = (r
         .withColumn("damaged_building_pct",
                     100 * (F.col("roof_loss_building_count") + F.col("debris_building_count"))
                     / F.when(with_data_n > 0, with_data_n))
         .withColumn("damaged_area_pct",
                     100 * (F.col("roof_loss_built_area_m2") + F.col("debris_built_area_m2"))
                     / F.when(with_data_a > 0, with_data_a))
         .withColumn("no_data_building_pct", 100 * F.col("no_data_building_count") / F.col("building_count"))
         .withColumn("no_data_area_pct",
                     100 * F.col("no_data_built_area_m2") / F.when(F.col("built_area_m2") > 0,
                                                                   F.col("built_area_m2"))))
    return r.orderBy(*keys)

commune_df = summarize(s, ["commune_code", "commune_name"])
commune_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_commune)
apply_comments(gold_commune, DOCS["commune_summary"]["table"], DOCS["commune_summary"]["columns"])

neighborhood_df = summarize(s, ["commune_code", "commune_name", "neighborhood_code", "neighborhood_name"])
neighborhood_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_neighborhood)
apply_comments(gold_neighborhood, DOCS["neighborhood_summary"]["table"], DOCS["neighborhood_summary"]["columns"])

print(f"Guardadas {gold_commune} y {gold_neighborhood}")

# COMMAND ----------

display(spark.table(gold_commune).orderBy(F.col("damaged_area_pct").desc_nulls_last()))
