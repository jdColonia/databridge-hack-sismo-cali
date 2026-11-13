# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: cifras de contexto del evento (sismo del 10 de agosto)
# MAGIC Una tabla en formato largo, **una fila por cifra**, lista para citar en las respuestas: `gold_{team}_event_context`.
# MAGIC
# MAGIC **Qué reúne:**
# MAGIC - Cifras oficiales de impacto de la Alcaldía de Cali (fallecidos, lesionados, rescatados, desaparecidos y colapsos), tal como están en `bronze_{team}_manual_official_impact`, con su fuente, fecha de corte y estado de verificación.
# MAGIC - Datos del sismo principal y de la secuencia de réplicas del USGS (`us6000tjl2`): magnitud, profundidad, número de réplicas M ≥ 3,5 y mayor réplica.
# MAGIC - Probabilidad oficial del USGS de al menos una réplica M ≥ 5 en cada ventana de tiempo (1 día, 1 semana, 1 mes, 1 año).
# MAGIC
# MAGIC **Decisiones:**
# MAGIC - Formato largo (`metric_code`, `value`, `unit`, `time_window`): agregar una cifra es agregar una fila, no una columna.
# MAGIC - `verification_status` pasa a `verified` / `pending_verification`: **las respuestas usan solo filas `verified`**. Las `pending_verification` (ej. 308 edificaciones colapsadas atribuidas a UNGRD) se conservan para no perderlas, pero no se citan.
# MAGIC - La probabilidad del USGS (0 a 1) se publica en porcentaje con 2 decimales.
# MAGIC - Cada fila lleva su fuente, enlace y fecha de corte: nada se cifra sin trazabilidad.
# MAGIC - Las cifras de la secuencia se calculan sobre la tabla bronze (`_queried_at` es la fecha de corte); la M5,4 del 3 nov (Rioblanco) no está porque queda fuera del círculo oficial de la secuencia.

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

events_table = f"{catalog}.{schema}.bronze_{team}_usgs_earthquakes"
forecast_table = f"{catalog}.{schema}.bronze_{team}_usgs_aftershock_forecast"
impact_table = f"{catalog}.{schema}.bronze_{team}_manual_official_impact"
gold_context = f"{catalog}.{schema}.gold_{team}_event_context"

missing_tables = [t for t in (events_table, forecast_table, impact_table) if not spark.catalog.tableExists(t)]
assert not missing_tables, f"Faltan tablas {missing_tables}: corre antes bronze_usgs_context."

MAINSHOCK_ID = "us6000tjl2"
EVENT_URL = f"https://earthquake.usgs.gov/earthquakes/eventpage/{MAINSHOCK_ID}"
SEQUENCE_SOURCE = "USGS – catálogo FDSN (secuencia us6000tjl2)"
FORECAST_SOURCE = "USGS – pronóstico de réplicas (producto oaf)"
WINDOWS = {"1 Day": "1_day", "1 Week": "1_week", "1 Month": "1_month", "1 Year": "1_year"}
STATUS = {"verificada": "verified", "por verificar": "pending_verification"}

def apply_comments(table, table_comment, column_comments):
    """Aplica COMMENT de tabla y de cada columna; exige que el diccionario cubra TODAS las columnas."""
    esc = lambda s: s.replace("'", "''")
    cols = spark.table(table).columns
    assert set(cols) == set(column_comments), (
        f"{table}: sin comentario {set(cols) - set(column_comments)}, sobran {set(column_comments) - set(cols)}")
    spark.sql(f"COMMENT ON TABLE {table} IS '{esc(table_comment)}'")
    for c in cols:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} COMMENT '{esc(column_comments[c])}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comentarios de tabla y columna
# MAGIC Formato: descripción, unidad y `Origen:` (`docs/data_dictionary.md` se mantiene idéntico a estos textos).

# COMMAND ----------

DOCS = {
    "event_context": {
        "table": "Cifras de contexto del evento listas para citar (Alcaldía y USGS), en formato largo: una fila por cifra, con unidad, ventana de tiempo, fuente, enlace, fecha de corte y estado de verificación. Las respuestas usan solo filas verified.",
        "columns": {
            "metric_code": "Código de la cifra (ej. deaths, mainshock_magnitude, aftershock_probability_m5). Con time_window forma la llave. Origen: bronze_el_equipo_manual_official_impact y derivado de bronze_el_equipo_usgs_earthquakes y bronze_el_equipo_usgs_aftershock_forecast.",
            "metric_name": "Nombre de la cifra en español. Origen: bronze_el_equipo_manual_official_impact o definido en el notebook.",
            "value": "Valor de la cifra, en la unidad de la columna unit. Origen: bronze_el_equipo_manual_official_impact; magnitud, profundidad, conteo y máximo de bronze_el_equipo_usgs_earthquakes; probabilidad de bronze_el_equipo_usgs_aftershock_forecast (probability × 100, 2 decimales).",
            "unit": "Unidad del valor (ej. personas, edificaciones, Mw, km, sismos, %). Origen: bronze_el_equipo_manual_official_impact o definido en el notebook.",
            "time_window": "Ventana de tiempo del pronóstico: 1_day, 1_week, 1_month o 1_year; nulo si la cifra no es un pronóstico. Origen: window_label de bronze_el_equipo_usgs_aftershock_forecast.",
            "source_name": "Nombre de la fuente de la cifra. Origen: bronze_el_equipo_manual_official_impact o definido en el notebook.",
            "source_url": "Enlace de la fuente; nulo si la cifra no tiene fuente con enlace. Origen: bronze_el_equipo_manual_official_impact o definido en el notebook.",
            "cutoff_date": "Fecha de corte de la cifra (fecha del dato, no de la descarga, salvo la secuencia de réplicas, que usa la fecha de consulta). Origen: cutoff_date de bronze_el_equipo_manual_official_impact; event_time, _queried_at o forecast_created_at de las tablas del USGS.",
            "verification_status": "Estado de verificación: verified o pending_verification. Las respuestas usan solo verified. Origen: verification_status de bronze_el_equipo_manual_official_impact (verificada, por verificar); las cifras del USGS son verified.",
        },
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Armado de las cifras
# MAGIC Los tres insumos son pequeños (decenas de filas): las cifras del USGS se calculan en Spark, se traen al driver y se unen con las oficiales en un DataFrame con esquema explícito.

# COMMAND ----------

ev = spark.table(events_table)
fc = spark.table(forecast_table)
imp = spark.table(impact_table)

schema_out = StructType([
    StructField("metric_code", StringType()),
    StructField("metric_name", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("time_window", StringType()),
    StructField("source_name", StringType()),
    StructField("source_url", StringType()),
    StructField("cutoff_date", DateType()),
    StructField("verification_status", StringType()),
])

rows = []

# 1) Cifras oficiales de impacto (todas las filas; se traduce el estado de verificación)
for r in imp.collect():
    assert r["verification_status"] in STATUS, f"estado de verificación desconocido: {r['verification_status']}"
    rows.append((
        r["metric_code"], r["metric_name"], float(r["value"]), r["unit"], None,
        r["source_name"], r["source_url"] or None,
        date.fromisoformat(r["cutoff_date"]) if r["cutoff_date"] else None,
        STATUS[r["verification_status"]],
    ))

# 2) Sismo principal
main = ev.filter(f"is_mainshock AND event_id = '{MAINSHOCK_ID}'").collect()
assert len(main) == 1, f"se esperaba 1 sismo principal, hay {len(main)}"
m = main[0]
main_date = m["event_time"].date()
rows.append(("mainshock_magnitude", "Magnitud del sismo principal", float(m["magnitude"]), "Mw", None,
             SEQUENCE_SOURCE, EVENT_URL, main_date, "verified"))
rows.append(("mainshock_depth_km", "Profundidad del sismo principal", float(m["depth_km"]), "km", None,
             SEQUENCE_SOURCE, EVENT_URL, main_date, "verified"))

# 3) Secuencia de réplicas (cifras al momento de la consulta al USGS)
after = ev.filter("NOT is_mainshock")
stats = after.agg(F.count("*").alias("n"), F.max("magnitude").alias("max_mag"),
                  F.max("_queried_at").alias("queried_at")).first()
queried_date = stats["queried_at"].date()
rows.append(("aftershock_count_m35", "Réplicas de magnitud 3,5 o mayor", float(stats["n"]), "sismos", None,
             SEQUENCE_SOURCE, EVENT_URL, queried_date, "verified"))
rows.append(("max_aftershock_magnitude", "Magnitud de la mayor réplica", float(stats["max_mag"]), "Mw", None,
             SEQUENCE_SOURCE, EVENT_URL, queried_date, "verified"))

# 4) Probabilidad de al menos una réplica M >= 5 por ventana de tiempo
for r in fc.filter("min_magnitude = 5.0").collect():
    assert r["window_label"] in WINDOWS, f"ventana desconocida: {r['window_label']}"
    rows.append(("aftershock_probability_m5", "Probabilidad de al menos una réplica de magnitud 5 o mayor",
                 round(float(r["probability"]) * 100, 2), "%", WINDOWS[r["window_label"]],
                 FORECAST_SOURCE, EVENT_URL, r["forecast_created_at"].date(), "verified"))

context_df = spark.createDataFrame(rows, schema=schema_out).orderBy("metric_code", "time_window")
context_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_context)
apply_comments(gold_context, DOCS["event_context"]["table"], DOCS["event_context"]["columns"])
print(f"Guardada {gold_context}: {spark.table(gold_context).count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación
# MAGIC - Sin llaves repetidas (`metric_code`, `time_window`).
# MAGIC - Cifras oficiales `verified` presentes para fallecidos, lesionados, rescatados, desaparecidos y colapsos totales.
# MAGIC - 60 réplicas M ≥ 3,5 al 13 nov (si el catálogo creció, se acepta `>= 60` con aviso).
# MAGIC - P(M ≥ 5) en 1 mes ≈ 31,5 % (tolerancia 0,05).

# COMMAND ----------

ctx = spark.table(gold_context)

dups = ctx.groupBy("metric_code", "time_window").count().filter("count > 1").count()
assert dups == 0, f"{dups} combinaciones (metric_code, time_window) repetidas"

verified_codes = {r[0] for r in ctx.filter("verification_status = 'verified'").select("metric_code").collect()}
required = {"deaths", "injured", "rescued", "missing", "total_collapses"}
assert required <= verified_codes, f"faltan cifras oficiales verified: {required - verified_codes}"

n_after = ctx.filter("metric_code = 'aftershock_count_m35'").first()["value"]
assert n_after >= 60, f"réplicas M>=3,5: {n_after} (se esperaban al menos 60)"
if n_after != 60:
    print(f"AVISO: el catálogo tiene {n_after:.0f} réplicas (se esperaban 60); probablemente hay réplicas nuevas.")

p_month = ctx.filter("metric_code = 'aftershock_probability_m5' AND time_window = '1_month'").first()["value"]
assert abs(p_month - 31.5) < 0.05, f"P(M>=5) a 1 mes es {p_month}, no ~31,5: revisa el pronóstico del USGS (¿se actualizó?)."

assert ctx.filter("value IS NULL OR source_name IS NULL OR verification_status IS NULL").count() == 0, \
    "hay cifras sin valor, fuente o estado"
assert ctx.filter("verification_status = 'verified' AND (cutoff_date IS NULL OR source_url IS NULL)").count() == 0, \
    "hay cifras verificadas sin fecha de corte o URL de fuente"
print(f"Réplicas M>=3,5: {n_after:.0f} | P(M>=5) a 1 mes: {p_month}%")

display(ctx.orderBy("metric_code", "time_window"))
