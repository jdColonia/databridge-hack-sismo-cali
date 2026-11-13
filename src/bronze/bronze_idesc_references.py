# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: tablas de referencia oficiales (IDESC, Alcaldía de Cali)
# MAGIC **Decisión:** el dataset de edificios trae *códigos* (comuna, barrio, id de predio...) sin significado. Para traducirlos se usan tres capas oficiales del IDESC y una tabla de códigos catastrales:
# MAGIC
# MAGIC | Tabla bronze | Fuente | Contenido |
# MAGIC | --- | --- | --- |
# MAGIC | `idesc_communes` | `dapm:pdt_dpa_comunas` | 22 comunas con nombre y acuerdo |
# MAGIC | `idesc_neighborhoods` | `dapm:pdt_dpa_barrios_sectores` | 342 barrios/sectores (Acuerdo 0636 de 2026, DIVIPOLA), con `geometry` |
# MAGIC | `idesc_cadastre_parcels` | `catastro:cat_bas_terrenos` | Solo los registros de los predios de los edificios (por `id_predio` y por `conexion`), sin geometría |
# MAGIC | `idesc_cadastre_neighborhoods` | `catastro:cat_bas_terrenos` | Nombre catastral de cada código de comuna + barrio del dataset |
# MAGIC | `manual_code_catalog` | `reference/codigos_catastrales.csv` (repo) | DEPAPRED, MUNIPRED, TIPO_AVALU, SECTOR, COND con fuente y estado de verificación (curada a mano, no es del IDESC) |
# MAGIC
# MAGIC Las capas se descargaron **localmente** con `scripts/download_referencias.py` (la salida a internet desde serverless no está comprobada) y se subieron al volumen con `scripts/upload_referencias.sh`.
# MAGIC Bronze guarda las propiedades **tal cual** (con sus nombres originales, sin trim ni cambios de tipo, salvo `id_predio` como long) y agrega los metadatos de ingesta `_source_url` y `_queried_at` tomados del `manifest.json`.

# COMMAND ----------

import json
import os
import pandas as pd
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

ref_dir = f"/Volumes/{catalog}/{raw_schema}/{volume}/externos/referencias/"
t_communes = f"{catalog}.{schema}.bronze_{team}_idesc_communes"
t_neighborhoods = f"{catalog}.{schema}.bronze_{team}_idesc_neighborhoods"
t_parcels = f"{catalog}.{schema}.bronze_{team}_idesc_cadastre_parcels"
t_cad_neighborhoods = f"{catalog}.{schema}.bronze_{team}_idesc_cadastre_neighborhoods"
t_codes = f"{catalog}.{schema}.bronze_{team}_manual_code_catalog"

FILES = ["comunas.geojson", "barrios_divipola.geojson", "catastro_terrenos_predios.json",
         "catastro_barrios.json", "manifest.json"]
missing = [f for f in FILES if not os.path.exists(ref_dir + f)]
if missing:
    msg = (f"Faltan archivos en {ref_dir}: {missing}. Descárgalos con "
           "`uv run --no-project --with requests python -I scripts/download_referencias.py` "
           "y súbelos con `bash scripts/upload_referencias.sh`.")
    print(msg)
    dbutils.notebook.exit(msg)

def load(name):
    with open(ref_dir + name, "r", encoding="utf-8") as fh:
        return json.load(fh)

manifest = load("manifest.json")

def meta(name):
    m = manifest[name]
    return f"{m['url_base']}?typeNames={m['typeName']}", m["fecha_consulta"]

# COMMAND ----------

def save(df, table):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(table))
    print(f"Guardada {table}: {spark.table(table).count()} filas")

def to_df(features, name, cols, with_geometry=False):
    """cols: lista de (propiedad, tipo Spark). Se agregan _source_url y _queried_at."""
    url, fecha = meta(name)
    fields = [StructField(c, t) for c, t in cols]
    if with_geometry:
        fields.append(StructField("geometry", StringType()))
    fields += [StructField("_source_url", StringType()), StructField("_queried_at", StringType())]
    rows = []
    for f in features:
        p = f.get("properties", {})
        # DoubleType no acepta int de Python: se convierte (el valor no cambia)
        row = [float(p[c]) if isinstance(t, DoubleType) and p.get(c) is not None else p.get(c)
               for c, t in cols]
        if with_geometry:
            row.append(json.dumps(f.get("geometry")))
        rows.append(tuple(row + [url, fecha]))
    return spark.createDataFrame(rows, schema=StructType(fields))

S, L, D = StringType(), LongType(), DoubleType()

# COMMAND ----------

# Comunas y barrios DIVIPOLA (propiedades tal cual + geometry como texto GeoJSON)
df_com = to_df(load("comunas.geojson")["features"], "comunas.geojson",
               [("comcodigo", S), ("comnombre", S), ("comacuerdo", S), ("shape_leng", D),
                ("shape_area", D), ("comconsul", S)], with_geometry=True)
save(df_com, t_communes)

df_bar = to_df(load("barrios_divipola.geojson")["features"], "barrios_divipola.geojson",
               [("barcodigo", S), ("barnombre", S), ("barcomuna", S), ("barcategor", S),
                ("barlimites", S), ("barmapa", S), ("baracusan", S), ("shape_leng", D),
                ("shape_area", D), ("barconsul", S)], with_geometry=True)
save(df_bar, t_neighborhoods)

# COMMAND ----------

# Catastro: todo string salvo id_predio (long) y los conteos/medidas numéricas del origen
df_ter = to_df(load("catastro_terrenos_predios.json")["features"], "catastro_terrenos_predios.json",
               [("id_predio", L), ("idbarrio", S), ("nom_barrio", S), ("comuna", S), ("barrio", S),
                ("manzana", S), ("terreno", S), ("condicion", S), ("edifpred", S), ("pisopred", S),
                ("predio", S), ("npn", S), ("conexion", S), ("direpred", S), ("tipo_avalu", S),
                ("destinacio", S), ("uso_princi", S), ("n_construc", L), ("total_pis1", L),
                ("area_cons1", D), ("last_edite", S)])
save(df_ter, t_parcels)

df_cb = to_df(load("catastro_barrios.json")["features"], "catastro_barrios.json",
              [("idbarrio", S), ("nom_barrio", S), ("comuna", S), ("barrio", S)])
save(df_cb, t_cad_neighborhoods)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Códigos catastrales (CSV versionado en el repo)
# MAGIC `reference/codigos_catastrales.csv` se despliega junto con el notebook. Se lee relativo a la carpeta del notebook
# MAGIC (`../../reference/`); si el directorio de trabajo no es esa carpeta se prueba con la ruta del notebook bajo `/Workspace`.
# MAGIC Los significados que no están confirmados en una fuente oficial quedan vacíos y con estado `por verificar`: no se inventan.

# COMMAND ----------

rel = os.path.join("..", "..", "reference", "codigos_catastrales.csv")
candidates = [os.path.abspath(rel)]
try:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    candidates.append(os.path.abspath(os.path.join("/Workspace" + os.path.dirname(nb), rel)))
except Exception:
    pass
csv_path = next((p for p in candidates if os.path.exists(p)), None)
if csv_path is None:
    msg = (f"No se encontró codigos_catastrales.csv; se probó: {candidates}. "
           "Ejecuta `databricks bundle deploy` para subir `reference/`.")
    print(msg)
    dbutils.notebook.exit(msg)

pdf = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
pdf["_source_url"] = "reference/codigos_catastrales.csv"
pdf["_queried_at"] = max(m["fecha_consulta"] for m in manifest.values())
save(spark.createDataFrame(pdf), t_codes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría
# MAGIC Esperamos 22 comunas, 342 barrios DIVIPOLA, 52 códigos de barrio catastrales y 10 filas de códigos.

# COMMAND ----------

for t in [t_communes, t_neighborhoods, t_parcels, t_cad_neighborhoods, t_codes]:
    d = spark.table(t)
    print(f"{t}: {d.count()} filas, {len(d.columns)} columnas")
assert spark.table(t_communes).count() == 22
assert spark.table(t_neighborhoods).count() == 342
display(spark.table(t_parcels).limit(5))
