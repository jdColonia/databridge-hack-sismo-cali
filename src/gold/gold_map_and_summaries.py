# Databricks notebook source
# MAGIC %pip install shapely

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold: mapa de edificios y resúmenes por comuna, barrio y zona sísmica
# MAGIC Tablas listas para consumir (mapa, tableros):
# MAGIC
# MAGIC - `gold_{team}_map_buildings`: una fila por edificio con color, geometría WKT, zona de microzonificación sísmica y distancias a equipamientos (IDESC). **Incluye los polígonos dudosos** (con su flag `is_suspect_polygon`) para que quien consuma decida si filtrarlos.
# MAGIC - `gold_{team}_map_seismic_zones` y `gold_{team}_map_facilities`: capas de contexto para el mapa (polígonos de microzonificación que tocan el área de los edificios, con conteo y % de daño; equipamientos del área de estudio).
# MAGIC - `gold_{team}_commune_summary` y `gold_{team}_neighborhood_summary`: conteos, área construida y porcentajes de daño por comuna y por barrio **oficiales** (DIVIPOLA 2026, por ubicación).
# MAGIC - `gold_{team}_seismic_zone_summary`: los mismos conteos, área y porcentajes por **zona de microzonificación sísmica** (agrupada por nombre, porque el nombre se repite en varios polígonos), más pisos promedio y medianas de distancia al hospital y al espacio público de encuentro.
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
from pyspark.sql import Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("team", "el_equipo")
dbutils.widgets.text("volume", "el_equipo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
team = dbutils.widgets.get("team")
volume = dbutils.widgets.get("volume")

silver_table = f"{catalog}.{schema}.silver_{team}_buildings"
zones_table = f"{catalog}.{schema}.silver_{team}_building_seismic_zones"
dist_table = f"{catalog}.{schema}.silver_{team}_building_facility_distances"
zones_poly_table = f"{catalog}.{schema}.silver_{team}_seismic_zones"
facilities_table = f"{catalog}.{schema}.silver_{team}_facilities"
gold_map = f"{catalog}.{schema}.gold_{team}_map_buildings"
gold_map_zones = f"{catalog}.{schema}.gold_{team}_map_seismic_zones"
gold_map_facilities = f"{catalog}.{schema}.gold_{team}_map_facilities"
gold_commune = f"{catalog}.{schema}.gold_{team}_commune_summary"
gold_neighborhood = f"{catalog}.{schema}.gold_{team}_neighborhood_summary"
gold_zone = f"{catalog}.{schema}.gold_{team}_seismic_zone_summary"

missing_tables = [t for t in (silver_table, zones_table, dist_table, zones_poly_table, facilities_table) if not spark.catalog.tableExists(t)]
assert not missing_tables, f"Faltan tablas {missing_tables}: corre antes silver_buildings, silver_seismic_zones y silver_facilities."

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
    "map_buildings": {
        "table": "Vista plana para el mapa: una fila por edificio (5.146) con estado estructural, color, comuna y barrio oficiales, área, zona de microzonificación sísmica del IDESC, distancias a equipamientos del IDESC y geometría WKT. Incluye polígonos dudosos con su bandera.",
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
            "seismic_zone_id": "Identificador de la zona de microzonificación elegida (mayor área de intersección con la huella); nulo si el edificio no toca ninguna zona. Origen: silver_el_equipo_building_seismic_zones.",
            "seismic_zone_name": "Nombre de la zona de microzonificación elegida (ej. 'Zona 4C'); nulo si no toca ninguna. Origen: silver_el_equipo_building_seismic_zones.",
            "etc_am_g": "Espectro ETC, aceleración máxima (am) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "etc_sm_g": "Espectro ETC, aceleración de la meseta espectral (sm) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "etc_tc_s": "Espectro ETC, periodo tc en segundos de la zona; nulo si la zona requiere estudio particular o no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "is_liquefaction_susceptible": "Verdadero si la zona elegida es susceptible a licuación; nulo si no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "has_lateral_spread_risk": "Verdadero si la zona elegida tiene riesgo de corrimiento lateral; nulo si no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "requires_site_study": "Verdadero si la zona elegida requiere estudio particular de sitio (sin valores ETC); nulo si no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "seismic_zone_overlap_count": "Número de polígonos de zona que toca la huella del edificio (0 si ninguno). Mayor que 1 indica un edificio sobre el borde entre zonas. Origen: silver_el_equipo_building_seismic_zones.",
            "seismic_zone_overlap_pct": "Porcentaje (0 a 100) de la huella del edificio que cae dentro de la zona elegida; nulo si no hay zona. Origen: silver_el_equipo_building_seismic_zones.",
            "nearest_health_m": "Distancia en m al equipamiento de salud más cercano (hospital o no). Origen: silver_el_equipo_building_facility_distances.",
            "nearest_hospital_m": "Distancia en m al hospital más cercano (salud con is_hospital). Origen: silver_el_equipo_building_facility_distances.",
            "nearest_emergency_m": "Distancia en m a la estación de bomberos o sede de Defensa Civil más cercana. Origen: silver_el_equipo_building_facility_distances.",
            "nearest_police_m": "Distancia en m al equipamiento de policía más cercano (CAI, estaciones, etc.). Origen: silver_el_equipo_building_facility_distances.",
            "nearest_school_m": "Distancia en m a la sede educativa más cercana. Origen: silver_el_equipo_building_facility_distances.",
            "nearest_open_space_m": "Distancia en m al espacio público de encuentro más cercano (borde del polígono; 0 si el edificio está dentro). Origen: silver_el_equipo_building_facility_distances.",
            "nearest_hospital_name": "Nombre del hospital más cercano; nulo si el hospital no tiene nombre. Origen: silver_el_equipo_building_facility_distances.",
            "nearest_emergency_name": "Nombre de la estación de bomberos o sede de Defensa Civil más cercana; nulo si no tiene nombre. Origen: silver_el_equipo_building_facility_distances.",
            "nearest_open_space_name": "Nombre del espacio público más cercano; nulo si no tiene nombre. Origen: silver_el_equipo_building_facility_distances.",
            "open_space_count_500m": "Número de espacios públicos con alguna parte a 500 m o menos del centroide del edificio. Origen: silver_el_equipo_building_facility_distances.",
            "aftershock_priority_code": "Prioridad ante réplicas (criterio del equipo, no cifra oficial): high, medium, low o no_data. high = daño (roof_loss o debris) con algún factor agravante; medium = daño sin agravante, o sin cambio sobre suelo que requiere estudio particular o es licuable; low = sin cambio sin agravante de suelo; no_data = sin datos satelitales. Origen: derivado de structural_status_code y los factores agravantes.",
            "has_aggravating_factor": "Verdadero si el edificio tiene algún factor agravante ante réplicas: zona que requiere estudio particular, suelo licuable, hospital a más de 1.000 m o ningún espacio público de encuentro a 500 m. Nunca nulo. Origen: derivado de requires_site_study, is_liquefaction_susceptible, nearest_hospital_m y open_space_count_500m.",
            "priority_color_hex": "Color para el mapa según aftershock_priority_code: rojo high, naranja medium, verde low, gris no_data. Origen: derivado de aftershock_priority_code.",
            "geom_wkt": "Geometría del edificio en WKT, EPSG:4326 (lon, lat). Origen: silver_el_equipo_buildings.",
        },
    },
    "map_seismic_zones": {
        "table": "Polígonos de microzonificación sísmica del IDESC que tocan el área de los edificios (bbox de los edificios del mapa), con espectros ETC, banderas de suelo, número de edificios asignados y porcentaje de daño. Una fila por polígono; capa de contexto para el mapa.",
        "columns": {
            "seismic_zone_id": "Identificador del polígono de microzonificación (id del feature en el WFS del IDESC). Origen: silver_el_equipo_seismic_zones.",
            "seismic_zone_name": "Nombre de la zona de microzonificación (ej. 'Zona 4C'); puede repetirse entre polígonos. Origen: silver_el_equipo_seismic_zones.",
            "etc_am_g": "Espectro ETC, aceleración máxima (am) en g; nulo si la zona requiere estudio particular. Origen: silver_el_equipo_seismic_zones.",
            "etc_sm_g": "Espectro ETC, aceleración de la meseta espectral (sm) en g; nulo si la zona requiere estudio particular. Origen: silver_el_equipo_seismic_zones.",
            "etc_tc_s": "Espectro ETC, periodo tc en segundos; nulo si la zona requiere estudio particular. Origen: silver_el_equipo_seismic_zones.",
            "requires_site_study": "Verdadero si la zona requiere estudio particular de sitio (sin valores ETC). Origen: silver_el_equipo_seismic_zones.",
            "is_liquefaction_susceptible": "Verdadero si el suelo de la zona es susceptible a licuación. Origen: silver_el_equipo_seismic_zones.",
            "has_lateral_spread_risk": "Verdadero si la zona tiene riesgo de corrimiento lateral. Origen: silver_el_equipo_seismic_zones.",
            "building_count": "Número de edificios del mapa asignados a este polígono (0 si ninguno). Origen: conteo sobre gold_el_equipo_map_buildings por seismic_zone_id.",
            "damaged_building_pct": "Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. Origen: derivado de gold_el_equipo_map_buildings.",
            "geom_wkt": "Geometría de la zona en WKT, EPSG:4326 (lon, lat). Origen: silver_el_equipo_seismic_zones.",
        },
    },
    "map_facilities": {
        "table": "Equipamientos del IDESC dentro del área de estudio (bbox de los edificios ampliado 2.000 m): salud, bomberos y Defensa Civil, policía, colegios y espacio público de encuentro. Una fila por equipamiento; capa de contexto para el mapa.",
        "columns": {
            "facility_id": "Identificador del equipamiento, formato '<capa>.<id del WFS>'. Origen: silver_el_equipo_facilities.",
            "facility_type_code": "Tipo unificado: health, emergency, police, school u open_space. Origen: silver_el_equipo_facilities.",
            "facility_subtype": "Subtipo original (categoría de salud, tipo de seguridad, estado del colegio o tipo de espacio público); nulo si venía vacío. Origen: silver_el_equipo_facilities.",
            "facility_name": "Nombre del equipamiento; nulo si venía vacío. Origen: silver_el_equipo_facilities.",
            "is_hospital": "Verdadero si es de salud con categoría 'Hospitales - Hospitalización'. Origen: silver_el_equipo_facilities.",
            "care_level": "Nivel de atención de salud (1, 2 o 3); nulo si no es de salud o no tiene nivel. Origen: silver_el_equipo_facilities.",
            "enrollment_count": "Matrícula total de la sede educativa, en estudiantes; nulo si no es colegio. Origen: silver_el_equipo_facilities.",
            "lon": "Longitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. Origen: silver_el_equipo_facilities.",
            "lat": "Latitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. Origen: silver_el_equipo_facilities.",
            "geom_wkt": "Geometría del equipamiento en WKT, EPSG:4326 (lon, lat). Origen: silver_el_equipo_facilities.",
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
    "seismic_zone_summary": {
        "table": "Resumen del daño por zona de microzonificación sísmica (agrupado por nombre de zona, que se repite entre polígonos): conteos y área construida por estado estructural, porcentajes de daño y de cobertura sin datos, pisos promedio y medianas de distancia al hospital y al espacio público de encuentro. Una fila por zona con edificios del dataset, más una fila con zona nula si hay edificios que no tocan ninguna zona.",
        "columns": {
            "seismic_zone_name": "Nombre de la zona de microzonificación (ej. 'Zona 4C'); nulo para los edificios que no tocan ninguna zona. Origen: silver_el_equipo_building_seismic_zones.",
            "etc_am_g": "Espectro ETC, aceleración máxima (am) en g de la zona (máximo entre sus polígonos); nulo si la zona requiere estudio particular. Origen: silver_el_equipo_building_seismic_zones.",
            "etc_sm_g": "Espectro ETC, aceleración de la meseta espectral (sm) en g de la zona (máximo entre sus polígonos); nulo si la zona requiere estudio particular. Origen: silver_el_equipo_building_seismic_zones.",
            "etc_tc_s": "Espectro ETC, periodo tc en segundos de la zona (máximo entre sus polígonos); nulo si la zona requiere estudio particular. Origen: silver_el_equipo_building_seismic_zones.",
            "is_liquefaction_susceptible": "Verdadero si algún edificio de la zona está en suelo susceptible a licuación. Origen: silver_el_equipo_building_seismic_zones.",
            "requires_site_study": "Verdadero si algún edificio de la zona requiere estudio particular de sitio (sin valores ETC). Origen: silver_el_equipo_building_seismic_zones.",
            "building_count": "Número de edificios de la zona. Origen: conteo sobre silver_el_equipo_buildings.",
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
            "no_data_building_pct": "Porcentaje (0-100) de edificios sin datos satelitales sobre el total de la zona. Origen: derivado de los conteos.",
            "no_data_area_pct": "Porcentaje (0-100) del área construida sin datos satelitales sobre el total de la zona. Origen: derivado de las áreas.",
            "avg_floor_count": "Promedio de pisos de los edificios de la zona con número de pisos válido; nulo si ninguno lo tiene. Origen: promedio de floor_count de silver_el_equipo_buildings.",
            "median_nearest_hospital_m": "Mediana en m de la distancia al hospital más cercano de los edificios de la zona. Origen: mediana de nearest_hospital_m de silver_el_equipo_building_facility_distances.",
            "median_nearest_open_space_m": "Mediana en m de la distancia al espacio público de encuentro más cercano de los edificios de la zona. Origen: mediana de nearest_open_space_m de silver_el_equipo_building_facility_distances.",
        },
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapa de edificios
# MAGIC Paleta (de menor a mayor daño): azul `no_change`, naranja `roof_loss`, rojo `debris`, gris `no_data`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prioridad ante réplicas (`aftershock_priority_code`)
# MAGIC **Es un criterio del equipo para priorizar edificios ante réplicas, no una cifra oficial ni una evaluación estructural.** Combina el daño inferido por satélite con factores del entorno (IDESC):
# MAGIC
# MAGIC - **Factor agravante** (`has_aggravating_factor`): la zona elegida requiere estudio particular de sitio, el suelo es licuable, el hospital más cercano está a más de 1.000 m (`HOSPITAL_FAR_M`) o no hay espacio público de encuentro a 500 m (`OPEN_SPACE_RADIUS_M`, ya calculado en `open_space_count_500m`). Las banderas nulas (edificio sin zona) cuentan como falso.
# MAGIC - `no_data`: sin datos satelitales; no se puede priorizar.
# MAGIC - `high`: `roof_loss` o `debris` **con** algún factor agravante.
# MAGIC - `medium`: `roof_loss` o `debris` sin agravante, o `no_change` sobre suelo que requiere estudio particular o es licuable.
# MAGIC - `low`: `no_change` sin agravante de suelo.
# MAGIC
# MAGIC Los umbrales son constantes con nombre para poder cambiarlos. "Sin cambio aparente" no garantiza ausencia de daño interno: `low` no significa seguro.
# MAGIC Colores de prioridad: rojo `high`, naranja `medium`, verde `low`, gris `no_data`.

# COMMAND ----------

HOSPITAL_FAR_M = 1000       # hospital "lejos": más de 1.000 m
OPEN_SPACE_RADIUS_M = 500   # radio del conteo open_space_count_500m
assert OPEN_SPACE_RADIUS_M == 500, "open_space_count_500m está calculado a 500 m en silver_facilities"

# COMMAND ----------

s = spark.table(silver_table)

color = (F.when(F.col("structural_status_code") == "no_change", "#2b83ba")
          .when(F.col("structural_status_code") == "roof_loss", "#fdae61")
          .when(F.col("structural_status_code") == "debris", "#d7191c")
          .otherwise("#9e9e9e"))

zones = spark.table(zones_table)
dist = spark.table(dist_table)

# Prioridad ante réplicas (ver celda markdown anterior); las banderas nulas cuentan como falso para que el código nunca sea nulo
st = F.col("structural_status_code")
soil_factor = F.coalesce(F.col("requires_site_study"), F.lit(False)) | F.coalesce(F.col("is_liquefaction_susceptible"), F.lit(False))
access_factor = (F.coalesce(F.col("nearest_hospital_m") > HOSPITAL_FAR_M, F.lit(False))
                 | F.coalesce(F.col(f"open_space_count_{OPEN_SPACE_RADIUS_M}m") == 0, F.lit(False)))
aggravating = soil_factor | access_factor
is_damaged = st.isin("roof_loss", "debris")

priority = (F.when(st == "no_data", "no_data")
             .when(is_damaged & aggravating, "high")
             .when(is_damaged | ((st == "no_change") & soil_factor), "medium")
             .otherwise("low"))
priority_color = (F.when(priority == "high", "#d7191c")
                   .when(priority == "medium", "#fdae61")
                   .when(priority == "low", "#74add1")
                   .otherwise("#bababa"))

map_df = (s.select(
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
    .join(zones, on="building_id", how="left")
    .join(dist, on="building_id", how="left"))

map_df = (map_df
    .withColumn("aftershock_priority_code", priority)
    .withColumn("has_aggravating_factor", aggravating)
    .withColumn("priority_color_hex", priority_color))

# geom_wkt al final
cols = [c for c in map_df.columns if c != "geom_wkt"] + ["geom_wkt"]
map_df = map_df.select(cols)

map_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_map)
apply_comments(gold_map, DOCS["map_buildings"]["table"], DOCS["map_buildings"]["columns"])
n_map = spark.table(gold_map).count()
assert n_map == 5146, n_map
assert spark.table(gold_map).select("building_id").distinct().count() == n_map, "building_id repetido en el mapa"
print(f"Guardada {gold_map}: {n_map} filas")

# Auditoría de la prioridad
map_saved = spark.table(gold_map)
assert map_saved.filter(F.col("aftershock_priority_code").isNull()).count() == 0, "aftershock_priority_code nulo"
assert map_saved.filter(F.col("has_aggravating_factor").isNull()).count() == 0, "has_aggravating_factor nulo"
print("Distribución de aftershock_priority_code:")
(map_saved.groupBy("aftershock_priority_code").count()
    .withColumn("pct", F.round(100 * F.col("count") / n_map, 1))
    .orderBy("aftershock_priority_code").show())
print("has_aggravating_factor por estado estructural:")
(map_saved.groupBy("structural_status_code", "has_aggravating_factor").count()
    .withColumn("pct_of_status", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy("structural_status_code")), 1))
    .orderBy("structural_status_code", "has_aggravating_factor").show())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capas de contexto para el mapa: zonas sísmicas y equipamientos
# MAGIC - `gold_{team}_map_seismic_zones`: solo los polígonos de microzonificación que intersectan el bbox de los edificios (la capa trae toda Cali). La prueba geométrica usa `shapely` en EPSG:4326 en el driver (son 16 polígonos). Cada polígono lleva cuántos edificios tiene asignados y su % de daño (sobre edificios con dato satelital; nulo si ninguno).
# MAGIC - `gold_{team}_map_facilities`: equipamientos con `is_in_study_area` (bbox de los edificios ampliado 2.000 m), para dibujar hospitales, bomberos, colegios y espacios de encuentro junto a los edificios.

# COMMAND ----------

import shapely.wkt
from shapely.geometry import box

# El mapa no trae lon/lat; el bbox sale de los centroides de silver_buildings (los mismos 5.146 edificios)
bb = spark.table(silver_table).agg(F.min("lon").alias("x0"), F.max("lon").alias("x1"),
                                   F.min("lat").alias("y0"), F.max("lat").alias("y1")).first()
bbox = box(bb["x0"], bb["y0"], bb["x1"], bb["y1"])
print(f"bbox de los edificios: lon {bb['x0']:.4f}..{bb['x1']:.4f}, lat {bb['y0']:.4f}..{bb['y1']:.4f}")

zone_polys = spark.table(zones_poly_table).select("seismic_zone_id", "geom_wkt").toPandas()
touching_ids = [r.seismic_zone_id for r in zone_polys.itertuples()
                if shapely.wkt.loads(r.geom_wkt).intersects(bbox)]
print(f"{len(touching_ids)} de {len(zone_polys)} polígonos tocan el bbox")
assert touching_ids, "ningún polígono de zona toca el bbox"

zone_counts = (map_saved.groupBy("seismic_zone_id").agg(
    F.count("*").alias("building_count"),
    F.sum(F.when(F.col("structural_status_code").isin("roof_loss", "debris"), 1).otherwise(0)).alias("_damaged"),
    F.sum(F.when(F.col("structural_status_code").isin("no_change", "roof_loss", "debris"), 1).otherwise(0)).alias("_with_data")))

zones_map_df = (spark.table(zones_poly_table)
    .filter(F.col("seismic_zone_id").isin(touching_ids))
    .join(zone_counts, on="seismic_zone_id", how="left")
    .withColumn("building_count", F.coalesce(F.col("building_count"), F.lit(0)).cast("long"))
    .withColumn("damaged_building_pct", 100 * F.col("_damaged") / F.when(F.col("_with_data") > 0, F.col("_with_data")))
    .select("seismic_zone_id", "seismic_zone_name", "etc_am_g", "etc_sm_g", "etc_tc_s",
            "requires_site_study", "is_liquefaction_susceptible", "has_lateral_spread_risk",
            "building_count", "damaged_building_pct", "geom_wkt")
    .orderBy("seismic_zone_id"))
zones_map_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_map_zones)
apply_comments(gold_map_zones, DOCS["map_seismic_zones"]["table"], DOCS["map_seismic_zones"]["columns"])
n_zones = spark.table(gold_map_zones).count()
assert n_zones == len(touching_ids), n_zones
assigned = spark.table(gold_map_zones).agg(F.sum("building_count")).first()[0]
assert assigned <= n_map, assigned
print(f"Guardada {gold_map_zones}: {n_zones} filas, {assigned} de {n_map} edificios asignados a algún polígono")

fac_df = (spark.table(facilities_table)
    .filter(F.col("is_in_study_area"))
    .select("facility_id", "facility_type_code", "facility_subtype", "facility_name", "is_hospital",
            "care_level", "enrollment_count", "lon", "lat", "geom_wkt")
    .orderBy("facility_id"))
fac_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_map_facilities)
apply_comments(gold_map_facilities, DOCS["map_facilities"]["table"], DOCS["map_facilities"]["columns"])
n_fac = spark.table(gold_map_facilities).count()
assert n_fac > 0, "sin equipamientos en el área de estudio"
print(f"Guardada {gold_map_facilities}: {n_fac} filas")
spark.table(gold_map_facilities).groupBy("facility_type_code").count().orderBy("facility_type_code").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resúmenes por comuna, barrio y zona sísmica
# MAGIC Misma función para los tres niveles. Porcentajes: `(roof_loss + debris) / (no_change + roof_loss + debris)`, por conteo y por área construida.

# COMMAND ----------

def summarize(df, keys, extra_aggs=()):
    statuses = ["no_change", "roof_loss", "debris", "no_data"]
    aggs = [F.count("*").alias("building_count")]
    for st in statuses:
        aggs.append(F.sum(F.when(F.col("structural_status_code") == st, 1).otherwise(0)).cast("long")
                     .alias(f"{st}_building_count"))
    aggs.append(F.sum("built_area_m2").alias("built_area_m2"))
    for st in statuses:
        aggs.append(F.sum(F.when(F.col("structural_status_code") == st, F.col("built_area_m2")).otherwise(0.0))
                     .alias(f"{st}_built_area_m2"))
    r = df.groupBy(*keys).agg(*aggs, *extra_aggs)

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

# Zona sísmica: el nombre se repite entre polígonos, así que se agrupa por nombre (los edificios sin zona quedan en una fila con nombre nulo)
map_tbl = spark.table(gold_map).alias("m")
zone_df = summarize(
    map_tbl, ["seismic_zone_name"],
    extra_aggs=[
        F.max("etc_am_g").alias("etc_am_g"),
        F.max("etc_sm_g").alias("etc_sm_g"),
        F.max("etc_tc_s").alias("etc_tc_s"),
        F.coalesce(F.max(F.col("is_liquefaction_susceptible").cast("int")) == 1, F.lit(False)).alias("is_liquefaction_susceptible"),
        F.coalesce(F.max(F.col("requires_site_study").cast("int")) == 1, F.lit(False)).alias("requires_site_study"),
        F.avg("floor_count").alias("avg_floor_count"),
        F.percentile_approx("nearest_hospital_m", 0.5).alias("median_nearest_hospital_m"),
        F.percentile_approx("nearest_open_space_m", 0.5).alias("median_nearest_open_space_m"),
    ],
)
zone_cols = ["seismic_zone_name", "etc_am_g", "etc_sm_g", "etc_tc_s", "is_liquefaction_susceptible", "requires_site_study"]
zone_df = zone_df.select(*zone_cols, *[c for c in zone_df.columns if c not in zone_cols])
zone_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_zone)
apply_comments(gold_zone, DOCS["seismic_zone_summary"]["table"], DOCS["seismic_zone_summary"]["columns"])

print(f"Guardadas {gold_commune}, {gold_neighborhood} y {gold_zone}")

# COMMAND ----------

display(spark.table(gold_zone).orderBy(F.col("damaged_area_pct").desc_nulls_last()))
display(spark.table(gold_commune).orderBy(F.col("damaged_area_pct").desc_nulls_last()))
