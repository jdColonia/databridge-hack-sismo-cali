# CLAUDE.md — Contexto, Arquitectura y Skills del Proyecto (DataBridge Hack)

Eres un **Ingeniero de Datos Senior, Especialista SIG (Sistemas de Información Geográfica) y Científico de Datos** colaborando en el hackathon **DataBridge Hack (IEEE Computer Society Colombia Chapter)**.
Tu objetivo es ayudar al equipo a diseñar, programar, validar y documentar un pipeline analítico y geoespacial de alto impacto en **Databricks (Free Edition)** y una visualización cartográfica clara antes de las **2:00 p. m. del viernes 13 de noviembre** (cierre de desarrollo) y envío por correo antes de las **5:00 p. m.**.

**Equipo:** "El Equipo" → prefijo de tablas **`el_equipo`** (ej. `bronze_el_equipo_hackathon_buildings`). Asunto del correo: `databridgehack: El Equipo`.
**Workspace compartido:** `https://dbc-240be77b-cf4c.cloud.databricks.com`, perfil de la CLI `el-equipo`. El flujo de trabajo detallado está en `README.md`; la convención de nombres en [`docs/conventions.md`](docs/conventions.md) y el diccionario de tablas en [`docs/data_dictionary.md`](docs/data_dictionary.md).

---

## 1. El Reto y Criterios de Evaluación (Rúbrica)

* **Pregunta Problema:** *¿Qué afectaciones generó el sismo del 10 de agosto en la ciudad de Cali y qué factores contribuyen a reducir el impacto de las réplicas sobre la población, la infraestructura y los servicios urbanos?*
* **Contexto del Evento:** Sismo de magnitud 7,4 con epicentro en el Chocó que afectó fuertemente a Santiago de Cali (colapsos de edificios, daños en hospitales y colegios; cifras oficiales preliminares).
* **Rúbrica del Jurado (100 puntos):**
  1. **Respuesta a la pregunta problema (30 pts):** Responde ambas partes (afectaciones del sismo + factores de mitigación ante réplicas en población, infraestructura y servicios) conectadas y visibles en el mapa.
  2. **Datos externos (25 pts):** Fuentes reales, citadas con enlace, pertinentes para Cali, recientes y cruzadas espacial/catastralmente con los edificios. Un cruce sólido vale más que cinco superficiales. *Prohibido inventar datos*.
  3. **Calidad del análisis (20 pts):** Hallazgos concretos, cifras trazables hasta tablas `gold`, y reconocimiento explícito de los sesgos/límites del dato satelital.
  4. **Presentación de resultados (15 pts):** Mapa comprensible en 30 segundos + 3 diapositivas ordenadas (sustentación de 5 min el sábado 14 de nov, estrictamente con lo enviado el viernes).
  5. **Arquitectura de las capas (10 pts):** Decisiones técnicas justificadas en `bronze`, `silver` y `gold`.

---

## 2. Dataset Base (`Datosprediales.json`) y Trampas Conocidas

El archivo base es un `FeatureCollection` GeoJSON con **5.146 polígonos de edificios en Cali**, **20 propiedades** + `geometry` en CRS `EPSG:4326` (`[longitud, latitud]`), concentrados en el corredor centro-sur y ladera de Cali (Comunas **3, 9, 10, 17, 18, 19 y 20**).

### Distribución de `Estado_Estructural` (basado en `Indice_Escombros` satelital):
* `Sin cambio aparente / Daño interno`: **3.357** edificios
* `Posible desaparición de techo`: **1.058** edificios
* `Sin datos satelitales`: **435** edificios
* `Alta probabilidad de escombros`: **296** edificios

### Reglas Obligatorias de Calidad de Datos ("Trampas" del Dataset):
Al escribir código PySpark / Python / SQL, **SIEMPRE** debes manejar estas anomalías detectadas en el esquema real:
1. **Llave Primaria (`OBJECTID`):** `ID_PREDIO` es `null` en 1.509 filas y `0` en otras 1.697. Usa **`OBJECTID`** (1 a 5.146) como identificador único.
2. **Identificación de Barrios (`COMUNA` + `BARRIO`):** El código de `BARRIO` se repite entre distintas comunas (16 códigos aparecen en más de una comuna). Nunca agrupes solo por `BARRIO`; usa el código `CCBB` = `CONCAT(TRIM(COMUNA), TRIM(BARRIO))` (`cadastral_neighborhood_code` en `silver_el_equipo_buildings`, 4 dígitos sin guion). `COMUNA` y `BARRIO` están vacíos en 2 filas. Para nombres, mapa y agregaciones se usa el barrio oficial por ubicación (`neighborhood_code`, `neighborhood_name`).
3. **Strings con Espacios en Blanco:** Campos como `TERRENO`, `EDIFPRED`, `NUMEPRED`, `COND`, `MUNIPRED`, `SECTOR` contienen espacios `" "` en lugar de `null` (ej. `"TERRENO": " "` o `" 0042"`). Aplica `TRIM()` y convierte cadenas vacías a `NULL` en Silver.
4. **Cálculo de Áreas Reales (`EPSG:9377`):** `Shape__Area` y `Shape__Length` vienen en **grados decimales** (ej. `4.26e-09`), no en metros. Para calcular $m^2$ reales, reproyecta las geometrías al sistema métrico oficial de Colombia **MAGNA-SIRGAS / Origen-Nacional (`EPSG:9377`)**.
5. **Altura (`NPISOS = 0`):** Hay 1.234 edificios con `NPISOS = 0` (lotes, estructuras no categorizadas o dato faltante en catastro). No los trates como edificios de 0 pisos válidos. En silver: `floor_count_raw` (el original), `floor_count` (null si `NPISOS <= 0`), bandera `is_floor_count_missing` y `built_area_m2 = area_m2 * coalesce(floor_count, 1)` con bandera `is_built_area_estimated`.
6. **Área sin datos satelitales (¡la guía y el dato no coinciden!):** la guía dice que los 435 edificios `"Sin datos satelitales"` suman ~45 % del área construida, pero **medido sobre el archivo es ≈9,5 %** (huella en EPSG:9377 y también con `Shape__Area`). Usa la cifra medida y menciona la discrepancia como límite del dato. Los porcentajes de daño excluyen `no_data` del denominador y la cobertura se reporta aparte (`no_data_area_pct`).
7. **Lectura Responsable:** Son inferencias satelitales, no inspecciones estructurales en sitio. *"Sin cambio aparente"* no garantiza ausencia de daño estructural interno.

---

## 3. Arquitectura Medallion en Databricks

Convención de nombres `<capa>_el_equipo_<contenido>` (ej. `bronze_el_equipo_hackathon_buildings`, `silver_el_equipo_buildings`), con identificadores en inglés y textos en español: ver [`docs/conventions.md`](docs/conventions.md). El detalle de columnas está en [`docs/data_dictionary.md`](docs/data_dictionary.md). Las tablas finales (las de la captura del correo) viven en **`workspace.default`**; las pruebas de cada integrante, en `workspace.dev_<usuario>` (ver sección 4).

| Capa | Tabla | Estado |
| ---- | ----- | ------ |
| Bronze | `bronze_el_equipo_hackathon_buildings` (5.146 filas × 21 columnas) | **Creada** en `workspace.default` |
| Bronze | `bronze_el_equipo_idesc_communes`, `_idesc_neighborhoods`, `_idesc_cadastre_parcels`, `_idesc_cadastre_neighborhoods`, `bronze_el_equipo_manual_code_catalog` | Probadas en `dev`; referencias ya subidas al volumen compartido |
| Bronze | `bronze_el_equipo_idesc_seismic_zones`, `_idesc_health_facilities`, `_idesc_security_facilities`, `_idesc_schools`, `_idesc_public_spaces`, `bronze_el_equipo_usgs_earthquakes`, `_usgs_aftershock_forecast`, `bronze_el_equipo_manual_official_impact` | Escritas; pendiente correrlas |
| Silver | `silver_el_equipo_buildings` (con comuna/barrio oficiales y catastro) | Probada en `dev`; pendiente en `team` |
| Silver | `silver_el_equipo_communes`, `_neighborhoods`, `_cadastre_parcels`, `_cadastre_neighborhoods`, `_code_catalog` | Probadas en `dev` |
| Silver | `silver_el_equipo_neighborhoods` (342 barrios oficiales) y `silver_el_equipo_neighborhood_crosswalk` | Probadas en `dev` |
| Silver | `silver_el_equipo_seismic_zones` (16 zonas) y `_building_seismic_zones` (zona de cada edificio) | Escritas; pendiente correrlas |
| Silver | `silver_el_equipo_facilities` y `_building_facility_distances` (distancias a salud, hospital, bomberos/Defensa Civil, policía, colegios y espacio público) | Escritas; pendiente correrlas |
| Gold | `gold_el_equipo_map_buildings` (con zona sísmica y distancias), `_commune_summary`, `_neighborhood_summary`, `_seismic_zone_summary` (provisionales) | Pendiente de correr en `team`; el modelo estrella está por definir |

* **Capa Bronze (`bronze_el_equipo_*`):**
  * Ingesta cruda desde el volumen compartido `/Volumes/workspace/default/el_equipo/Datosprediales.json` (ya subido).
  * Esquema exacto de `bronze_el_equipo_hackathon_buildings`: **5.146 filas y 21 columnas** (20 propiedades intactas + `geometry` como texto GeoJSON), con los nombres originales y sin metadatos. Cero transformaciones (excepción documentada en `docs/conventions.md`).
  * **Cada fuente externa entra primero a bronze, en crudo**, con las columnas originales más `_source_url`, `_queried_at` y `_load_origin` (`wfs`, `api` o `volume_copy`) (`bronze_el_equipo_<source>_<entity>`). Nunca directo a silver.
* **Capa Silver (`silver_el_equipo_*`):**
  * Limpieza de strings (`TRIM`, vacíos a null), código de barrio catastral `cadastral_neighborhood_code` (`CCBB`), `floor_count`, reparación de geometría (`is_geometry_repaired`), bandera `is_suspect_polygon` (área < 5 m² o > 50.000 m²: 344 edificios miden menos de 5 m²; se marcan, no se borran).
  * Reproyección a `EPSG:9377` para `area_m2`, `perimeter_m` y `built_area_m2`. Centroide `lon`/`lat` en 4326. `structural_status_code` (`no_change`, `roof_loss`, `debris`, `no_data`) y `damage_level` ordinal.
  * **Los cruces espaciales con fuentes externas se hacen en silver** (`sjoin`, `sjoin_nearest` en EPSG:9377), no en gold.
* **Capa Gold (`gold_el_equipo_*`): modelo estrella (pendiente de definir).**
  * Grano decidido: **una fila por edificio** (`building_id`) en la tabla de hechos `fact_buildings`.
  * Dimensiones ya claras: `dim_structural_status` (4 categorías, orden, color) y `dim_territory` (comuna, barrio, manzana), que sale de `silver_el_equipo_neighborhoods`. **Usar siempre `neighborhood_code/neighborhood_name` y `commune_code/commune_name`** (DIVIPOLA 2026 por ubicación, cubre los 5.146 edificios) para nombres, mapa y agregaciones; `cadastral_neighborhood_code` es solo el código catastral del dataset (su traducción está en `silver_el_equipo_neighborhood_crosswalk`).
  * Ya existen como silver: la zona de microzonificación de cada edificio (`silver_el_equipo_building_seismic_zones`, candidata a `dim_seismic_zone`) y las distancias a equipamientos (`silver_el_equipo_building_facility_distances`); ambas se unen en `gold_el_equipo_map_buildings`. Pendientes: intensidad del sismo (PGA/MMI como medidas del hecho) y `fact_aftershocks` (una fila por sismo, contexto temporal; la tabla bronze `bronze_el_equipo_usgs_earthquakes` ya está).
  * El mapa sale de una **vista plana** que une hechos y dimensiones (`gold_el_equipo_map_buildings`); esa vista es la que se exporta a GeoJSON.
  * El notebook actual `src/gold/gold_map_and_summaries.py` (tablas planas) es provisional y se reemplaza al definir la estrella.
  * **CRÍTICO PARA EXPORTACIÓN DE MAPA:** Antes de exportar Gold a `.geojson`, **reproyectar de vuelta a `EPSG:4326`** verificando orden `[longitud, latitud]` (`[-76.5..., 3.4...]`), incluir solo columnas necesarias para tooltips/filtros y una columna categórica/numérica limpia lista para colorear (ej. `aftershock_risk_level`, `color_hex` o `mitigation_priority_index`).

---

## 4. Repositorio y Databricks Asset Bundle (cómo se trabaja)

Todo el código vive en este repositorio y se despliega con un **Databricks Asset Bundle**. No se crean jobs ni notebooks a mano en la interfaz.

```
databricks.yml                         # bundle, variables y targets
resources/el_equipo_medallion.job.yml  # job serverless: setup → bronze → silver → gold → export
docs/                                  # conventions.md (convención de nombres) y data_dictionary.md
src/setup/   src/bronze/   src/silver/   src/gold/   src/export/   # un notebook .py por paso, por capa
scripts/upload_data.sh                 # sube el crudo al volumen
scripts/diagrams/                     # generador DBML desde Unity Catalog y ChartDB local
tools/diagrams/                       # validador DBML (importación ChartDB)
docs/diagrams/                        # diagramas de datos .dbml + imágenes (ver Skill 4)
data/                                  # datos locales, en .gitignore (nunca se suben)
```

**Reglas para el código:**
* Los notebooks son archivos `.py` en formato fuente de Databricks (`# Databricks notebook source`, celdas separadas por `# COMMAND ----------`, markdown con `# MAGIC %md`).
* **Nunca escribas nombres de tabla ni rutas a mano** (`workspace.default.xxx`). Lee los parámetros con `dbutils.widgets` (`catalog`, `schema`, `raw_schema`, `team`, `volume`) y arma los nombres: `f"{catalog}.{schema}.silver_{team}_buildings"`. El crudo se lee de `f"/Volumes/{catalog}/{raw_schema}/{volume}/..."`.
* Un notebook nuevo va en la carpeta de su capa y **se agrega como tarea** en `resources/el_equipo_medallion.job.yml` con `depends_on` y `base_parameters: *params`. Los nombres no llevan números: el orden lo da el job. El `task_key` es el nombre del notebook sin carpeta (`silver_buildings`).
* **Todo identificador nuevo (tabla, columna, `task_key`, valor categórico) sigue `docs/conventions.md`.** Cada tabla silver/gold aplica comentarios de tabla y columna en español desde un diccionario `DOCS` del notebook, y se documenta en `docs/data_dictionary.md`.
* Cada notebook explica sus decisiones en celdas markdown en español (sirven para la diapositiva de arquitectura).

**Targets:**

| Target | Escribe en | Uso |
| ------ | ---------- | --- |
| `dev` (por defecto) | `workspace.dev_<usuario>` | Pruebas de cada integrante |
| `team` | `workspace.default` | Versión final, desde `main`, una sola persona |

```bash
databricks bundle validate -p el-equipo
databricks bundle deploy -t dev -p el-equipo
databricks bundle run el_equipo_medallion -t dev -p el-equipo --only silver_buildings
```

* `deploy` sube código y actualiza el job, no toca datos; `run` ejecuta y crea tablas.
* Si alguien edita un notebook en la interfaz de Databricks, el siguiente `deploy` lo sobrescribe: los cambios se hacen en el repositorio.
* En Git Bash (Windows), las llamadas `databricks api` con rutas `/api/...` necesitan `MSYS_NO_PATHCONV=1`.

**Git:** ramas cortas (`feat/...`, `fix/...`) con PR a `main`; Conventional Commits en inglés; `main` siempre pasa `bundle validate`; nunca subir datos, Excel ni credenciales.

---

## 5. Fuentes Externas Investigadas

| Fuente | Estado | Uso |
| ------ | ------ | --- |
| **USGS, evento `us6000tjl2`** (M7,4, 10 ago 2026, 116 km de profundidad). [Evento](https://earthquake.usgs.gov/earthquakes/eventpage/us6000tjl2), ShakeMap `grid.xml` (MMI, PGA, Vs30) y réplicas por API FDSN | Verificada | Contexto de intensidad. En el área de los edificios el MMI varía poco (6,4–7,1): no explica diferencias entre edificios. Réplicas: usar el círculo oficial de la secuencia USGS (centro 4,4791 / −76,5739, radio 125,4 km): 60 réplicas M≥3,5 al 13 nov, la mayor **M5,0 (10 ago, `us6000tjm4`)**. La M5,4 del 3 nov (Rioblanco, Tolima, 10 km de profundidad) queda fuera del círculo: **no es réplica**. Pronóstico oficial (producto `oaf`): P(M≥5) en el próximo mes = 31,5 % (`bronze_el_equipo_usgs_aftershock_forecast`). |
| **IDESC, microzonificación sísmica** `idesc:mc_microzonificacion_sismica` (16 zonas: `zona_mzsc`, `tc_etc`, `sucep_licu`, `corrim_lat`). WFS: `https://ws-idesc.cali.gov.co/geoserver/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=idesc:mc_microzonificacion_sismica&outputFormat=application/json&srsName=EPSG:4326` | Verificada 13 nov 2026 (HTTP 200, 16 polígonos, 835 KB). Estudio INGEOMINAS–DAGMA 2005, adoptado por el Decreto 411.0.20.0158 de 2014 | **Cruce principal:** suelo × daño × pisos (resonancia). Explica el *por qué*. Ver trampas abajo |
| **IDESC, equipamientos:** `pot_2014:eqp_uco_salud` (482), `pot_2014:eqp_uba_seguridad_ciudadana` (bomberos, Defensa Civil), `educacion:esf_establecimiento_educativo` (356, actualizada 2023–2024), `dapm:pdt_pmep_encuentro` | Revisadas por el agente de investigación. Capas `pot_2014` sin fecha (probablemente 2014) | **Segundo cruce:** distancia a la ayuda más cercana. Responde *qué reduce el impacto de las réplicas* |
| OpenStreetMap (Overpass) | **Descartada:** no respondió en las pruebas | Ya no se usa; los equipamientos salen del IDESC |
| **Cifras oficiales de impacto**, Alcaldía de Santiago de Cali: [Repositorio oficial](https://www.cali.gov.co/boletines/publicaciones/193607/terremoto-de-cali-repositorio-oficial-de-informacion/), corte 16 oct 2026 | Verificada | Contexto: 154 fallecidos, 1.657 lesionados, 88 rescatados, 14 desaparecidos y 24 edificaciones con colapso total (`reference/official_impact.csv`). La cifra de 308 edificaciones colapsadas atribuida a UNGRD/DANE queda **por verificar** (no se encontró la fuente) |
| Población por comuna (Planeación / DANE) | No verificada | Pendiente |
| Prensa (Infobae: 74 muertos en Cali) | Sin fuente oficial | Citar como "prensa", no como dato oficial |

* **IDESC, referencias de códigos** (descargadas el 13 nov 2026 con `scripts/download_referencias.py`; datos catastrales con `last_edite` 2025-09-02): `dapm:pdt_dpa_comunas` (22 comunas), `dapm:pdt_dpa_barrios_sectores` (342 barrios, Acuerdo 0636 de 2026 DIVIPOLA, [acuerdo](https://idesc.cali.gov.co/download/acuerdos/2026-06-26_acuerdo_0636_divipola.pdf)) y `catastro:cat_bas_terrenos` (756.277 terrenos; solo se piden los de nuestros predios). Verificadas. Tasas medidas con la simulación local: código catastral de barrio vigente (`is_neighborhood_code_current`) **91,4 %** (4.703 de 5.144 con código; 13 códigos que el Acuerdo 0636 omitió o cambió, el resto de la diferencia son barrios recodificados), 2 edificios sin código catastral (COMUNA/BARRIO vacíos); todos los edificios tienen barrio oficial por ubicación; catastro (`cadastre_match_method`): `parcel_id` 37,1 %, `connection` única 52,3 %, `ambiguous_connection` 7,4 %, `no_match` 3,2 %. El GeoServer responde 403 a filtros CQL con texto por GET: usar POST.
* `datos.cali.gov.co` está caído (403) desde el 21 oct 2026; el GeoServer del IDESC sí responde. Descargar las capas cuanto antes.

### Microzonificación sísmica: zonas y trampas

Zonas que tocan el bbox de los edificios (`am_etc` y `sm_etc` en g, `tc_etc` en s): Zona 1 cerros/roca (0,22 · 0,45 · 0,55), Zona 2 (0,35 · 0,75 · 0,45), Zona 3 piedemonte (0,35 · 0,85 · 1,05), Zona 4A (0,33 · 0,75 · 0,75), Zona 4B (0,28 · 0,65 · 0,70), **Zona 4C abanico de Cañaveralejo (0,40 · 1,00 · 0,45, la mayor demanda de la ciudad)**, Zona 4D (0,28 · 0,62 · 1,20), Zona 5 (0,28 · 0,70 · 0,60, licuable), además de Depósitos Antrópicos, Suelo Coluvión y Cauce Activo. Zonas 4E y 6 quedan fuera del bbox.

1. **Los ceros no son cero:** en `Depositos Antropicos`, `Suelo Coluvion` y `Cauce Activo o Terra` los campos `*_etc` valen `0`, que significa "requiere estudio particular". En silver pasarlos a `NULL` con bandera `requiere_estudio_particular`; nunca tratarlos como aceleración cero.
2. **Polígonos repetidos:** `Zona 6` aparece 2 veces y `Cauce Activo o Terra` 3, con igual `area`/`perimeter` pero distinto `sucep_licu`/`corrim_lat`. No deduplicar por nombre: conservar el `id` del feature y revisar edificios que caigan en más de un polígono.
3. **Columnas basura y tipos:** descartar `mcooiiain_`, `mcooiiain1`, `mcooiiai_1`, `mcooiiai_2`. Las `*_etl` vienen como texto (`'0.40'`) y casi siempre nulas: castear o ignorar. `area`/`perimeter` están en el CRS original; si se necesita área, recalcular en EPSG:9377.
4. **Cruce:** `sjoin` directo edificio → microzona en EPSG:9377 (no pasa por barrios). Implementado en `silver_el_equipo_building_seismic_zones`.
5. **Respaldo:** la infografía de El País (sept 2026, `multimedia.elpais.com.co/2026/Septiembre/Un_mes_despues_Cali_sigue_de_pie/infografia/microzonas-v9.html`) trae la tabla barrio → microzona (342 barrios, % por zona) con los mismos valores. Usar solo si el GeoServer cae; es fuente secundaria (prensa).
6. **Fecha para el correo:** la capa no trae fecha. Citar el estudio de 2005 y el decreto de 2014 (sin verificar en el texto del decreto).
* En la lista de fuentes del correo, anotar la **fecha del dato**, no la fecha de descarga.
* Recomendación: **dos cruces sólidos** (microzonificación y equipamientos) mejor que muchos superficiales.

---

## 6. Skills Especializados Activos

Cuando te pida código o análisis, aplica automáticamente estas habilidades:

### Skill 1: `databricks-geo-etl` (Python / PySpark / GeoPandas / Shapely)
* Databricks Free Edition (Serverless) no siempre tiene Apache Sedona preinstalado; prioriza soluciones híbridas robustas usando `json` nativo de Python + `shapely` + `pyproj` + `geopandas` (con `# MAGIC %pip install ...` y `dbutils.library.restartPython()` al inicio del notebook) para convertir a DataFrames de Spark y guardar con `.write.format("delta").mode("overwrite").saveAsTable(tabla)`, donde `tabla` se arma con los parámetros del bundle (sección 4).
* Maneja tanto `Polygon` como `MultiPolygon` (ej. `OBJECTID: 338` es `MultiPolygon`, y varios polígonos tienen anillos interiores/patios) sin que fallen las transformaciones de coordenadas.

### Skill 2: `fast-external-data-cali` (Extracción y Cruce de Fuentes Externas)
Cuando te pida descargar o cruzar datos externos de Cali para responder el *por qué* del daño y *qué reduce el impacto de las réplicas*:
0. **Primero el IDESC** (sección 5): fuente oficial y verificada. OpenStreetMap se descartó porque no respondió. La salida a internet desde serverless en Free Edition no está comprobada; si falla, descargar localmente a `data/external/` y subir al volumen (copia de respaldo que lee el bronze, `_load_origin = 'volume_copy'`).
1. **Equipamientos del IDESC** (`silver_el_equipo_facilities`): salud (hospitales aparte con `is_hospital`), bomberos y Defensa Civil (`emergency`), policía, sedes educativas y espacio público de encuentro (`open_space`, puntos de reunión libres de escombros ante réplicas). Las distancias exactas en metros salen de `silver_el_equipo_building_facility_distances` (`nearest_hospital_m`, `nearest_emergency_m`, `nearest_open_space_m`, `open_space_count_500m`).
2. **Microzonificación Sísmica / Suelo de Cali (IDESC - Alcaldía de Cali):** Si cargamos un Shapefile/GeoJSON de IDESC (`idesc.cali.gov.co`) en el Volume, crúzalo espacialmente en `EPSG:9377` para asignar a cada edificio su zona de respuesta sísmica / tipo de suelo y correlacionarlo con `Estado_Estructural` y `NPISOS`.
3. **Trazabilidad y Citación:** Guarda toda extracción externa cruda primero en `bronze_el_equipo_<source>_<entity>` (incluyendo `_source_url`, `_queried_at` y `_load_origin`) antes de limpiarla y cruzarla en `silver_el_equipo_*` para calcular distancias exactas en metros (`nearest_hospital_m`, `nearest_open_space_m`).

### Skill 3: `deliverables-generator` (Textos Exactos y Pitch de 5 Minutos)
Cuando solicite los textos de entrega para el correo (`marcelasabogue@ieee.org`, asunto `databridgehack: El Equipo`), respeta **estrictamente** los límites de palabras de la guía:
* **Texto 1 (Decisiones en `silver` y `gold`):** Qué se realizó en cada capa y por qué — **máximo 200 palabras**.
* **Texto 2 (Respuesta a las dos preguntas):** Qué afectaciones generó el sismo y qué factores reducen el impacto de las réplicas sobre población, infraestructura y servicios urbanos, con cifras trazables a `gold` — **máximo 500 palabras**.
* **Lista de Fuentes Externas:** Nombre oficial, enlace verificable y qué aportó cada una al análisis (no cuenta dentro del límite de palabras).
* **Estructura de 3 Diapositivas (PowerPoint/PDF):**
  * **Diapositiva 1 (Arquitectura):** Decisiones justificadas en `bronze`, `silver` y `gold`.
  * **Diapositiva 2 (Análisis y Respuesta):** Afectaciones encontradas + factores que reducen el impacto de réplicas, respaldados con datos de `gold`.
  * **Diapositiva 3 (El Mapa):** Pantallazo del mapa final + qué se eligió mostrar, por qué y cómo responde las preguntas.

### Skill 4: `chartdb-diagrams` (Diagramas de datos, herramienta oficial: ChartDB)
**ChartDB es la herramienta oficial para diagramas de datos.** Guía completa: `docs/diagrams/README.md`.
* **Fuente de verdad:** archivos DBML en `docs/diagrams/*.dbml`, versionados en Git. ChartDB no tiene conector para Databricks y en su versión cloud la exportación DBML/JSON es paga: se importa DBML (pantalla inicial: PostgreSQL › pestaña DBML › Import; con un diagrama abierto: *Actions › Import*) y se exporta la imagen (*Actions › Export as › PNG/SVG*) a `docs/diagrams/img/`, con un *Area* por capa porque ChartDB no importa `TableGroup`.
* **Generar desde Databricks:** `python3 scripts/diagrams/catalog_to_dbml.py --schema dev_<usuario> --relaciones docs/diagrams/relaciones_logicas.json --out docs/diagrams/<nombre>.dbml`. Para tablas aún no corridas, genera desde el diccionario: `python3 scripts/diagrams/dictionary_to_dbml.py` (vistas actuales: `medallion_overview`, `silver_gold_model`, `bronze_sources`; comandos en `docs/diagrams/README.md`). Si cambias `docs/data_dictionary.md` o una relación, regenera los tres DBML y sus PNG.
* **Validar siempre** antes del PR: `npm ci --prefix tools/diagrams` (una vez) y `node tools/diagrams/validate_dbml.mjs docs/diagrams`. Reproduce la importación de ChartDB 1.21.0.
* **Visualizar:** `bash scripts/diagrams/chartdb.sh up` → `http://localhost:8080` (o app.chartdb.io).
* **Convenciones:** nombre real de la tabla sin esquema, tipos de Databricks tal cual, `pk` en la llave del grano, `note` con la decisión de cada tabla y columna derivada, `Ref` `>` (muchos a uno) o `-` (linaje del mismo edificio entre capas), `TableGroup` y `headercolor` por capa.
* **En Databricks:** documenta con `COMMENT ON TABLE/COLUMN` y declara PK/FK informativas (`NOT ENFORCED`) en el modelo estrella de gold, al final del notebook después de escribir; el generador las dibuja.
* **Regla de PR:** si un PR crea, renombra o borra tablas o columnas, actualiza el `.dbml` y la imagen en el mismo PR. Sin eso, el revisor lo rechaza.

---

## 7. Instrucciones de Interacción

1. **Directo al código y al hallazgo:** No des explicaciones teóricas largas. El código va en los notebooks `.py` de `src/<capa>/` y se despliega con el bundle; las celdas sueltas para `Shift + Enter` son solo para exploración rápida.
2. **Verificación incorporada:** Todo script de transformación debe terminar con un bloque corto de auditoría (`count()`, conteo por `Estado_Estructural`, verificación de rango de coordenadas `[-76.6, -76.4]` y `[3.3, 3.5]`; el bbox real es lon -76,563 a -76,524 y lat 3,389 a 3,445) para garantizar que no haya geometrías corruptas ni en otro continente.
3. El equipo es **El Equipo** (`el_equipo`). Si aún no se ha elegido la herramienta de mapa (Kepler.gl, QGIS, MapLibre + deck.gl, ArcGIS), pregúntala antes de preparar la exportación.
4. **Nunca inventes datos ni cifras.** Toda cifra de los entregables debe rastrearse a una tabla gold.