# Diccionario de datos

Describe cada tabla de las tres capas: grano, llave, fuente y columnas (nombre, tipo, descripción y origen). Las convenciones de nombres están en [`conventions.md`](conventions.md).

Los nombres reales de las tablas llevan el esquema y el prefijo del equipo (`{catalog}.{schema}.<capa>_el_equipo_<contenido>`). Las descripciones de silver y gold son **idénticas** a los comentarios que cada notebook aplica en Databricks (`COMMENT ON TABLE` y `ALTER COLUMN ... COMMENT`) y que usan el catálogo y Genie; bronze no lleva comentarios: es el dato tal como llega.

## Índice

| Capa | Tabla | Grano |
| --- | --- | --- |
| Bronze | `bronze_el_equipo_hackathon_buildings` | Un feature del GeoJSON (5.146) |
| Bronze | `bronze_el_equipo_idesc_communes` | Una comuna (22) |
| Bronze | `bronze_el_equipo_idesc_neighborhoods` | Un barrio o sector DIVIPOLA (342) |
| Bronze | `bronze_el_equipo_idesc_cadastre_neighborhoods` | Un código catastral de barrio (52) |
| Bronze | `bronze_el_equipo_idesc_cadastre_parcels` | Un registro del catastro |
| Bronze | `bronze_el_equipo_manual_code_catalog` | Un código de un dominio |
| Bronze | `bronze_el_equipo_idesc_seismic_zones` | Un polígono de microzonificación sísmica (16) |
| Bronze | `bronze_el_equipo_idesc_health_facilities`, `_idesc_security_facilities`, `_idesc_schools`, `_idesc_public_spaces` | Un elemento de cada capa de equipamientos |
| Bronze | `bronze_el_equipo_usgs_earthquakes` | Un sismo (principal más réplicas M ≥ 3,5) |
| Bronze | `bronze_el_equipo_usgs_aftershock_forecast` | Una ventana por magnitud mínima del pronóstico (20) |
| Bronze | `bronze_el_equipo_manual_official_impact` | Una métrica oficial de impacto (6) |
| Silver | `silver_el_equipo_communes` | Una comuna |
| Silver | `silver_el_equipo_neighborhoods` | Un barrio o sector oficial (342) |
| Silver | `silver_el_equipo_cadastre_neighborhoods` | Un código catastral de barrio de los edificios (52) |
| Silver | `silver_el_equipo_cadastre_parcels` | Un registro del catastro (pueden repetirse `parcel_id` y `connection_code`) |
| Silver | `silver_el_equipo_code_catalog` | Un código de un dominio |
| Silver | `silver_el_equipo_buildings` | Un edificio (5.146) |
| Silver | `silver_el_equipo_neighborhood_crosswalk` | Un par código catastral → barrio oficial (54) |
| Silver | `silver_el_equipo_seismic_zones` | Un polígono de microzonificación sísmica (16) |
| Silver | `silver_el_equipo_building_seismic_zones` | Un edificio (5.146) |
| Silver | `silver_el_equipo_facilities` | Un equipamiento de Cali |
| Silver | `silver_el_equipo_building_facility_distances` | Un edificio (5.146) |
| Gold | `gold_el_equipo_map_buildings` | Un edificio (5.146) |
| Gold | `gold_el_equipo_map_seismic_zones` | Un polígono de microzonificación que toca el área de los edificios |
| Gold | `gold_el_equipo_map_facilities` | Un equipamiento del área de estudio |
| Gold | `gold_el_equipo_commune_summary` | Una comuna oficial con edificios |
| Gold | `gold_el_equipo_neighborhood_summary` | Un barrio oficial con edificios |
| Gold | `gold_el_equipo_seismic_zone_summary` | Una zona de microzonificación (por nombre) con edificios |

## Bronze

Bronze guarda el dato tal como llega, sin limpiar. Las tablas del IDESC, el USGS y la manual conservan los nombres originales de las columnas y agregan los metadatos de ingesta `_source_url` (de dónde salió) y `_queried_at` (cuándo se consultó); las que pueden leerse de una copia de respaldo agregan además `_load_origin` (`wfs`, `api` o `volume_copy`). `bronze_el_equipo_hackathon_buildings` es la excepción documentada: 21 columnas originales y sin metadatos.

### `bronze_el_equipo_hackathon_buildings`

- **Grano:** un feature del GeoJSON (5.146 filas × 21 columnas).
- **Llave:** `OBJECTID`.
- **Fuente:** `Datosprediales.json` del hackathon, volumen `/Volumes/<catalog>/<raw_schema>/<volume>/`.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `OBJECTID` | bigint | Identificador único del edificio (1 a 5.146) |
| `OBJECTID_1` | bigint | Segundo identificador; 100 % nulo |
| `DEPAPRED` | string | Código del departamento (76 = Valle del Cauca) |
| `MUNIPRED` | string | Código del municipio (001 = Santiago de Cali) |
| `TIPO_AVALU` | string | Tipo de avalúo / zona |
| `SECTOR` | string | Sector catastral |
| `COMUNA` | string | Comuna catastral; puede venir con espacios |
| `BARRIO` | string | Barrio catastral; se repite entre comunas |
| `MANZANA` | string | Manzana catastral |
| `TERRENO` | string | Terreno (lote); puede venir con espacios |
| `COND` | string | Condición de la propiedad |
| `EDIFPRED` | string | Edificio dentro del predio; puede venir con espacios |
| `PREDIO` | string | Predio dentro del terreno |
| `NUMEPRED` | string | Número del predio; puede venir con espacios |
| `NPISOS` | bigint | Número de pisos; 0 en 1.234 edificios (dato faltante) |
| `ID_PREDIO` | string | Identificador del predio en el catastro; nulo o 0 en 3.206 edificios |
| `Indice_Escombros` | double | Índice de escombros satelital |
| `Estado_Estructural` | string | Estado estructural inferido por satélite, en texto |
| `Shape__Area` | double | Área en grados² (no sirve como medida) |
| `Shape__Length` | double | Perímetro en grados (no sirve como medida) |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 (lon, lat) |

### `bronze_el_equipo_idesc_communes`

- **Grano:** Una comuna (22).
- **Llave:** `comcodigo`.
- **Fuente:** IDESC `dapm:pdt_dpa_comunas`.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `comcodigo` | string | Código de la comuna |
| `comnombre` | string | Nombre de la comuna |
| `comacuerdo` | string | Enlace al acuerdo |
| `shape_leng` | double | Perímetro en el CRS original |
| `shape_area` | double | Área en el CRS original |
| `comconsul` | string | Enlace de consulta |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada (o ruta del CSV en el repositorio) |
| `_queried_at` | string | Metadato de ingesta: fecha y hora de la consulta, del `manifest.json` |

### `bronze_el_equipo_idesc_neighborhoods`

- **Grano:** Un barrio o sector DIVIPOLA (342).
- **Llave:** `barcodigo`.
- **Fuente:** IDESC `dapm:pdt_dpa_barrios_sectores` (Acuerdo 0636 de 2026).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `barcodigo` | string | Código del barrio, 4 dígitos |
| `barnombre` | string | Nombre del barrio |
| `barcomuna` | string | Código de la comuna |
| `barcategor` | string | Categoría (barrio, sector...) |
| `barlimites` | string | Descripción de los límites |
| `barmapa` | string | Enlace al mapa del barrio |
| `baracusan` | string | Enlace al acuerdo |
| `shape_leng` | double | Perímetro en el CRS original |
| `shape_area` | double | Área en el CRS original |
| `barconsul` | string | Enlace de consulta |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada (o ruta del CSV en el repositorio) |
| `_queried_at` | string | Metadato de ingesta: fecha y hora de la consulta, del `manifest.json` |

### `bronze_el_equipo_idesc_cadastre_neighborhoods`

- **Grano:** Un código catastral de barrio (52).
- **Llave:** `comuna` + `barrio`.
- **Fuente:** IDESC `catastro:cat_bas_terrenos`.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `idbarrio` | string | Código comuna + barrio |
| `nom_barrio` | string | Nombre catastral del barrio |
| `comuna` | string | Código de la comuna, 2 dígitos |
| `barrio` | string | Código del barrio, 2 dígitos |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada (o ruta del CSV en el repositorio) |
| `_queried_at` | string | Metadato de ingesta: fecha y hora de la consulta, del `manifest.json` |

### `bronze_el_equipo_idesc_cadastre_parcels`

- **Grano:** Un registro del catastro.
- **Llave:** Sin llave única.
- **Fuente:** IDESC `catastro:cat_bas_terrenos`, solo los predios de los edificios.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `id_predio` | bigint | Identificador del predio |
| `idbarrio` | string | Código comuna + barrio |
| `nom_barrio` | string | Nombre catastral del barrio |
| `comuna` | string | Comuna |
| `barrio` | string | Barrio |
| `manzana` | string | Manzana |
| `terreno` | string | Terreno |
| `condicion` | string | Condición de la propiedad |
| `edifpred` | string | Edificio dentro del predio |
| `pisopred` | string | Piso dentro del predio |
| `predio` | string | Predio |
| `npn` | string | Número Predial Nacional |
| `conexion` | string | Código de conexión del terreno |
| `direpred` | string | Dirección |
| `tipo_avalu` | string | Tipo de avalúo |
| `destinacio` | string | Destinación económica |
| `uso_princi` | string | Uso principal |
| `n_construc` | bigint | Número de construcciones |
| `total_pis1` | bigint | Total de pisos |
| `area_cons1` | double | Área construida en m² |
| `last_edite` | string | Fecha de última edición, texto con sufijo Z |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada (o ruta del CSV en el repositorio) |
| `_queried_at` | string | Metadato de ingesta: fecha y hora de la consulta, del `manifest.json` |

### `bronze_el_equipo_manual_code_catalog`

- **Grano:** Un código de un dominio.
- **Llave:** `dominio` + `codigo`.
- **Fuente:** `reference/codigos_catastrales.csv` (curado a mano, no es del IDESC).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `dominio` | string | Campo del dataset al que pertenece el código |
| `codigo` | string | Valor del código |
| `significado` | string | Significado; vacío si no está confirmado |
| `fuente` | string | Fuente que lo respalda |
| `enlace` | string | Enlace a la fuente |
| `estado` | string | Estado de verificación en español |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada (o ruta del CSV en el repositorio) |
| `_queried_at` | string | Metadato de ingesta: fecha y hora de la consulta, del `manifest.json` |

### `bronze_el_equipo_idesc_seismic_zones`

- **Grano:** Un polígono de microzonificación sísmica (16 filas × 25 columnas).
- **Llave:** `feature_id` (`zona_mzsc` se repite).
- **Fuente:** IDESC `idesc:mc_microzonificacion_sismica` (WFS). Conserva las 20 propiedades originales con sus nombres originales; las medidas son `double` y las `*_etl` quedan como texto.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `feature_id` | string | Identificador del feature en el WFS (`zona_mzsc` se repite) |
| `area` | double | Área en el CRS original de la capa (no sirve como medida) |
| `perimeter` | double | Perímetro en el CRS original de la capa |
| `mcooiiain_` | bigint | Columna basura del origen; se descarta en silver |
| `mcooiiain1` | bigint | Columna basura del origen; se descarta en silver |
| `mcooiiai_1` | double | Columna basura del origen; se descarta en silver |
| `mcooiiai_2` | double | Columna basura del origen; se descarta en silver |
| `zona_mzsc` | string | Nombre de la zona de microzonificación (ej. 'Zona 4C'); se repite en varios polígonos |
| `label` | string | Etiqueta de la zona en la capa |
| `am_etc` | double | Espectro ETC, aceleración máxima (g); 0 significa estudio particular |
| `sm_etc` | double | Espectro ETC, aceleración de la meseta (g); 0 significa estudio particular |
| `to_etc` | double | Espectro ETC, periodo t0 (s); 0 significa estudio particular |
| `tc_etc` | double | Espectro ETC, periodo tc (s); 0 significa estudio particular |
| `tl_etc` | double | Espectro ETC, periodo tl (s); 0 significa estudio particular |
| `am_etl` | string | Espectro ETL, aceleración máxima (g), como texto; casi siempre nulo |
| `sm_etl` | string | Espectro ETL, aceleración de la meseta (g), como texto; casi siempre nulo |
| `to_etl` | string | Espectro ETL, periodo t0 (s), como texto; casi siempre nulo |
| `tc_etl` | string | Espectro ETL, periodo tc (s), como texto; casi siempre nulo |
| `tl_etl` | string | Espectro ETL, periodo tl (s), como texto; casi siempre nulo |
| `sucep_licu` | bigint | Susceptibilidad a licuación (1 = susceptible) |
| `corrim_lat` | bigint | Riesgo de corrimiento lateral (1 = hay riesgo) |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 (lon, lat) |
| `_source_url` | string | Metadato de ingesta: URL del WFS consultado |
| `_load_origin` | string | Metadato de ingesta: `wfs` o `volume_copy` |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta (de la copia, su fecha de modificación) |

### `bronze_el_equipo_idesc_health_facilities`

- **Grano:** Un elemento de la capa.
- **Llave:** `feature_id`.
- **Fuente:** IDESC `pot_2014:eqp_uco_salud` (WFS).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `feature_id` | string | Identificador del feature en el WFS |
| `categoria` | string | Categoría del equipamiento de salud (ej. 'Hospitales - Hospitalización') |
| `nombre` | string | Nombre |
| `localizaci` | string | Dirección o localización |
| `nivel` | string | Nivel de atención |
| `propiedad` | string | Propiedad (PUB o PRIV) |
| `(otras propiedades)` | string | El resto de propiedades originales de la capa, con sus nombres originales, todas como texto (la lista exacta la define la capa en cada descarga) |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 (lon, lat) |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta |
| `_load_origin` | string | Metadato de ingesta: `wfs` o `volume_copy` |

### `bronze_el_equipo_idesc_security_facilities`

- **Grano:** Un elemento de la capa.
- **Llave:** `feature_id`.
- **Fuente:** IDESC `pot_2014:eqp_uba_seguridad_ciudadana` (WFS).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `feature_id` | string | Identificador del feature en el WFS |
| `tipo` | string | Tipo (CAI, estación, bomberos, Defensa Civil...) |
| `nombre` | string | Nombre |
| `localizaci` | string | Dirección o localización |
| `propiedad` | string | Propiedad (PUB o PRIV) |
| `(otras propiedades)` | string | El resto de propiedades originales de la capa, con sus nombres originales, todas como texto (la lista exacta la define la capa en cada descarga) |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 (lon, lat) |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta |
| `_load_origin` | string | Metadato de ingesta: `wfs` o `volume_copy` |

### `bronze_el_equipo_idesc_schools`

- **Grano:** Un elemento de la capa.
- **Llave:** `feature_id`.
- **Fuente:** IDESC `educacion:esf_establecimiento_educativo` (WFS).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `feature_id` | string | Identificador del feature en el WFS |
| `eeestado` | string | Estado de la sede (ej. 'Oficial') |
| `eenomsede` | string | Nombre de la sede |
| `eedirecc` | string | Dirección |
| `ieomatrtot` | string | Matrícula total |
| `(otras propiedades)` | string | El resto de propiedades originales de la capa, con sus nombres originales, todas como texto (la lista exacta la define la capa en cada descarga) |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 (lon, lat) |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta |
| `_load_origin` | string | Metadato de ingesta: `wfs` o `volume_copy` |

### `bronze_el_equipo_idesc_public_spaces`

- **Grano:** Un elemento de la capa.
- **Llave:** `feature_id`.
- **Fuente:** IDESC `dapm:pdt_pmep_encuentro` (WFS).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `feature_id` | string | Identificador del feature en el WFS |
| `ecrpotele` | string | Tipo de elemento de espacio público (POT) |
| `ecrnomcom` | string | Nombre común |
| `ecrdirecci` | string | Dirección |
| `(otras propiedades)` | string | El resto de propiedades originales de la capa, con sus nombres originales, todas como texto (la lista exacta la define la capa en cada descarga) |
| `geometry` | string | Geometría GeoJSON como texto, EPSG:4326 (lon, lat) |
| `_source_url` | string | Metadato de ingesta: URL de la capa consultada |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta |
| `_load_origin` | string | Metadato de ingesta: `wfs` o `volume_copy` |

### `bronze_el_equipo_usgs_earthquakes`

- **Grano:** Un sismo (sismo principal más réplicas M ≥ 3,5 dentro del círculo oficial de la secuencia).
- **Llave:** `event_id`.
- **Fuente:** USGS, API FDSN `event/1/query` (evento `us6000tjl2`).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `event_id` | string | Identificador del evento en el USGS |
| `magnitude` | double | Magnitud |
| `magnitude_type` | string | Tipo de magnitud (mww, mb...) |
| `place` | string | Descripción del lugar |
| `event_time` | timestamp | Hora del evento, UTC |
| `depth_km` | double | Profundidad en km |
| `lon` | double | Longitud en grados |
| `lat` | double | Latitud en grados |
| `event_type` | string | Tipo de evento según el USGS |
| `status` | string | Estado de revisión |
| `url` | string | Página del evento en el USGS |
| `is_mainshock` | boolean | Verdadero para el sismo principal `us6000tjl2` |
| `_source_url` | string | Metadato de ingesta: URL de la consulta a la API |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta |
| `_load_origin` | string | Metadato de ingesta: de dónde se leyó el crudo (`wfs` o `volume_copy` para el IDESC; `api` o `volume_copy` para el USGS) |

### `bronze_el_equipo_usgs_aftershock_forecast`

- **Grano:** Una combinación de ventana de tiempo y magnitud mínima del pronóstico (4 ventanas × 5 magnitudes = 20).
- **Llave:** `window_label` + `min_magnitude`.
- **Fuente:** USGS, producto `oaf` del evento `us6000tjl2` (`forecast.json`, modelo ETAS).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `forecast_created_at` | timestamp | Fecha de creación del pronóstico, UTC |
| `advisory_time_frame` | string | Periodo de validez del aviso |
| `window_label` | string | Ventana: 1 Day, 1 Week, 1 Month o 1 Year |
| `window_start_at` | timestamp | Inicio de la ventana, UTC |
| `window_end_at` | timestamp | Fin de la ventana, UTC |
| `min_magnitude` | double | Magnitud mínima de la réplica (3 a 7) |
| `probability` | double | Probabilidad (0 a 1, tal como la publica el USGS) de al menos una réplica de esa magnitud en la ventana |
| `expected_count_median` | bigint | Mediana del número esperado de réplicas |
| `expected_count_p95_min` | bigint | Límite inferior del rango del 95 % del número esperado |
| `expected_count_p95_max` | bigint | Límite superior del rango del 95 % del número esperado |
| `_source_url` | string | Metadato de ingesta: URL del `forecast.json` |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la consulta |
| `_load_origin` | string | Metadato de ingesta: de dónde se leyó el crudo (`wfs` o `volume_copy` para el IDESC; `api` o `volume_copy` para el USGS) |

### `bronze_el_equipo_manual_official_impact`

- **Grano:** Una métrica oficial de impacto (6).
- **Llave:** `metric_code`.
- **Fuente:** `reference/official_impact.csv` (curado a mano: Alcaldía de Santiago de Cali, repositorio oficial, corte 16 oct 2026; la cifra de 308 edificaciones colapsadas atribuida a UNGRD/DANE queda 'por verificar' porque no se encontró la fuente).

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `metric_code` | string | Código de la métrica (deaths, injured, rescued, missing, total_collapses, collapsed_buildings) |
| `metric_name` | string | Nombre de la métrica |
| `value` | double | Valor |
| `unit` | string | Unidad |
| `scope` | string | Alcance geográfico |
| `source_name` | string | Fuente |
| `source_url` | string | Enlace a la fuente; vacío si no se encontró |
| `cutoff_date` | string | Fecha de corte; vacía si no se conoce |
| `verification_status` | string | Estado de verificación en español (`verificada` o `por verificar`) |
| `note` | string | Nota sobre la cifra |
| `_source_url` | string | Metadato de ingesta: ruta del CSV en el repositorio |
| `_queried_at` | timestamp | Metadato de ingesta: fecha y hora (UTC) de la carga |

## Silver

### `silver_el_equipo_communes`

Las 22 comunas de Cali según la DIVIPOLA 2026 (Acuerdo 0636), una fila por comuna, con geometría. Fuente: IDESC.

- **Grano:** Una comuna.
- **Llave:** `commune_code`.
- **Fuente:** IDESC `dapm:pdt_dpa_comunas` vía `bronze_el_equipo_idesc_communes`.
- **Notebook:** `src/silver/silver_references.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `commune_code` | string | Código de la comuna, 2 dígitos con cero a la izquierda (ej. '03'). | IDESC dapm:pdt_dpa_comunas, campo comcodigo |
| `commune_name` | string | Nombre oficial de la comuna (ej. 'Comuna 03'). | IDESC, campo comnombre |
| `agreement_url` | string | Enlace al acuerdo municipal que define la comuna (Acuerdo 0636 de 2026). | IDESC, campo comacuerdo |
| `geom_wkt` | string | Polígono de la comuna en WKT, EPSG:4326 (lon, lat). | geometría de IDESC |

### `silver_el_equipo_neighborhoods`

Los 342 barrios y sectores oficiales de Cali (DIVIPOLA 2026, Acuerdo 0636), una fila por barrio, con su comuna y geometría. Fuente: IDESC.

- **Grano:** Un barrio o sector oficial (342).
- **Llave:** `neighborhood_code`.
- **Fuente:** IDESC `dapm:pdt_dpa_barrios_sectores` (DIVIPOLA 2026) vía `bronze_el_equipo_idesc_neighborhoods`.
- **Notebook:** `src/silver/silver_references.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `neighborhood_code` | string | Código oficial del barrio, 4 dígitos 'CCBB' (comuna + barrio, sin guion, ej. '1931'). | IDESC dapm:pdt_dpa_barrios_sectores, campo barcodigo |
| `neighborhood_name` | string | Nombre oficial del barrio o sector. | IDESC, campo barnombre |
| `neighborhood_category` | string | Categoría del polígono (ej. 'Barrio', 'Sector'). | IDESC, campo barcategor |
| `commune_code` | string | Código de la comuna a la que pertenece el barrio, 2 dígitos. | primeros 2 dígitos de neighborhood_code |
| `commune_name` | string | Nombre oficial de la comuna. | silver_el_equipo_communes por commune_code |
| `agreement_url` | string | Enlace al acuerdo municipal que define el barrio (Acuerdo 0636 de 2026). | IDESC, campo baracusan |
| `geom_wkt` | string | Polígono del barrio en WKT, EPSG:4326 (lon, lat). | geometría de IDESC |

### `silver_el_equipo_cadastre_neighborhoods`

Nombre que el catastro da a cada código de barrio 'CCBB' presente en el dataset de edificios (52 códigos). Auxiliar: sirve para los códigos que la DIVIPOLA 2026 cambió u omitió. Fuente: IDESC catastro.

- **Grano:** Un código catastral de barrio de los edificios (52).
- **Llave:** `cadastral_neighborhood_code`.
- **Fuente:** IDESC `catastro:cat_bas_terrenos` vía `bronze_el_equipo_idesc_cadastre_neighborhoods`.
- **Notebook:** `src/silver/silver_references.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `cadastral_neighborhood_code` | string | Código catastral del barrio, 4 dígitos 'CCBB' (comuna + barrio, sin guion). | IDESC catastro:cat_bas_terrenos, campos comuna y barrio |
| `cadastral_neighborhood_name` | string | Nombre del barrio según el catastro. | IDESC catastro, campo nom_barrio |

### `silver_el_equipo_cadastre_parcels`

Registros del catastro de Cali (terrenos) que corresponden a los edificios del dataset, buscados por id_predio y por conexion. Puede haber varios registros por parcel_id o connection_code (varias unidades por terreno). Fuente: IDESC catastro.

- **Grano:** Un registro del catastro (pueden repetirse `parcel_id` y `connection_code`).
- **Llave:** Sin llave única.
- **Fuente:** IDESC `catastro:cat_bas_terrenos` vía `bronze_el_equipo_idesc_cadastre_parcels`.
- **Notebook:** `src/silver/silver_references.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `parcel_id` | bigint | Identificador del predio en el catastro. | IDESC catastro:cat_bas_terrenos, campo id_predio |
| `national_parcel_number` | string | Número Predial Nacional (30 dígitos). | IDESC catastro, campo npn |
| `connection_code` | string | Código de conexión del terreno: comuna + barrio + manzana + predio (12 dígitos). | IDESC catastro, campo conexion |
| `land_use` | string | Uso principal del predio (ej. 'RESIDENCIAL'). | IDESC catastro, campo uso_princi |
| `economic_destination_code` | string | Código de destinación económica del predio (ej. 'A'). | IDESC catastro, campo destinacio |
| `cadastre_floor_count` | int | Número de pisos según el catastro. | IDESC catastro, campo total_pis1 |
| `cadastre_built_area_m2` | double | Área construida según el catastro, en m². | IDESC catastro, campo area_cons1 |
| `address` | string | Dirección del predio según el catastro. | IDESC catastro, campo direpred |
| `cadastre_edited_date` | date | Fecha de última edición del registro catastral. | IDESC catastro, campo last_edite (primeros 10 caracteres) |
| `construction_count` | int | Número de construcciones registradas en el predio. | IDESC catastro, campo n_construc |

### `silver_el_equipo_code_catalog`

Significado de los códigos catastrales del dataset (DEPAPRED, MUNIPRED, TIPO_AVALU, SECTOR, COND) con su fuente y estado de verificación. Los significados no confirmados quedan vacíos. Fuente: curada a mano (reference/codigos_catastrales.csv).

- **Grano:** Un código de un dominio.
- **Llave:** `domain` + `code`.
- **Fuente:** `reference/codigos_catastrales.csv` vía `bronze_el_equipo_manual_code_catalog`.
- **Notebook:** `src/silver/silver_references.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `domain` | string | Campo del dataset al que pertenece el código (ej. 'TIPO_AVALU'). | reference/codigos_catastrales.csv, columna dominio |
| `code` | string | Valor del código tal como aparece en el dataset (ej. '01'). | CSV, columna codigo |
| `meaning` | string | Significado del código; nulo si no está confirmado en una fuente. | CSV, columna significado |
| `source` | string | Nombre de la fuente que respalda el significado. | CSV, columna fuente |
| `source_url` | string | Enlace a la fuente que respalda el significado. | CSV, columna enlace |
| `verification_status` | string | Estado de verificación: 'verified' o 'pending_verification'. | CSV, columna estado traducida |

### `silver_el_equipo_buildings`

Edificios de Cali afectados por el sismo del 10 de agosto, una fila por edificio (5.146). Datos del hackathon limpios, geometría en EPSG:4326 con medidas en EPSG:9377, barrio y comuna oficiales por ubicación y datos del catastro.

- **Grano:** Un edificio (5.146).
- **Llave:** `building_id`.
- **Fuente:** `bronze_el_equipo_hackathon_buildings` más cruces con `silver_el_equipo_neighborhoods`, `silver_el_equipo_cadastre_neighborhoods` y `silver_el_equipo_cadastre_parcels`.
- **Notebook:** `src/silver/silver_buildings.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `building_id` | bigint | Identificador único del edificio (1 a 5.146). | bronze, OBJECTID |
| `department_code` | string | Código DANE del departamento (76 = Valle del Cauca). | bronze, DEPAPRED |
| `municipality_code` | string | Código DANE del municipio (001 = Santiago de Cali). | bronze, MUNIPRED |
| `zone_type_code` | string | Código del tipo de avalúo / zona (ej. '01' urbana). | bronze, TIPO_AVALU |
| `sector_code` | string | Código del sector catastral. | bronze, SECTOR |
| `cadastral_commune_code` | string | Código catastral de la comuna, 2 dígitos; nulo en 2 edificios. | bronze, COMUNA |
| `cadastral_neighborhood_code` | string | Código catastral del barrio, 4 dígitos 'CCBB' (comuna + barrio, sin guion); puede no coincidir con el oficial de 2026. | bronze, COMUNA + BARRIO |
| `block_code` | string | Código catastral de la manzana, 4 dígitos. | bronze, MANZANA |
| `cadastral_block_code` | string | Código catastral de manzana completo 'CCBBMMMM' (8 dígitos); nulo si falta un componente. | bronze, COMUNA + BARRIO + MANZANA |
| `lot_code` | string | Código del terreno (lote) dentro de la manzana; nulo si venía vacío. | bronze, TERRENO |
| `ownership_condition_code` | string | Código de condición de la propiedad (ej. propiedad horizontal); nulo si venía vacío. | bronze, COND |
| `building_code` | string | Código del edificio dentro del predio; nulo si venía vacío. | bronze, EDIFPRED |
| `parcel_code` | string | Código del predio dentro del terreno. | bronze, PREDIO |
| `parcel_number` | string | Número del predio; nulo si venía vacío. | bronze, NUMEPRED |
| `parcel_id` | bigint | Identificador del predio en el catastro; nulo si venía vacío o en 0. | bronze, ID_PREDIO |
| `floor_count_raw` | string | Número de pisos tal como venía, incluidos los ceros. | bronze, NPISOS |
| `floor_count` | bigint | Número de pisos válido (mayor que 0); nulo si el dato falta. | derivado de NPISOS |
| `is_floor_count_missing` | boolean | Verdadero si no hay número de pisos válido (floor_count nulo). | derivado de NPISOS |
| `debris_index` | double | Índice de escombros satelital. | bronze, Indice_Escombros |
| `structural_status` | string | Estado estructural inferido por satélite, texto original en español. | bronze, Estado_Estructural |
| `structural_status_code` | string | Estado estructural codificado: no_change, roof_loss, debris o no_data. | derivado de structural_status |
| `damage_level` | bigint | Nivel de daño ordinal: 0 no_change, 1 roof_loss, 2 debris; nulo si no hay datos. | derivado de structural_status_code |
| `has_satellite_data` | boolean | Verdadero si el edificio tiene datos satelitales (estado distinto de no_data). | derivado de structural_status_code |
| `area_m2` | double | Área de la huella en m² calculada en EPSG:9377. | geometría reproyectada |
| `perimeter_m` | double | Perímetro de la huella en m calculado en EPSG:9377. | geometría reproyectada |
| `built_area_m2` | double | Área construida estimada en m²: area_m2 por floor_count (1 piso si falta). | derivado de area_m2 y floor_count |
| `is_built_area_estimated` | boolean | Verdadero si built_area_m2 asumió 1 piso por falta de floor_count. | derivado de floor_count |
| `is_geometry_valid` | boolean | Verdadero si la geometría original era válida. | validación con shapely |
| `is_geometry_repaired` | boolean | Verdadero si la geometría original era inválida y se reparó con make_valid. | validación con shapely |
| `is_suspect_polygon` | boolean | Verdadero si el polígono es dudoso: área menor de 5 m², mayor de 50.000 m² o vacío. No se elimina. | derivado de area_m2 |
| `lon` | double | Longitud del centroide en grados, EPSG:4326. | centroide calculado en EPSG:9377 |
| `lat` | double | Latitud del centroide en grados, EPSG:4326. | centroide calculado en EPSG:9377 |
| `geom_wkt` | string | Geometría del edificio en WKT, EPSG:4326 (lon, lat), ya reparada. | bronze, geometry |
| `commune_code` | string | Código oficial de la comuna (2 dígitos) según la ubicación del centroide. | cruce espacial con silver_el_equipo_neighborhoods (DIVIPOLA 2026) |
| `commune_name` | string | Nombre oficial de la comuna según la ubicación. | silver_el_equipo_neighborhoods |
| `neighborhood_code` | string | Código oficial del barrio (4 dígitos 'CCBB') según la ubicación del centroide. | cruce espacial con silver_el_equipo_neighborhoods (DIVIPOLA 2026) |
| `neighborhood_name` | string | Nombre oficial del barrio según la ubicación. | silver_el_equipo_neighborhoods |
| `cadastral_neighborhood_name` | string | Nombre del barrio según el código catastral (DIVIPOLA si el código existe, si no el del catastro); nulo si no hay código. | silver_el_equipo_neighborhoods y silver_el_equipo_cadastre_neighborhoods |
| `is_neighborhood_code_current` | boolean | Verdadero si el código catastral del barrio coincide con el oficial; nulo si falta alguno. | cadastral_neighborhood_code = neighborhood_code |
| `national_parcel_number` | string | Número Predial Nacional (30 dígitos) del catastro; nulo si no hay match. | silver_el_equipo_cadastre_parcels |
| `land_use` | string | Uso principal del predio según el catastro (ej. 'RESIDENCIAL'). | silver_el_equipo_cadastre_parcels |
| `economic_destination_code` | string | Código de destinación económica del predio según el catastro. | silver_el_equipo_cadastre_parcels |
| `cadastre_floor_count` | int | Número de pisos según el catastro. | silver_el_equipo_cadastre_parcels |
| `cadastre_built_area_m2` | double | Área construida según el catastro, en m². | silver_el_equipo_cadastre_parcels |
| `address` | string | Dirección del predio según el catastro. | silver_el_equipo_cadastre_parcels |
| `cadastre_edited_date` | date | Fecha de última edición del registro catastral. | silver_el_equipo_cadastre_parcels |
| `cadastre_match_method` | string | Cómo se unió con el catastro: parcel_id, connection (conexión única), ambiguous_connection (varias unidades, no se adivina) o no_match. | derivado del cruce con silver_el_equipo_cadastre_parcels |

### `silver_el_equipo_neighborhood_crosswalk`

Equivalencia entre el código de barrio catastral de los edificios y el barrio oficial DIVIPOLA 2026 por ubicación, con el número de edificios de cada par. Los building_count suman los edificios con barrio oficial.

- **Grano:** Un par código catastral → barrio oficial (54).
- **Llave:** `cadastral_neighborhood_code` + `neighborhood_code`.
- **Fuente:** `silver_el_equipo_buildings`.
- **Notebook:** `src/silver/silver_neighborhood_crosswalk.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `cadastral_neighborhood_code` | string | Código catastral del barrio, 4 dígitos 'CCBB'; nulo en los edificios sin comuna o barrio en el origen. | silver_el_equipo_buildings |
| `cadastral_neighborhood_name` | string | Nombre del barrio según el código catastral (DIVIPOLA si existe, si no el catastro). | silver_el_equipo_buildings |
| `neighborhood_code` | string | Código oficial del barrio (4 dígitos 'CCBB') donde cae el centroide. | silver_el_equipo_buildings (DIVIPOLA 2026) |
| `neighborhood_name` | string | Nombre oficial del barrio donde cae el centroide. | silver_el_equipo_buildings (DIVIPOLA 2026) |
| `is_same_code` | boolean | Verdadero si el código catastral coincide con el oficial; nulo si no hay código catastral. | cadastral_neighborhood_code = neighborhood_code |
| `building_count` | bigint | Número de edificios del dataset con ese par de códigos. | conteo sobre silver_el_equipo_buildings |

### `silver_el_equipo_seismic_zones`

Zonas de la microzonificación sísmica de Cali, una fila por polígono (16). ETC y ETL son los dos espectros de la microzonificación sísmica (estudio INGEOMINAS–DAGMA 2005, Decreto 411.0.20.0158 de 2014); su significado exacto está por verificar en el decreto. Notación del estudio: am = aceleración máxima (g), sm = aceleración de la meseta espectral (g), t0/tc/tl = periodos (s). Un nombre de zona puede repetirse con distintos riesgos: la llave es seismic_zone_id. Origen: bronze_el_equipo_idesc_seismic_zones (IDESC, capa mc_microzonificacion_sismica).

- **Grano:** Un polígono de microzonificación (16).
- **Llave:** `seismic_zone_id` (el nombre de zona se repite).
- **Fuente:** `bronze_el_equipo_idesc_seismic_zones` (IDESC `idesc:mc_microzonificacion_sismica`).
- **Notebook:** `src/silver/silver_seismic_zones.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `seismic_zone_id` | string | Identificador del polígono (id del feature en el WFS del IDESC); es la llave, el nombre de zona se repite. | bronze, feature_id |
| `seismic_zone_name` | string | Nombre de la zona de microzonificación (ej. 'Zona 4C'); puede repetirse (Zona 6 x2, Cauce Activo o Terra x3). | bronze, zona_mzsc |
| `etc_am_g` | double | Espectro ETC, aceleración máxima (am) en g; nulo si la zona requiere estudio particular. | bronze, am_etc (0 pasa a nulo) |
| `etc_sm_g` | double | Espectro ETC, aceleración de la meseta espectral (sm) en g; nulo si la zona requiere estudio particular. | bronze, sm_etc (0 pasa a nulo) |
| `etc_t0_s` | double | Espectro ETC, periodo t0 en segundos; nulo si la zona requiere estudio particular. | bronze, to_etc (0 pasa a nulo) |
| `etc_tc_s` | double | Espectro ETC, periodo tc en segundos; nulo si la zona requiere estudio particular. | bronze, tc_etc (0 pasa a nulo) |
| `etc_tl_s` | double | Espectro ETC, periodo tl en segundos; nulo si la zona requiere estudio particular. | bronze, tl_etc (0 pasa a nulo) |
| `etl_am_g` | double | Espectro ETL, aceleración máxima (am) en g; solo Zona 4B, 5 y 4C, el resto nulo. | bronze, am_etl (texto a double) |
| `etl_sm_g` | double | Espectro ETL, aceleración de la meseta espectral (sm) en g; solo Zona 4B, 5 y 4C, el resto nulo. | bronze, sm_etl (texto a double) |
| `etl_t0_s` | double | Espectro ETL, periodo t0 en segundos; solo Zona 4B, 5 y 4C, el resto nulo. | bronze, to_etl (texto a double) |
| `etl_tc_s` | double | Espectro ETL, periodo tc en segundos; solo Zona 4B, 5 y 4C, el resto nulo. | bronze, tc_etl (texto a double) |
| `etl_tl_s` | double | Espectro ETL, periodo tl en segundos; solo Zona 4B, 5 y 4C, el resto nulo. | bronze, tl_etl (texto a double) |
| `is_liquefaction_susceptible` | boolean | Verdadero si el suelo de la zona es susceptible a licuación. | bronze, sucep_licu = 1 |
| `has_lateral_spread_risk` | boolean | Verdadero si la zona tiene riesgo de corrimiento lateral. | bronze, corrim_lat = 1 |
| `requires_site_study` | boolean | Verdadero si la zona requiere estudio particular de sitio: todos los valores ETC venían en 0, que no significa aceleración nula. | derivado de los ETC |
| `area_m2` | double | Área del polígono en m² recalculada en EPSG:9377. | geometría reproyectada |
| `geom_wkt` | string | Geometría de la zona en WKT, EPSG:4326 (lon, lat), reparada con make_valid si era inválida. | bronze, geometry |

### `silver_el_equipo_building_seismic_zones`

Zona de microzonificación sísmica de cada edificio, una fila por edificio (5.146). Se asigna la zona con mayor área de intersección con la huella (cruce en EPSG:9377); los edificios que no tocan ninguna zona quedan con nulos. ETC y ETL son los dos espectros de la microzonificación sísmica (estudio INGEOMINAS–DAGMA 2005, Decreto 411.0.20.0158 de 2014); su significado exacto está por verificar en el decreto. Origen: silver_el_equipo_buildings y silver_el_equipo_seismic_zones.

- **Grano:** Un edificio (5.146).
- **Llave:** `building_id`.
- **Fuente:** `silver_el_equipo_buildings` y `silver_el_equipo_seismic_zones` (cruce espacial en EPSG:9377).
- **Notebook:** `src/silver/silver_seismic_zones.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `building_id` | bigint | Identificador del edificio. | silver_el_equipo_buildings |
| `seismic_zone_id` | string | Identificador de la zona elegida (mayor área de intersección; empate, el menor id); nulo si el edificio no toca ninguna zona. | silver_el_equipo_seismic_zones |
| `seismic_zone_name` | string | Nombre de la zona elegida; nulo si no toca ninguna. | silver_el_equipo_seismic_zones |
| `etc_am_g` | double | Espectro ETC, aceleración máxima (am) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. | silver_el_equipo_seismic_zones |
| `etc_sm_g` | double | Espectro ETC, aceleración de la meseta espectral (sm) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. | silver_el_equipo_seismic_zones |
| `etc_tc_s` | double | Espectro ETC, periodo tc en segundos de la zona; nulo si la zona requiere estudio particular o no hay zona. | silver_el_equipo_seismic_zones |
| `is_liquefaction_susceptible` | boolean | Verdadero si la zona elegida es susceptible a licuación; nulo si no hay zona. | silver_el_equipo_seismic_zones |
| `has_lateral_spread_risk` | boolean | Verdadero si la zona elegida tiene riesgo de corrimiento lateral; nulo si no hay zona. | silver_el_equipo_seismic_zones |
| `requires_site_study` | boolean | Verdadero si la zona elegida requiere estudio particular de sitio (sin valores ETC); nulo si no hay zona. | silver_el_equipo_seismic_zones |
| `seismic_zone_overlap_count` | int | Número de polígonos de zona que toca la huella del edificio (0 si ninguno). Mayor que 1 indica un edificio sobre el borde entre zonas. | cruce espacial en EPSG:9377 |
| `seismic_zone_overlap_pct` | double | Porcentaje (0 a 100) de la huella del edificio que cae dentro de la zona elegida; nulo si no hay zona. | área de intersección sobre area_m2, en EPSG:9377 |

### `silver_el_equipo_facilities`

Equipamientos de toda Cali (salud, bomberos y Defensa Civil, policía, sedes educativas y espacio público de encuentro), un equipamiento por fila. Fuente: capas oficiales del IDESC (Alcaldía de Santiago de Cali); las capas pot_2014 (salud y seguridad) no traen fecha, probablemente 2014; las sedes educativas se actualizaron en 2023–2024; el espacio público viene del PMEP 2021–2023. Pueden incluir equipamientos que ya no existen.

- **Grano:** Un equipamiento de Cali (salud, bomberos y Defensa Civil, policía, sedes educativas y espacio público).
- **Llave:** `facility_id`.
- **Fuente:** Las cuatro bronze `bronze_el_equipo_idesc_health_facilities`, `_idesc_security_facilities`, `_idesc_schools` y `_idesc_public_spaces`.
- **Notebook:** `src/silver/silver_facilities.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `facility_id` | string | Identificador del equipamiento, formato '<capa>.<id del WFS>' (ej. 'health.eqp_uco_salud.484'). | bronze idesc, feature_id |
| `source_layer` | string | Capa de origen: health, security, schools o public_spaces. | tabla bronze idesc de la que sale |
| `facility_type_code` | string | Tipo unificado: health, emergency (bomberos y Defensa Civil), police (resto de seguridad ciudadana, incluye tipo nulo), school u open_space. | derivado de la capa y de tipo |
| `facility_subtype` | string | Subtipo original: categoria (salud), tipo (seguridad), eeestado (sedes educativas) o ecrpotele (espacio público); nulo si venía vacío. | bronze idesc |
| `facility_name` | string | Nombre del equipamiento (nombre, eenomsede o ecrnomcom); nulo si venía vacío. | bronze idesc |
| `address` | string | Dirección o localización (localizaci, eedirecc o ecrdirecci); nula si venía vacía. | bronze idesc |
| `is_hospital` | boolean | Verdadero si es de salud con categoría 'Hospitales - Hospitalización'; falso para el resto. | derivado de categoria |
| `care_level` | int | Nivel de atención de salud (1, 2 o 3); nulo si no es de salud o no tiene nivel. | bronze idesc, nivel |
| `is_public` | boolean | Verdadero si es público (propiedad PUB o estado Oficial); falso si es privado (PRIV) o no oficial; nulo si no aplica o no se sabe. | propiedad (salud) o eeestado (colegios) |
| `enrollment_count` | int | Matrícula total de la sede educativa, en estudiantes; nulo si no es colegio. | bronze idesc, ieomatrtot |
| `area_m2` | double | Área de la huella en m² calculada en EPSG:9377; nula para puntos (sedes educativas). | geometría reproyectada |
| `is_in_study_area` | boolean | Verdadero si el equipamiento intersecta el bbox de los edificios ampliado 2.000 m (EPSG:9377). | cruce espacial con silver_el_equipo_buildings |
| `lon` | double | Longitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. | representative_point calculado en EPSG:4326 |
| `lat` | double | Latitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. | representative_point calculado en EPSG:4326 |
| `geom_wkt` | string | Geometría del equipamiento en WKT, EPSG:4326 (lon, lat), ya reparada. | bronze idesc, geometry |

### `silver_el_equipo_building_facility_distances`

Distancia en metros de cada edificio al equipamiento más cercano de cada tipo, una fila por edificio (5.146). Medida en EPSG:9377 desde el centroide del edificio hasta la geometría del equipamiento (0 si está dentro), con todos los equipamientos de Cali. Fuente: capas oficiales del IDESC (Alcaldía de Santiago de Cali); salud y seguridad sin fecha (probablemente 2014), colegios 2023–2024, espacio público PMEP 2021–2023.

- **Grano:** Un edificio (5.146).
- **Llave:** `building_id`.
- **Fuente:** `silver_el_equipo_buildings` y `silver_el_equipo_facilities`.
- **Notebook:** `src/silver/silver_facilities.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `building_id` | bigint | Identificador del edificio. | silver_el_equipo_buildings |
| `nearest_health_m` | double | Distancia en m al equipamiento de salud más cercano (hospital o no). | sjoin_nearest con silver_el_equipo_facilities (tipo health), EPSG:9377 |
| `nearest_hospital_m` | double | Distancia en m al hospital más cercano (salud con is_hospital). | sjoin_nearest con silver_el_equipo_facilities, EPSG:9377 |
| `nearest_emergency_m` | double | Distancia en m a la estación de bomberos o sede de Defensa Civil más cercana. | sjoin_nearest con silver_el_equipo_facilities (tipo emergency), EPSG:9377 |
| `nearest_police_m` | double | Distancia en m al equipamiento de policía más cercano (CAI, estaciones, etc.). | sjoin_nearest con silver_el_equipo_facilities (tipo police), EPSG:9377 |
| `nearest_school_m` | double | Distancia en m a la sede educativa más cercana. | sjoin_nearest con silver_el_equipo_facilities (tipo school), EPSG:9377 |
| `nearest_open_space_m` | double | Distancia en m al espacio público de encuentro más cercano (borde del polígono; 0 si el edificio está dentro). | sjoin_nearest con silver_el_equipo_facilities (tipo open_space), EPSG:9377 |
| `nearest_hospital_name` | string | Nombre del hospital más cercano; nulo si el hospital no tiene nombre. | silver_el_equipo_facilities, facility_name |
| `nearest_emergency_name` | string | Nombre de la estación de bomberos o sede de Defensa Civil más cercana; nulo si no tiene nombre. | silver_el_equipo_facilities, facility_name |
| `nearest_open_space_name` | string | Nombre del espacio público más cercano; nulo si no tiene nombre. | silver_el_equipo_facilities, facility_name |
| `open_space_count_500m` | int | Número de espacios públicos con alguna parte a 500 m o menos del centroide del edificio. | cruce espacial con silver_el_equipo_facilities (tipo open_space), EPSG:9377 |

## Gold

### `gold_el_equipo_map_buildings`

Vista plana para el mapa: una fila por edificio (5.146) con estado estructural, color, comuna y barrio oficiales, área, zona de microzonificación sísmica del IDESC, distancias a equipamientos del IDESC y geometría WKT. Incluye polígonos dudosos con su bandera.

- **Grano:** Un edificio (5.146).
- **Llave:** `building_id`.
- **Fuente:** `silver_el_equipo_buildings`, `silver_el_equipo_building_seismic_zones` y `silver_el_equipo_building_facility_distances` (uniones por `building_id`).
- **Notebook:** `src/gold/gold_map_and_summaries.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `building_id` | bigint | Identificador único del edificio. | silver_el_equipo_buildings |
| `commune_code` | string | Código oficial de la comuna (2 dígitos) según la ubicación. | silver_el_equipo_buildings |
| `commune_name` | string | Nombre oficial de la comuna. | silver_el_equipo_buildings |
| `neighborhood_code` | string | Código oficial del barrio (4 dígitos 'CCBB') según la ubicación. | silver_el_equipo_buildings |
| `neighborhood_name` | string | Nombre oficial del barrio. | silver_el_equipo_buildings |
| `floor_count` | bigint | Número de pisos válido (mayor que 0); nulo si el dato falta. | silver_el_equipo_buildings |
| `area_m2` | double | Área de la huella en m² calculada en EPSG:9377. | silver_el_equipo_buildings |
| `built_area_m2` | double | Área construida estimada en m² (huella por pisos, 1 piso si falta el dato). | silver_el_equipo_buildings |
| `structural_status_code` | string | Estado estructural inferido por satélite: no_change, roof_loss, debris o no_data. | silver_el_equipo_buildings |
| `structural_status` | string | Estado estructural en el texto original en español. | silver_el_equipo_buildings |
| `damage_level` | bigint | Nivel de daño ordinal: 0 no_change, 1 roof_loss, 2 debris; nulo si no hay datos. | silver_el_equipo_buildings |
| `debris_index` | double | Índice de escombros satelital. | silver_el_equipo_buildings |
| `color_hex` | string | Color para el mapa según el estado: azul no_change, naranja roof_loss, rojo debris, gris no_data. | derivado de structural_status_code |
| `is_suspect_polygon` | boolean | Verdadero si el polígono es dudoso (área menor de 5 m², mayor de 50.000 m² o vacío). | silver_el_equipo_buildings |
| `seismic_zone_id` | string | Identificador de la zona de microzonificación elegida (mayor área de intersección con la huella); nulo si el edificio no toca ninguna zona. | silver_el_equipo_building_seismic_zones |
| `seismic_zone_name` | string | Nombre de la zona de microzonificación elegida (ej. 'Zona 4C'); nulo si no toca ninguna. | silver_el_equipo_building_seismic_zones |
| `etc_am_g` | double | Espectro ETC, aceleración máxima (am) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. | silver_el_equipo_building_seismic_zones |
| `etc_sm_g` | double | Espectro ETC, aceleración de la meseta espectral (sm) en g de la zona; nulo si la zona requiere estudio particular o no hay zona. | silver_el_equipo_building_seismic_zones |
| `etc_tc_s` | double | Espectro ETC, periodo tc en segundos de la zona; nulo si la zona requiere estudio particular o no hay zona. | silver_el_equipo_building_seismic_zones |
| `is_liquefaction_susceptible` | boolean | Verdadero si la zona elegida es susceptible a licuación; nulo si no hay zona. | silver_el_equipo_building_seismic_zones |
| `has_lateral_spread_risk` | boolean | Verdadero si la zona elegida tiene riesgo de corrimiento lateral; nulo si no hay zona. | silver_el_equipo_building_seismic_zones |
| `requires_site_study` | boolean | Verdadero si la zona elegida requiere estudio particular de sitio (sin valores ETC); nulo si no hay zona. | silver_el_equipo_building_seismic_zones |
| `seismic_zone_overlap_count` | int | Número de polígonos de zona que toca la huella del edificio (0 si ninguno). Mayor que 1 indica un edificio sobre el borde entre zonas. | silver_el_equipo_building_seismic_zones |
| `seismic_zone_overlap_pct` | double | Porcentaje (0 a 100) de la huella del edificio que cae dentro de la zona elegida; nulo si no hay zona. | silver_el_equipo_building_seismic_zones |
| `nearest_health_m` | double | Distancia en m al equipamiento de salud más cercano (hospital o no). | silver_el_equipo_building_facility_distances |
| `nearest_hospital_m` | double | Distancia en m al hospital más cercano (salud con is_hospital). | silver_el_equipo_building_facility_distances |
| `nearest_emergency_m` | double | Distancia en m a la estación de bomberos o sede de Defensa Civil más cercana. | silver_el_equipo_building_facility_distances |
| `nearest_police_m` | double | Distancia en m al equipamiento de policía más cercano (CAI, estaciones, etc.). | silver_el_equipo_building_facility_distances |
| `nearest_school_m` | double | Distancia en m a la sede educativa más cercana. | silver_el_equipo_building_facility_distances |
| `nearest_open_space_m` | double | Distancia en m al espacio público de encuentro más cercano (borde del polígono; 0 si el edificio está dentro). | silver_el_equipo_building_facility_distances |
| `nearest_hospital_name` | string | Nombre del hospital más cercano; nulo si el hospital no tiene nombre. | silver_el_equipo_building_facility_distances |
| `nearest_emergency_name` | string | Nombre de la estación de bomberos o sede de Defensa Civil más cercana; nulo si no tiene nombre. | silver_el_equipo_building_facility_distances |
| `nearest_open_space_name` | string | Nombre del espacio público más cercano; nulo si no tiene nombre. | silver_el_equipo_building_facility_distances |
| `open_space_count_500m` | int | Número de espacios públicos con alguna parte a 500 m o menos del centroide del edificio. | silver_el_equipo_building_facility_distances |
| `aftershock_priority_code` | string | Prioridad ante réplicas (criterio del equipo, no cifra oficial): high, medium, low o no_data. high = daño (roof_loss o debris) con algún factor agravante; medium = daño sin agravante, o sin cambio sobre suelo que requiere estudio particular o es licuable; low = sin cambio sin agravante de suelo; no_data = sin datos satelitales. | derivado de structural_status_code y los factores agravantes |
| `has_aggravating_factor` | boolean | Verdadero si el edificio tiene algún factor agravante ante réplicas: zona que requiere estudio particular, suelo licuable, hospital a más de 1.000 m o ningún espacio público de encuentro a 500 m. Nunca nulo. | derivado de requires_site_study, is_liquefaction_susceptible, nearest_hospital_m y open_space_count_500m |
| `priority_color_hex` | string | Color para el mapa según aftershock_priority_code: rojo high, naranja medium, verde low, gris no_data. | derivado de aftershock_priority_code |
| `geom_wkt` | string | Geometría del edificio en WKT, EPSG:4326 (lon, lat). | silver_el_equipo_buildings |

### `gold_el_equipo_map_seismic_zones`

Polígonos de microzonificación sísmica del IDESC que tocan el área de los edificios (bbox de los edificios del mapa), con espectros ETC, banderas de suelo, número de edificios asignados y porcentaje de daño. Una fila por polígono; capa de contexto para el mapa.

- **Grano:** Un polígono de microzonificación que toca el bbox de los edificios.
- **Llave:** `seismic_zone_id`.
- **Fuente:** `silver_el_equipo_seismic_zones` (polígonos que intersectan el bbox de los edificios, prueba con shapely) y `gold_el_equipo_map_buildings` (conteo y % de daño por `seismic_zone_id`).
- **Notebook:** `src/gold/gold_map_and_summaries.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `seismic_zone_id` | string | Identificador del polígono de microzonificación (id del feature en el WFS del IDESC). | silver_el_equipo_seismic_zones |
| `seismic_zone_name` | string | Nombre de la zona de microzonificación (ej. 'Zona 4C'); puede repetirse entre polígonos. | silver_el_equipo_seismic_zones |
| `etc_am_g` | double | Espectro ETC, aceleración máxima (am) en g; nulo si la zona requiere estudio particular. | silver_el_equipo_seismic_zones |
| `etc_sm_g` | double | Espectro ETC, aceleración de la meseta espectral (sm) en g; nulo si la zona requiere estudio particular. | silver_el_equipo_seismic_zones |
| `etc_tc_s` | double | Espectro ETC, periodo tc en segundos; nulo si la zona requiere estudio particular. | silver_el_equipo_seismic_zones |
| `requires_site_study` | boolean | Verdadero si la zona requiere estudio particular de sitio (sin valores ETC). | silver_el_equipo_seismic_zones |
| `is_liquefaction_susceptible` | boolean | Verdadero si el suelo de la zona es susceptible a licuación. | silver_el_equipo_seismic_zones |
| `has_lateral_spread_risk` | boolean | Verdadero si la zona tiene riesgo de corrimiento lateral. | silver_el_equipo_seismic_zones |
| `building_count` | bigint | Número de edificios del mapa asignados a este polígono (0 si ninguno). | conteo sobre gold_el_equipo_map_buildings por seismic_zone_id |
| `damaged_building_pct` | double | Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. | derivado de gold_el_equipo_map_buildings |
| `geom_wkt` | string | Geometría de la zona en WKT, EPSG:4326 (lon, lat). | silver_el_equipo_seismic_zones |

### `gold_el_equipo_map_facilities`

Equipamientos del IDESC dentro del área de estudio (bbox de los edificios ampliado 2.000 m): salud, bomberos y Defensa Civil, policía, colegios y espacio público de encuentro. Una fila por equipamiento; capa de contexto para el mapa.

- **Grano:** Un equipamiento dentro del área de estudio (`is_in_study_area`).
- **Llave:** `facility_id`.
- **Fuente:** `silver_el_equipo_facilities` filtrada por `is_in_study_area`.
- **Notebook:** `src/gold/gold_map_and_summaries.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `facility_id` | string | Identificador del equipamiento, formato '<capa>.<id del WFS>'. | silver_el_equipo_facilities |
| `facility_type_code` | string | Tipo unificado: health, emergency, police, school u open_space. | silver_el_equipo_facilities |
| `facility_subtype` | string | Subtipo original (categoría de salud, tipo de seguridad, estado del colegio o tipo de espacio público); nulo si venía vacío. | silver_el_equipo_facilities |
| `facility_name` | string | Nombre del equipamiento; nulo si venía vacío. | silver_el_equipo_facilities |
| `is_hospital` | boolean | Verdadero si es de salud con categoría 'Hospitales - Hospitalización'. | silver_el_equipo_facilities |
| `care_level` | int | Nivel de atención de salud (1, 2 o 3); nulo si no es de salud o no tiene nivel. | silver_el_equipo_facilities |
| `enrollment_count` | int | Matrícula total de la sede educativa, en estudiantes; nulo si no es colegio. | silver_el_equipo_facilities |
| `lon` | double | Longitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. | silver_el_equipo_facilities |
| `lat` | double | Latitud de un punto representativo dentro del equipamiento, en grados, EPSG:4326. | silver_el_equipo_facilities |
| `geom_wkt` | string | Geometría del equipamiento en WKT, EPSG:4326 (lon, lat). | silver_el_equipo_facilities |

### `gold_el_equipo_commune_summary`

Resumen del daño por comuna oficial: conteos y área construida por estado estructural, y porcentajes de daño y de cobertura sin datos. Una fila por comuna con edificios del dataset.

- **Grano:** Una comuna oficial con edificios.
- **Llave:** `commune_code`.
- **Fuente:** `silver_el_equipo_buildings`.
- **Notebook:** `src/gold/gold_map_and_summaries.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `commune_code` | string | Código oficial de la comuna (2 dígitos). | silver_el_equipo_buildings |
| `commune_name` | string | Nombre oficial de la comuna. | silver_el_equipo_buildings |
| `building_count` | bigint | Número de edificios de la comuna. | conteo sobre silver_el_equipo_buildings |
| `no_change_building_count` | bigint | Edificios con estado no_change (sin cambio aparente). | conteo por structural_status_code |
| `roof_loss_building_count` | bigint | Edificios con estado roof_loss (posible pérdida de techo). | conteo por structural_status_code |
| `debris_building_count` | bigint | Edificios con estado debris (alta probabilidad de escombros). | conteo por structural_status_code |
| `no_data_building_count` | bigint | Edificios sin datos satelitales (no_data). | conteo por structural_status_code |
| `built_area_m2` | double | Área construida total en m². | suma de built_area_m2 de silver_el_equipo_buildings |
| `no_change_built_area_m2` | double | Área construida en m² de los edificios no_change. | suma por structural_status_code |
| `roof_loss_built_area_m2` | double | Área construida en m² de los edificios roof_loss. | suma por structural_status_code |
| `debris_built_area_m2` | double | Área construida en m² de los edificios debris. | suma por structural_status_code |
| `no_data_built_area_m2` | double | Área construida en m² de los edificios no_data. | suma por structural_status_code |
| `damaged_building_pct` | double | Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. | derivado de los conteos |
| `damaged_area_pct` | double | Porcentaje (0-100) del área construida con daño (roof_loss + debris) sobre la que tiene dato satelital; nulo si ninguna tiene dato. | derivado de las áreas |
| `no_data_building_pct` | double | Porcentaje (0-100) de edificios sin datos satelitales sobre el total de la comuna. | derivado de los conteos |
| `no_data_area_pct` | double | Porcentaje (0-100) del área construida sin datos satelitales sobre el total de la comuna. | derivado de las áreas |

### `gold_el_equipo_neighborhood_summary`

Resumen del daño por barrio oficial (DIVIPOLA 2026): conteos y área construida por estado estructural, y porcentajes de daño y de cobertura sin datos. Una fila por barrio con edificios del dataset.

- **Grano:** Un barrio oficial con edificios.
- **Llave:** `neighborhood_code`.
- **Fuente:** `silver_el_equipo_buildings`.
- **Notebook:** `src/gold/gold_map_and_summaries.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `commune_code` | string | Código oficial de la comuna del barrio (2 dígitos). | silver_el_equipo_buildings |
| `commune_name` | string | Nombre oficial de la comuna del barrio. | silver_el_equipo_buildings |
| `neighborhood_code` | string | Código oficial del barrio (4 dígitos 'CCBB'). | silver_el_equipo_buildings |
| `neighborhood_name` | string | Nombre oficial del barrio. | silver_el_equipo_buildings |
| `building_count` | bigint | Número de edificios del barrio. | conteo sobre silver_el_equipo_buildings |
| `no_change_building_count` | bigint | Edificios con estado no_change (sin cambio aparente). | conteo por structural_status_code |
| `roof_loss_building_count` | bigint | Edificios con estado roof_loss (posible pérdida de techo). | conteo por structural_status_code |
| `debris_building_count` | bigint | Edificios con estado debris (alta probabilidad de escombros). | conteo por structural_status_code |
| `no_data_building_count` | bigint | Edificios sin datos satelitales (no_data). | conteo por structural_status_code |
| `built_area_m2` | double | Área construida total en m². | suma de built_area_m2 de silver_el_equipo_buildings |
| `no_change_built_area_m2` | double | Área construida en m² de los edificios no_change. | suma por structural_status_code |
| `roof_loss_built_area_m2` | double | Área construida en m² de los edificios roof_loss. | suma por structural_status_code |
| `debris_built_area_m2` | double | Área construida en m² de los edificios debris. | suma por structural_status_code |
| `no_data_built_area_m2` | double | Área construida en m² de los edificios no_data. | suma por structural_status_code |
| `damaged_building_pct` | double | Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. | derivado de los conteos |
| `damaged_area_pct` | double | Porcentaje (0-100) del área construida con daño (roof_loss + debris) sobre la que tiene dato satelital; nulo si ninguna tiene dato. | derivado de las áreas |
| `no_data_building_pct` | double | Porcentaje (0-100) de edificios sin datos satelitales sobre el total del barrio. | derivado de los conteos |
| `no_data_area_pct` | double | Porcentaje (0-100) del área construida sin datos satelitales sobre el total del barrio. | derivado de las áreas |

### `gold_el_equipo_seismic_zone_summary`

Resumen del daño por zona de microzonificación sísmica (agrupado por nombre de zona, que se repite entre polígonos): conteos y área construida por estado estructural, porcentajes de daño y de cobertura sin datos, pisos promedio y medianas de distancia al hospital y al espacio público de encuentro. Una fila por zona con edificios del dataset, más una fila con zona nula si hay edificios que no tocan ninguna zona.

- **Grano:** Una zona de microzonificación (por nombre) con edificios, más una fila con zona nula si hay edificios sin zona.
- **Llave:** `seismic_zone_name`.
- **Fuente:** `gold_el_equipo_map_buildings`.
- **Notebook:** `src/gold/gold_map_and_summaries.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `seismic_zone_name` | string | Nombre de la zona de microzonificación (ej. 'Zona 4C'); nulo para los edificios que no tocan ninguna zona. | silver_el_equipo_building_seismic_zones |
| `etc_am_g` | double | Espectro ETC, aceleración máxima (am) en g de la zona (máximo entre sus polígonos); nulo si la zona requiere estudio particular. | silver_el_equipo_building_seismic_zones |
| `etc_sm_g` | double | Espectro ETC, aceleración de la meseta espectral (sm) en g de la zona (máximo entre sus polígonos); nulo si la zona requiere estudio particular. | silver_el_equipo_building_seismic_zones |
| `etc_tc_s` | double | Espectro ETC, periodo tc en segundos de la zona (máximo entre sus polígonos); nulo si la zona requiere estudio particular. | silver_el_equipo_building_seismic_zones |
| `is_liquefaction_susceptible` | boolean | Verdadero si algún edificio de la zona está en suelo susceptible a licuación. | silver_el_equipo_building_seismic_zones |
| `requires_site_study` | boolean | Verdadero si algún edificio de la zona requiere estudio particular de sitio (sin valores ETC). | silver_el_equipo_building_seismic_zones |
| `building_count` | bigint | Número de edificios de la zona. | conteo sobre silver_el_equipo_buildings |
| `no_change_building_count` | bigint | Edificios con estado no_change (sin cambio aparente). | conteo por structural_status_code |
| `roof_loss_building_count` | bigint | Edificios con estado roof_loss (posible pérdida de techo). | conteo por structural_status_code |
| `debris_building_count` | bigint | Edificios con estado debris (alta probabilidad de escombros). | conteo por structural_status_code |
| `no_data_building_count` | bigint | Edificios sin datos satelitales (no_data). | conteo por structural_status_code |
| `built_area_m2` | double | Área construida total en m². | suma de built_area_m2 de silver_el_equipo_buildings |
| `no_change_built_area_m2` | double | Área construida en m² de los edificios no_change. | suma por structural_status_code |
| `roof_loss_built_area_m2` | double | Área construida en m² de los edificios roof_loss. | suma por structural_status_code |
| `debris_built_area_m2` | double | Área construida en m² de los edificios debris. | suma por structural_status_code |
| `no_data_built_area_m2` | double | Área construida en m² de los edificios no_data. | suma por structural_status_code |
| `damaged_building_pct` | double | Porcentaje (0-100) de edificios con daño (roof_loss + debris) sobre los que tienen dato satelital; nulo si ninguno tiene dato. | derivado de los conteos |
| `damaged_area_pct` | double | Porcentaje (0-100) del área construida con daño (roof_loss + debris) sobre la que tiene dato satelital; nulo si ninguna tiene dato. | derivado de las áreas |
| `no_data_building_pct` | double | Porcentaje (0-100) de edificios sin datos satelitales sobre el total de la zona. | derivado de los conteos |
| `no_data_area_pct` | double | Porcentaje (0-100) del área construida sin datos satelitales sobre el total de la zona. | derivado de las áreas |
| `avg_floor_count` | bigint | Promedio de pisos de los edificios de la zona con número de pisos válido; nulo si ninguno lo tiene. | promedio de floor_count de silver_el_equipo_buildings |
| `median_nearest_hospital_m` | double | Mediana en m de la distancia al hospital más cercano de los edificios de la zona. | mediana de nearest_hospital_m de silver_el_equipo_building_facility_distances |
| `median_nearest_open_space_m` | double | Mediana en m de la distancia al espacio público de encuentro más cercano de los edificios de la zona. | mediana de nearest_open_space_m de silver_el_equipo_building_facility_distances |

## Export

`src/export/export_geojson.py` escribe tres archivos en `export/` del volumen, todos en EPSG:4326 (`[lon, lat]`), con `geom_wkt` convertido en geometría y el resto de columnas como propiedades:

- `gold_el_equipo_map_buildings.geojson`: las filas de `gold_el_equipo_map_buildings` (5.146), con zona sísmica, prioridad ante réplicas (`aftershock_priority_code`, `has_aggravating_factor`, `priority_color_hex`) y las distancias a equipamientos redondeadas a metros enteros.
- `gold_el_equipo_map_seismic_zones.geojson`: las filas de `gold_el_equipo_map_seismic_zones`, con `etc_*` a 2 decimales y `damaged_building_pct` a 1.
- `gold_el_equipo_map_facilities.geojson`: las filas de `gold_el_equipo_map_facilities`.

Cada archivo se valida con su conteo esperado y con coordenadas dentro de lon −76,7 a −76,4 y lat 3,2 a 3,6.
