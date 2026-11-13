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
| Silver | `silver_el_equipo_communes` | Una comuna |
| Silver | `silver_el_equipo_neighborhoods` | Un barrio o sector oficial (342) |
| Silver | `silver_el_equipo_cadastre_neighborhoods` | Un código catastral de barrio de los edificios (52) |
| Silver | `silver_el_equipo_cadastre_parcels` | Un registro del catastro (pueden repetirse `parcel_id` y `connection_code`) |
| Silver | `silver_el_equipo_code_catalog` | Un código de un dominio |
| Silver | `silver_el_equipo_buildings` | Un edificio (5.146) |
| Silver | `silver_el_equipo_neighborhood_crosswalk` | Un par código catastral → barrio oficial (54) |
| Silver | `silver_el_equipo_osm_facilities` | Un equipamiento de OpenStreetMap |
| Silver | `silver_el_equipo_building_facility_distances` | Un edificio |
| Gold | `gold_el_equipo_map_buildings` | Un edificio (5.146) |
| Gold | `gold_el_equipo_commune_summary` | Una comuna oficial con edificios |
| Gold | `gold_el_equipo_neighborhood_summary` | Un barrio oficial con edificios |

## Bronze

Bronze guarda el dato tal como llega, sin limpiar. Las tablas del IDESC y la manual conservan los nombres originales de las columnas y agregan los metadatos de ingesta `_source_url` (de dónde salió) y `_queried_at` (cuándo se consultó). `bronze_el_equipo_hackathon_buildings` es la excepción documentada: 21 columnas originales y sin metadatos.

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

### `silver_el_equipo_osm_facilities`

Hospitales y clínicas, colegios y estaciones de bomberos de OpenStreetMap dentro del bbox de los edificios (+0,01°), un punto por elemento. Fuente: OpenStreetMap vía Overpass; puede quedar sin crear si Overpass no responde.

- **Grano:** Un equipamiento de OpenStreetMap.
- **Llave:** `osm_id`.
- **Fuente:** OpenStreetMap vía Overpass API (respaldo de las capas del IDESC).
- **Notebook:** `src/silver/silver_osm_facilities.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `osm_id` | bigint | Identificador del elemento en OpenStreetMap, formato 'tipo/id' (ej. 'node/123'). | Overpass API |
| `facility_type` | string | Tipo de equipamiento: hospital (incluye clínicas), school o fire_station. | etiqueta amenity de OpenStreetMap |
| `facility_name` | string | Nombre del equipamiento; nulo si OpenStreetMap no lo trae. | etiqueta name de OpenStreetMap |
| `lon` | double | Longitud del punto en grados, EPSG:4326 (centro si es polígono). | Overpass API |
| `lat` | double | Latitud del punto en grados, EPSG:4326 (centro si es polígono). | Overpass API |
| `geom_wkt` | string | Punto del equipamiento en WKT, EPSG:4326 (lon, lat). | lon y lat |

### `silver_el_equipo_building_facility_distances`

Distancia en metros de cada edificio al hospital, colegio y estación de bomberos más cercanos de OpenStreetMap, una fila por edificio. Calculada desde el centroide en EPSG:9377.

- **Grano:** Un edificio.
- **Llave:** `building_id`.
- **Fuente:** `silver_el_equipo_buildings` y `silver_el_equipo_osm_facilities`.
- **Notebook:** `src/silver/silver_osm_facilities.py`.

| Columna | Tipo | Descripción | Origen |
| --- | --- | --- | --- |
| `building_id` | bigint | Identificador del edificio. | silver_el_equipo_buildings |
| `nearest_hospital_m` | double | Distancia en m al hospital o clínica más cercano, en EPSG:9377; nula si no hay ninguno en el bbox. | sjoin_nearest con silver_el_equipo_osm_facilities |
| `nearest_school_m` | double | Distancia en m al colegio más cercano, en EPSG:9377; nula si no hay ninguno en el bbox. | sjoin_nearest con silver_el_equipo_osm_facilities |
| `nearest_fire_station_m` | double | Distancia en m a la estación de bomberos más cercana, en EPSG:9377; nula si no hay ninguna en el bbox. | sjoin_nearest con silver_el_equipo_osm_facilities |

## Gold

### `gold_el_equipo_map_buildings`

Vista plana para el mapa: una fila por edificio (5.146) con estado estructural, color, comuna y barrio oficiales, área, distancias a equipamientos (si existen) y geometría WKT. Incluye polígonos dudosos con su bandera.

- **Grano:** Un edificio (5.146).
- **Llave:** `building_id`.
- **Fuente:** `silver_el_equipo_buildings` y, si existe, `silver_el_equipo_building_facility_distances`.
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
| `nearest_hospital_m` | double | Distancia en m al hospital o clínica más cercano (OpenStreetMap); solo existe si corrió silver_osm_facilities. | silver_el_equipo_building_facility_distances |
| `nearest_school_m` | double | Distancia en m al colegio más cercano (OpenStreetMap); solo existe si corrió silver_osm_facilities. | silver_el_equipo_building_facility_distances |
| `nearest_fire_station_m` | double | Distancia en m a la estación de bomberos más cercana (OpenStreetMap); solo existe si corrió silver_osm_facilities. | silver_el_equipo_building_facility_distances |
| `geom_wkt` | string | Geometría del edificio en WKT, EPSG:4326 (lon, lat). | silver_el_equipo_buildings |

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

## Export

`export/gold_el_equipo_map_buildings.geojson` en el volumen: las filas de `gold_el_equipo_map_buildings` como GeoJSON en EPSG:4326 (`[lon, lat]`), con `geom_wkt` convertido en geometría. Lo genera `src/export/export_geojson.py`.
