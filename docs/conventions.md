# Convención de nombres

Todo identificador (tablas, columnas, `task_key` del job, notebooks y valores categóricos) va en **inglés**. Los textos (markdown de notebooks, comentarios de tabla y columna en Databricks, documentación) van en **español**.
Los nombres propios oficiales (barrios, comunas, direcciones, textos originales del dato) se dejan tal cual vienen, en español.

El diccionario de cada tabla está en [`data_dictionary.md`](data_dictionary.md).

## Tablas

Formato general: `<layer>_el_equipo_<content>` (la guía del reto exige capa y equipo en el nombre). El esquema y el prefijo salen de los parámetros del bundle (`{catalog}.{schema}.<layer>_{team}_<content>`); nunca se escriben a mano.

| Capa | Patrón | Reglas | Ejemplos |
| --- | --- | --- | --- |
| Bronze | `bronze_el_equipo_<source>_<entity>` | La fuente queda visible en el nombre: `hackathon`, `idesc`, `manual`. Entidad en plural | `bronze_el_equipo_hackathon_buildings`, `bronze_el_equipo_idesc_cadastre_parcels`, `bronze_el_equipo_manual_code_catalog` |
| Silver | `silver_el_equipo_<entity>` | Entidad en plural y **sin fuente**: ya está conformada. Los cruces llevan el nombre de lo que relacionan (`..._crosswalk`, `..._distances`) | `silver_el_equipo_buildings`, `silver_el_equipo_neighborhoods`, `silver_el_equipo_neighborhood_crosswalk` |
| Gold | `gold_el_equipo_dim_<entity>`, `gold_el_equipo_fact_<entity>`, `gold_el_equipo_<purpose>` | Dimensiones y hechos del modelo estrella; las vistas de consumo llevan su propósito | `gold_el_equipo_map_buildings`, `gold_el_equipo_commune_summary`, `gold_el_equipo_neighborhood_summary` |

La entidad principal del proyecto es el **edificio** (`buildings`).

## Columnas

`snake_case`, minúsculas, inglés y ASCII (sin tildes ni eñes). El **sufijo** (o prefijo) dice el tipo de dato:

| Sufijo / prefijo | Tipo | Ejemplo |
| --- | --- | --- |
| `_id` | Llave (long) | `building_id`, `parcel_id` |
| `_code` | Código oficial, **string** con ceros a la izquierda | `commune_code` = `'19'`, `neighborhood_code` = `'1931'` (formato DIVIPOLA, sin guion) |
| `_name` | Nombre legible | `neighborhood_name` |
| `is_` / `has_` | Boolean | `is_suspect_polygon`, `has_satellite_data` |
| `_count` | Entero de conteo | `floor_count`, `building_count` |
| `_pct` | Porcentaje de 0 a 100 | `damaged_area_pct` |
| `_date` / `_at` | `DATE` / `TIMESTAMP` | `cadastre_edited_date`, `_queried_at` |
| Unidad al final | `_m2`, `_m`, `_g`, `_s` | `built_area_m2`, `nearest_hospital_m` |
| `_raw` | Valor original conservado junto al limpio | `floor_count_raw` |
| `_url` | Enlace | `agreement_url` |
| `_wkt` | Geometría en texto WKT | `geom_wkt` |

### Códigos de ubicación

- `commune_code`: 2 dígitos (`'03'`).
- `neighborhood_code`: 4 dígitos `CCBB` (comuna + barrio, **sin guion**), tanto el oficial (DIVIPOLA 2026) como el catastral (`cadastral_neighborhood_code`). Un mismo código de barrio se repite entre comunas, por eso nunca se usa solo el barrio.
- `cadastral_block_code`: `CCBBMMMM` (8 dígitos).
- Los prefijos `cadastral_` y `cadastre_` indican que el dato viene del catastro; sin prefijo es el dato oficial vigente (por ejemplo `neighborhood_code` es el oficial por ubicación; `cadastral_neighborhood_code` es el código catastral del dataset).

## Formatos

- **Vacíos:** cadena vacía o solo espacios pasa a `NULL` (`trim`) en silver.
- **Categorías:** valores en `snake_case` inglés. Ejemplos: `structural_status_code` ∈ {`no_change`, `roof_loss`, `debris`, `no_data`}; `cadastre_match_method` ∈ {`parcel_id`, `connection`, `ambiguous_connection`, `no_match`}; `verification_status` ∈ {`verified`, `pending_verification`}; `facility_type` ∈ {`hospital`, `school`, `fire_station`}.
- **Geometría:** se guarda en EPSG:4326 como `geom_wkt` más `lon` y `lat` del centroide. EPSG:9377 (MAGNA-SIRGAS / Origen Nacional) solo se usa para calcular áreas, perímetros y distancias en metros.
- **Metadatos de ingesta** (solo en bronze de fuentes externas): `_source_url` y `_queried_at`, con guion bajo inicial para distinguirlos del dato.
- **Booleanos:** siempre con `is_` o `has_`; nulo significa "no se puede saber" (por ejemplo `is_neighborhood_code_current` es nulo si el edificio no tiene código catastral).

## Excepción documentada: bronze del hackathon

`bronze_el_equipo_hackathon_buildings` conserva las **21 columnas originales con sus nombres originales** (`OBJECTID`, `COMUNA`, `NPISOS`, `Estado_Estructural`...) y no lleva metadatos de ingesta, porque la guía del reto verifica exactamente 5.146 filas × 21 columnas y bronze guarda el dato tal como llega.
Las bronze de fuentes externas (`idesc`, `manual`) tampoco renombran sus columnas originales, pero sí agregan `_source_url` y `_queried_at`.
La traducción a la convención ocurre en silver (`silver_buildings.py`, diccionario `RENAME`); es el único lugar del código, fuera de bronze, donde aparecen los nombres originales.

## Comentarios en Databricks

Cada tabla silver y gold recibe `COMMENT ON TABLE` y un comentario por **cada** columna (`ALTER TABLE ... ALTER COLUMN ... COMMENT`), aplicados al guardar desde un diccionario `DOCS` dentro del notebook. Son en español, cortos, con unidad y origen (`Origen: ...`), y se mantienen idénticos a [`data_dictionary.md`](data_dictionary.md). Genie y el catálogo los usan para entender las tablas.

## Cómo nombrar algo nuevo

1. **Tabla:** elige la capa; en bronze agrega la fuente (`bronze_el_equipo_<source>_<entity>`); en silver solo la entidad en plural; en gold `dim_`/`fact_` o el propósito.
2. **Columna:** escribe el concepto en inglés en `snake_case` y agrega el sufijo del tipo (`_id`, `_code`, `_name`, `_count`, `_pct`, `_m2`...). Los booleanos empiezan con `is_` o `has_`.
3. **Valor categórico:** `snake_case` en inglés, sin tildes.
4. **Notebook y `task_key`:** el `task_key` es el nombre del notebook sin carpeta y sin extensión (`silver_buildings`). Los nombres no llevan números: el orden lo define el job con `depends_on`.
5. **Documentación:** agrega la tabla a `data_dictionary.md` y sus comentarios al diccionario `DOCS` del notebook.

## Ejemplos

| Bien | Mal | Por qué |
| --- | --- | --- |
| `silver_el_equipo_buildings` | `silver_el_equipo_edificios` | El identificador va en inglés y la entidad principal es el edificio |
| `bronze_el_equipo_idesc_communes` | `bronze_el_equipo_ref_layer` | La fuente (`idesc`) debe verse en bronze; `ref_` no dice de dónde viene |
| `neighborhood_code` = `'1931'` | `barrio_id` = `'19-31'` | Los códigos son string `CCBB` sin guion; `_id` es solo para llaves |
| `is_suspect_polygon` | `poligono_dudoso` | Los booleanos llevan `is_`/`has_` y el nombre va en inglés |
| `damaged_area_pct` | `pct_area_dano` | El tipo va como sufijo, no como prefijo |
| `built_area_m2` | `area_construida` | La unidad va al final |
| `structural_status_code` = `roof_loss` | `estado_estructural` = `techo` | Categorías en `snake_case` inglés |
| `floor_count_raw` junto a `floor_count` | Reemplazar `NPISOS` por el valor limpio | Se conserva el original con `_raw` para no perder información |
