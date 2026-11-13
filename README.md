# DataBridge Hack · Sismo de Cali — El Equipo

## El reto

**Pregunta problema:** ¿Qué afectaciones generó el sismo del 10 de agosto en la ciudad de Cali y qué factores contribuyen a reducir el impacto de las réplicas sobre la población, la infraestructura y los servicios urbanos?

El sismo fue de magnitud 7,4 con epicentro en el Chocó ([USGS `us6000tjl2`](https://earthquake.usgs.gov/earthquakes/eventpage/us6000tjl2)). Los datos base son 5.146 edificios de Cali (comunas 3, 9, 10, 17, 18, 19 y 20) con datos catastrales y un estado estructural inferido por análisis satelital (`Estado_Estructural`). La respuesta se construye en Databricks con arquitectura bronze / silver / gold, se enriquece con fuentes externas y se representa en un mapa.

## Arquitectura (medallion)

Todas las tablas viven en `workspace.default` (configurable) con prefijo `el_equipo`. El job `el_equipo_medallion` las construye en orden.

| Capa   | Tabla                                              | Qué hace                                                                                                                                                                      |
| ------ | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Setup  | volumen `el_equipo`                                | Guarda crudo (`Datosprediales.json`), externos (`externos/`) y exportes (`export/`)                                                                                           |
| Bronze | `bronze_el_equipo_predios`                         | Una fila por feature, 20 propiedades + `geometry` (texto GeoJSON), tal cual llega (21 columnas)                                                                               |
| Silver | `silver_el_equipo_predios`                         | Limpieza (trim, vacíos a null), llaves de barrio/manzana, pisos válidos, reparación de geometría, área/perímetro en EPSG:9377, centroide lon/lat, estado y nivel de daño, WKT |
| Silver | `silver_el_equipo_equipamientos_osm`               | Hospitales/clínicas, colegios y bomberos de OpenStreetMap (Overpass) en el bbox de los predios                                                                                |
| Silver | `silver_el_equipo_predios_distancia_equipamientos` | Distancia en metros de cada predio al hospital, colegio y estación de bomberos más cercanos                                                                                   |
| Gold   | `gold_el_equipo_mapa_edificios`                    | Edificios con color, estado, área, flag de polígono dudoso, distancias (si existen) y WKT                                                                                     |
| Gold   | `gold_el_equipo_resumen_comuna`                    | Conteos, área construida y `pct_edificios_dano`, `pct_area_dano`, `pct_area_sin_datos` por comuna                                                                             |
| Gold   | `gold_el_equipo_resumen_barrio`                    | Lo mismo por `comuna_barrio_id`                                                                                                                                               |
| Export | `export/gold_el_equipo_mapa_edificios.geojson`     | GeoJSON EPSG:4326 para Kepler.gl / QGIS                                                                                                                                       |

Los notebooks están en `src/`, una carpeta por capa (formato fuente `.py`), y el job en `resources/el_equipo_medallion.job.yml` (serverless, sin clusters):

```
src/
├── setup/    setup_volumen.py
├── bronze/   bronze_predios.py              (+ un notebook por fuente externa)
├── silver/   silver_predios.py, silver_datos_adicionales.py
├── gold/     gold_mapa_y_resumen.py         (luego: dim_*.py, fact_*.py, vista del mapa)
└── export/   export_geojson.py
```

Los nombres no llevan números: el orden lo define el job con `depends_on`. Para agregar un notebook, créalo en la carpeta de su capa y agrega su tarea en el job.

## Requisitos

- Databricks CLI ≥ 0.218 (probado con 1.26.0). Instalación: `winget install Databricks.DatabricksCLI` (Windows) o `brew tap databricks/tap && brew install databricks` (macOS). Comprobar con `databricks --version`.
- Ser usuario del workspace del equipo (el dueño te invita).
- Git y acceso a este repositorio.
- `Datosprediales.json` **no** hace falta para trabajar: ya está subido al volumen compartido. Solo quien vuelva a subirlo lo necesita en `data/raw/` (esa carpeta no se sube a git).

## Estado actual

| Paso | Estado |
| ---- | ------ |
| Volumen `workspace.default.el_equipo` con `Datosprediales.json` | Listo |
| `workspace.default.bronze_el_equipo_predios` (5.146 filas × 21 columnas) | Listo |
| Silver, gold y export | Pendiente |

## Cómo funciona el bundle

Un *Databricks Asset Bundle* describe en archivos de texto lo que debe existir en Databricks (notebooks, jobs, parámetros). La CLI lee esos archivos y lo crea o actualiza en el workspace: nada se configura a mano con clics, todo queda en Git.

| Archivo | Qué define |
| ------- | ---------- |
| `databricks.yml` | Nombre del bundle, **variables** (`catalog`, `schema`, `raw_schema`, `team`, `volume`) y **targets** (`dev`, `team`) |
| `resources/el_equipo_medallion.job.yml` | El job: 6 tareas en cadena, cada una corre un notebook con las variables como parámetros |
| `src/<capa>/*.py` | Los notebooks. Arman los nombres de tabla con los parámetros (`{catalog}.{schema}.bronze_{team}_predios`), nunca escritos a mano |
| `scripts/upload_data.sh` | Ayuda para subir el crudo al volumen (no es parte del bundle) |

El job corre en **serverless** (no declara clusters) y ejecuta en orden:

```
setup_volumen → bronze_predios → silver_predios → silver_datos_adicionales → gold_mapa_y_resumen → export_geojson
```

**`deploy` vs `run`:**

- `databricks bundle deploy` sube el código a `/Workspace/Users/<tu_usuario>/.bundle/databridge_hack_el_equipo/<target>/` y crea o actualiza el job. **No toca datos.** Respeta `.gitignore`, así que `data/` nunca se sube. Guarda un registro de lo desplegado, por eso la siguiente vez actualiza el job en lugar de duplicarlo.
- `databricks bundle run` ejecuta el job y es lo que crea las tablas. Para correr solo algunas tareas: `--only silver_predios,gold_mapa_y_resumen`.

> **Importante:** si editas un notebook directamente en la interfaz de Databricks, ese cambio no está en Git y el siguiente `deploy` lo sobrescribe. Edita en el repositorio y despliega; si pruebas algo en la interfaz, cópialo después al `.py`.

## Flujo colaborativo con bundles

Todo el equipo trabaja en **un mismo workspace**: `https://dbc-240be77b-cf4c.cloud.databricks.com` (el dueño de la cuenta invita a los demás). El host no está en el yml: sale del perfil de la CLI `el-equipo`.

| Target | Dónde escribe las tablas | Quién lo corre |
| ------ | ------------------------ | -------------- |
| `dev` (por defecto) | `workspace.dev_<tu_usuario>`: un esquema por integrante, para no pisarse | Cada integrante, para probar su rama |
| `team` | `workspace.default`: las 3 capas finales que van en la captura | Una sola persona, desde `main` |

El crudo `Datosprediales.json` vive en el volumen compartido `workspace.default.el_equipo` y se sube **una sola vez**; los dos targets lo leen de ahí.

**Una sola vez (el dueño del workspace):**
1. Invitar a los integrantes: Settings → Identity and access → Users → Add user.
2. Crear el volumen compartido y subir el crudo: correr una vez el job (la tarea `setup_volumen` crea el volumen) o en SQL `CREATE VOLUME IF NOT EXISTS workspace.default.el_equipo`, y luego:
   ```bash
   DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/upload_data.sh
   ```

**Cada integrante:**
1. Clonar el repositorio y entrar a la carpeta:
   ```bash
   git clone https://github.com/jdColonia/databridge-hack-sismo-cali.git
   cd databridge-hack-sismo-cali
   ```
2. Autenticarse contra el workspace compartido (abre el navegador):
   ```bash
   databricks auth login --host https://dbc-240be77b-cf4c.cloud.databricks.com --profile el-equipo
   ```
3. Validar, desplegar y correr en su esquema de pruebas (`workspace.dev_<tu_usuario>`; el job se llama `[dev tu_usuario] el_equipo_medallion`):
   ```bash
   databricks bundle validate -p el-equipo
   databricks bundle deploy -t dev -p el-equipo
   databricks bundle run el_equipo_medallion -t dev -p el-equipo
   ```

**Versión final (una persona, con `main` actualizado):**
```bash
databricks bundle deploy -t team -p el-equipo
databricks bundle run el_equipo_medallion -t team -p el-equipo
```

En `mode: development` el job y los notebooks quedan en la carpeta de cada usuario con prefijo `[dev usuario]`, así que nadie pisa los recursos de otro.

### Convenciones de ramas

- `main` siempre debe pasar `bundle validate`.
- Cada integrante trabaja su notebook en una rama corta (`feat/silver-pisos`, `fix/gold-pct`) y abre un PR a `main`.
- Los notebooks son `.py` (formato fuente de Databricks) para que el diff sea legible en el PR.
- Commits en inglés con Conventional Commits. No subir datos ni archivos Excel.

## Diagramas de datos (ChartDB)

ChartDB es la herramienta oficial para diagramas de datos. Los diagramas se guardan como DBML en `docs/diagrams/` y se importan en ChartDB para verlos y exportar la imagen. Guía completa: [`docs/diagrams/README.md`](docs/diagrams/README.md).

```bash
npm ci --prefix tools/diagrams                       # una vez
python3 scripts/diagrams/catalog_to_dbml.py --schema dev_<usuario> \
    --relaciones docs/diagrams/relaciones_logicas.json --out docs/diagrams/catalogo_dev.dbml
node tools/diagrams/validate_dbml.mjs docs/diagrams  # valida como lo importa ChartDB
bash scripts/diagrams/chartdb.sh up                  # http://localhost:8080 → PostgreSQL › DBML › Import
```

Todo PR que cambie tablas o columnas actualiza también su `.dbml`.

## Llevar el mapa a Kepler.gl / QGIS

1. Corre el job completo (genera `export/gold_el_equipo_mapa_edificios.geojson`).
2. En Databricks: Catalog > `workspace` > `default` > Volumes > `el_equipo` > `export` > descargar el `.geojson`.
3. **Kepler.gl** (kepler.gl/demo): arrastra el archivo; en Fill Color elige `estado_codigo` (o usa `color_hex`) y en Filters `poligono_dudoso = false` si quieres excluir dudosos.
4. **QGIS:** Capa > Añadir capa vectorial; simboliza por categorías con `estado_codigo` (EPSG:4326).

## Entregables del viernes

Correo antes de las 5:00 p. m. del viernes 13, asunto `databridgehack: El Equipo`.

| Entregable                                                                 | Estado |
| -------------------------------------------------------------------------- | ------ |
| Captura de Catalog con las tablas bronze, silver y gold de `el_equipo`     | [ ]    |
| Decisiones en silver y gold (máx. 200 palabras)                            | [ ]    |
| Respuesta a las dos preguntas (máx. 500 palabras)                          | [ ]    |
| Lista de fuentes externas: nombre, enlace y qué aportó                     | [ ]    |
| 3 diapositivas (PowerPoint o PDF): arquitectura, respuesta con datos, mapa | [ ]    |

## Lectura responsable de los datos

- El estado estructural es una **inferencia satelital**, no una inspección en terreno.
- **"Sin cambio aparente" no equivale a "sin daño"**: el satélite no ve daño interno ni fisuras. La propia etiqueta lo menciona ("Daño interno").
- 435 edificios no tienen datos satelitales: se reportan aparte y se excluyen del denominador de los porcentajes, pero la cobertura debe leerse siempre junto al porcentaje.
- El número de pisos es incompleto (`NPISOS = 0` en 1.234 predios); el área construida con 1 piso asumido está marcada como estimada (`area_construida_estimada`).
- Los polígonos dudosos (área < 5 m² o > 50.000 m²) se conservan con su flag, no se eliminan.
- Priorizar atención con estos datos requiere verificación en campo.
