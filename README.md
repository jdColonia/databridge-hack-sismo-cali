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

Los notebooks están en `src/` (formato fuente `.py`) y el job en `resources/el_equipo_medallion.job.yml` (serverless, sin clusters).

## Requisitos

- Databricks CLI ≥ 0.218 (probado con 1.26.0).
- Una cuenta de Databricks Free Edition (solo serverless; catálogo `workspace`, esquema `default`).
- Los datos `Datosprediales.json` en `data/raw/` (no se suben a git).

## Flujo colaborativo con bundles

Cada integrante despliega en su propia cuenta (target `dev`). El host no está en el yml: sale del perfil de la CLI.

1. Autenticar (una vez por persona):
   ```bash
   databricks auth login --host https://<tu-workspace>.cloud.databricks.com --profile el-equipo
   ```
2. Validar el bundle:
   ```bash
   databricks bundle validate -p el-equipo
   ```
3. Desplegar en tu cuenta:
   ```bash
   databricks bundle deploy -t dev -p el-equipo
   ```
4. Crear el volumen y subir los datos (el volumen debe existir antes; se crea con la tarea `setup_volumen` o con `CREATE VOLUME IF NOT EXISTS workspace.default.el_equipo`):
   ```bash
   DATABRICKS_CONFIG_PROFILE=el-equipo bash scripts/upload_data.sh
   ```
5. Ejecutar el job:
   ```bash
   databricks bundle run el_equipo_medallion -t dev -p el-equipo
   ```

Nota: en `mode: development` los recursos se prefijan con `[dev tu_usuario]`; las tablas llevan el nombre fijo `el_equipo`, así que dos personas en la misma cuenta se pisarían (por eso cada quien usa su cuenta).

**Target `team`:** la cuenta elegida donde quedan las 3 capas definitivas. Se usa igual pero con `-t team` (`bundle deploy -t team -p el-equipo`). Para dar acceso a más integrantes, agregar `permissions` en `databricks.yml` (hay un comentario en el target).

### Convenciones de ramas

- `main` siempre debe pasar `bundle validate`.
- Cada integrante trabaja su notebook en una rama corta (`feat/silver-pisos`, `fix/gold-pct`) y abre un PR a `main`.
- Los notebooks son `.py` (formato fuente de Databricks) para que el diff sea legible en el PR.
- Commits en inglés con Conventional Commits. No subir datos ni archivos Excel.

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
