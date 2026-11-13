# Diagramas de datos con ChartDB

**ChartDB ([chartdb.io](https://chartdb.io/)) es la herramienta oficial del proyecto para diagramas de datos** (tablas, columnas, llaves y relaciones entre capas). Los diagramas de arquitectura o de flujo no son de datos y no entran aquí.

## Principio: el DBML en Git es la fuente de verdad

ChartDB no tiene conector para Databricks: solo importa desde PostgreSQL, MySQL, SQL Server, MariaDB, SQLite, CockroachDB y ClickHouse. Además, en ChartDB Cloud la exportación a DBML y a JSON está reservada a planes pagos, y la versión self-hosted guarda los diagramas solo en el navegador (IndexedDB). Por eso:

1. El diagrama se escribe o se genera como **DBML** en `docs/diagrams/*.dbml` y se versiona en Git.
2. ChartDB se usa para **visualizar, ordenar y exportar la imagen** (PNG o SVG). Lo que se cambie en ChartDB a mano no queda en el repositorio: se lleva al `.dbml`.
3. La imagen exportada se guarda junto al `.dbml` (`docs/diagrams/img/<nombre>.png|svg`).

## Archivos

| Ruta | Qué es |
| ---- | ------ |
| `docs/diagrams/<nombre>.dbml` | Diagramas fuente, uno por vista: `medallion_overview` (las tablas con llaves y relaciones), `silver_gold_model` (modelo analítico con todas las columnas) y `bronze_sources` (esquema crudo de cada fuente) |
| `docs/diagrams/relaciones_logicas.json` | Relaciones de linaje entre capas (bronze → silver → gold) y cruces dentro de cada capa, que no existen como FK en Databricks. Lo usan los dos generadores |
| `docs/diagrams/img/` | Exportes PNG desde ChartDB (escala 4x), enlazados desde el README y para las diapositivas |
| `scripts/diagrams/dictionary_to_dbml.py` | Genera DBML desde `docs/data_dictionary.md` (`--layers`, `--keys-only`): sirve mientras las tablas están en diseño o sin correr |
| `scripts/diagrams/catalog_to_dbml.py` | Genera DBML desde Unity Catalog (Databricks CLI, sin SQL warehouse) |
| `scripts/diagrams/chartdb.sh` | Levanta ChartDB local en Docker (`up`, `down`, `status`) |
| `tools/diagrams/` | Validador: reproduce la importación de ChartDB 1.21.0 con `@dbml/core` 3.14.1 (la versión que usa ChartDB) |

## Configuración (una vez)

Requisitos: Node 18 o superior, Docker (opcional, para ChartDB local), Databricks CLI con el perfil `el-equipo`.

```bash
npm ci --prefix tools/diagrams          # instala @dbml/core y @dbml/cli fijados en 3.14.1
bash scripts/diagrams/chartdb.sh up     # ChartDB 1.21.0 en http://localhost:8080, sin telemetría
```

Alternativa sin Docker: [app.chartdb.io](https://app.chartdb.io) (la importación DBML funciona sin plan pago). No subas al cloud nada que no deba salir del equipo.

## Flujo de trabajo

1. **Generar o editar el DBML.**
   - Desde lo que ya existe en Databricks (preferido cuando las tablas están corridas):
     ```bash
     python3 scripts/diagrams/catalog_to_dbml.py --schema dev_<usuario> \
         --relaciones docs/diagrams/relaciones_logicas.json \
         --out docs/diagrams/catalogo_dev.dbml
     ```
     Usa `--schema default` solo para documentar producción.
   - Desde el diccionario de datos cuando la tabla todavía no se ha corrido (diseño). Así se generan los diagramas actuales:
     ```bash
     python3 scripts/diagrams/dictionary_to_dbml.py --keys-only --out docs/diagrams/medallion_overview.dbml
     python3 scripts/diagrams/dictionary_to_dbml.py --layers silver,gold --out docs/diagrams/silver_gold_model.dbml
     python3 scripts/diagrams/dictionary_to_dbml.py --layers bronze --out docs/diagrams/bronze_sources.dbml
     ```
     Una relación nueva entre tablas se agrega en `relaciones_logicas.json` (`from`, `to`, `type` `-` o `>`, `note`).
2. **Validar** (falla con línea y columna si hay error):
   ```bash
   node tools/diagrams/validate_dbml.mjs docs/diagrams
   ```
3. **Importar en ChartDB** (flujo verificado en 1.21.0):
   - Diagrama nuevo: en la pantalla *What is your Database?* elige **PostgreSQL** (ChartDB no ofrece Databricks ni *Generic*; los tipos de Databricks como `string` se conservan igual), pestaña **DBML**, pega el contenido en *DBML Script* (el editor de la derecha; el de la izquierda es un ejemplo) y pulsa **Import**.
   - Diagrama abierto: *Actions › Import › DBML*. Reimportar reemplaza las tablas con igual nombre.
4. **Ordenar y exportar:**
   - Ubica las tablas de izquierda a derecha (bronze → silver → gold) y crea un *Area* por capa (bronce `#B87333`, plata `#8A8D91`, oro `#D4A017`): ChartDB no importa los `TableGroup`. Evita *Auto Arrange* (barra inferior): coloca las capas de derecha a izquierda y superpone tablas.
   - Expande las tablas (*Show More*) para que salgan todas las columnas.
   - *Actions › Export as › PNG* (escala 4x) y *SVG*. Guarda en `docs/diagrams/img/<nombre>.png|svg`.
5. **PR:** el `.dbml` y la imagen van en el mismo PR que cambia el esquema.

## Convenciones del DBML

- Un `Table` por tabla Delta, con el nombre real (`<capa>_el_equipo_<contenido>`), sin esquema delante: el esquema cambia entre `dev_<usuario>` y `default`.
- Tipos de Databricks tal cual (`bigint`, `string`, `double`, `boolean`, `timestamp`). ChartDB los conserva.
- `pk` en la llave del grano (`OBJECTID` para tablas por edificio). `not null` solo si la columna lo es en Databricks.
- `note` en cada tabla con la decisión de la capa (sirve para la diapositiva de arquitectura) y en las columnas derivadas (cómo se calculó, CRS, banderas).
- `Ref` para relaciones: `>` muchos a uno, `-` uno a uno (linaje del mismo edificio entre capas). Comentar con `//` si la relación es lógica y no una FK declarada.
- `TableGroup` por capa (`bronze`, `silver`, `gold`) y `headercolor` por capa (bronce `#B87333`, plata `#8A8D91`, oro `#D4A017`). ChartDB los ignora al importar; dbdiagram y otros visores sí los usan.

**Límites verificados en el código de ChartDB 1.21.0** (`src/lib/dbml/dbml-import`): al importar quita los bloques `TableGroup` y `Note nombre { }`, quita los ajustes de tabla como `headercolor` y reemplaza tildes y eñes (á → a, ñ → n). Las notas de tabla y de columna sí se conservan. El validador aplica estas mismas transformaciones.

## Buenas prácticas en Databricks (para que el diagrama salga del catálogo)

- **Comentarios:** documenta tablas y columnas en Unity Catalog; el generador los convierte en `note`.
  ```sql
  COMMENT ON TABLE workspace.dev_x.silver_el_equipo_buildings IS 'Edificios limpios, EPSG:9377 para medidas';
  COMMENT ON COLUMN workspace.dev_x.silver_el_equipo_buildings.area_m2 IS 'Área de la huella en m², EPSG:9377';
  ```
- **Llaves informativas** (Unity Catalog, Runtime 13.3 LTS o superior): PRIMARY KEY y FOREIGN KEY **no se aplican** (son informativas), pero documentan el modelo y el generador las dibuja. La columna de la PK debe ser `NOT NULL`. Para el modelo estrella de gold:
  ```sql
  ALTER TABLE gold_el_equipo_fact_edificio ALTER COLUMN OBJECTID SET NOT NULL;
  ALTER TABLE gold_el_equipo_fact_edificio ADD CONSTRAINT fact_edificio_pk PRIMARY KEY (OBJECTID) NOT ENFORCED RELY;
  ALTER TABLE gold_el_equipo_fact_edificio ADD CONSTRAINT fact_edificio_estado_fk
    FOREIGN KEY (estado_codigo) REFERENCES gold_el_equipo_dim_estado_estructural NOT ENFORCED;
  ```
  Los notebooks reescriben las tablas con `overwrite`; agrega las restricciones al final del notebook, después de escribir, y verifica con `databricks tables get` que sigan ahí. `RELY` solo si el notebook garantiza la unicidad con un `assert`.
- **Metadatos para diagramas a mano:** `information_schema.columns`, `information_schema.table_constraints`, `information_schema.key_column_usage` y `information_schema.referential_constraints` (filtrar por `table_schema`).

## Cuándo actualizar el diagrama

- Cada PR que crea, renombra o elimina una tabla o columna de `bronze`, `silver` o `gold`.
- Antes de la entrega: la diapositiva 1 (arquitectura) usa la imagen exportada de ChartDB.
- El revisor rechaza el PR si el esquema cambió y el `.dbml` no.

## Referencias

- ChartDB: [repositorio](https://github.com/chartdb/chartdb), [documentación](https://docs.chartdb.io/), [importar DBML](https://docs.chartdb.io/docs/import/dbml.md), [self-hosted vs cloud](https://docs.chartdb.io/docs/cloud-vs-self-hosted.md)
- DBML: [sintaxis](https://dbml.dbdiagram.io/docs/), [CLI](https://dbml.dbdiagram.io/cli), [@dbml/core](https://dbml.dbdiagram.io/js-module/core)
- Databricks: [restricciones](https://docs.databricks.com/aws/en/tables/constraints), [CONSTRAINT en CREATE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-constraint), [COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment), [information_schema](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema)
