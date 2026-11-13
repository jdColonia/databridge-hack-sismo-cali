# CI/CD

## CI (`.github/workflows/ci.yml`)

Corre en cada pull request y en cada push a una rama distinta de `main`, o a mano.

- `checks` (sin secretos): compila con `py_compile` los `src/**/*.py` y `scripts/*.py`, revisa la sintaxis de `scripts/*.sh`, valida con `node --check` el JavaScript inline de `map/index.html`, comprueba que maplibre-gl y deck.gl estén con versión exacta y valida los `.dbml` de `docs/diagrams`.
- `bundle-validate`: ejecuta `databricks bundle validate` para `dev` y `team`. Si no hay credenciales configuradas, se omite con un aviso.

## CD (`.github/workflows/cd.yml`)

Corre en cada push a `main` o manualmente (`workflow_dispatch`, con la opción `run_pipeline`).

- Si cambian `databricks.yml`, `resources/**`, `src/**` o `reference/**`: valida, despliega y ejecuta `el_equipo_medallion` en el target `team`.
- Si cambian `map/**`, `scripts/download_map_data.sh` o el propio `cd.yml`, si el pipeline acaba de ejecutarse, o en una ejecución manual: descarga los GeoJSON del volumen, arma el sitio y lo publica en GitHub Pages.

Los datos nunca se versionan: `map/data/` está en `.gitignore` y los GeoJSON se descargan en cada build desde `/Volumes/workspace/default/el_equipo/export/`.

> Advertencia: cada push a `main` que toque archivos del pipeline reescribe las tablas finales en `workspace.default`.

## Configuración requerida

1. Variable de repositorio `DATABRICKS_HOST`: URL del workspace.
2. Secret de repositorio `DATABRICKS_TOKEN`: token personal creado en Databricks › Settings › Developer › Access tokens.
3. GitHub Pages: Settings › Pages › Source = GitHub Actions.

Con `gh` (el valor del token no se escribe en ningún archivo):

```bash
gh variable set DATABRICKS_HOST --body <url-del-workspace>
gh secret set DATABRICKS_TOKEN   # pide el valor de forma interactiva
```

## URL pública

https://jdcolonia.github.io/databridge-hack-sismo-cali/
