# Fuentes externas

## Dataset del hackathon
- **Nombre:** `Datosprediales.json`, análisis satelital del evento entregado por la organización de DataBridge Hack (IEEE Computer Society Colombia).
- **Aportó:** los 5.146 edificios con sus propiedades y geometría (tabla bronze `bronze_el_equipo_hackathon_buildings`).
- **Fecha del dato:** evento del 10 de agosto de 2026.

## IDESC, Infraestructura de Datos Espaciales de Cali (GeoServer WFS)
Servicio: https://ws-idesc.cali.gov.co/geoserver/web/ (catálogo de capas del GeoServer; los datos se descargaron por WFS desde el endpoint `/geoserver/ows`)

| Capa | Qué aportó | Fecha del dato |
| --- | --- | --- |
| `dapm:pdt_dpa_comunas` | Comunas | Acuerdo 0636 de 2026 |
| `dapm:pdt_dpa_barrios_sectores` | Barrios oficiales | Acuerdo 0636 de 2026 (DIVIPOLA): https://idesc.cali.gov.co/download/acuerdos/2026-06-26_acuerdo_0636_divipola.pdf |
| `catastro:cat_bas_terrenos` | Catastro (predios) | Última edición: 2 de septiembre de 2025 |
| `idesc:mc_microzonificacion_sismica` | Microzonificación sísmica | Estudio INGEOMINAS–DAGMA 2005, adoptado por el Decreto 411.0.20.0158 de 2014 |
| `pot_2014:eqp_uco_salud` | Equipamientos de salud | POT 2014 |
| `pot_2014:eqp_uba_seguridad_ciudadana` | Bomberos, Defensa Civil y policía | POT 2014 |
| `educacion:esf_establecimiento_educativo` | Colegios | Actualizada 2023–2024 |
| `dapm:pdt_pmep_encuentro` | Espacio público de encuentro | PMEP 2021–2023 |

## USGS (Servicio Geológico de Estados Unidos)
- **Evento us6000tjl2:** https://earthquake.usgs.gov/earthquakes/eventpage/us6000tjl2. Aportó magnitud, profundidad y epicentro del sismo principal. Fecha del dato: 10 de agosto de 2026.
- **USGS FDSN Event Web Service:** https://earthquake.usgs.gov/fdsnws/event/1/. Aportó el catálogo de réplicas. Fecha del dato: catálogo al 13 de noviembre de 2026.
- **USGS Aftershock Forecast** (producto `oaf` del evento us6000tjl2). Aportó las probabilidades de réplicas por ventana de tiempo y magnitud mínima. Fecha de emisión: versión vigente consultada el 13 de noviembre de 2026.

## Alcaldía de Santiago de Cali
- **"Terremoto de Cali: repositorio oficial de información":** https://www.cali.gov.co/boletines/publicaciones/193607/terremoto-de-cali-repositorio-oficial-de-informacion/. Aportó las cifras oficiales de impacto (fallecidos, lesionados, rescatados, desaparecidos y colapsos totales). Fecha del dato: corte del 16 de octubre de 2026, 3 p. m.

## DANE
- **Códigos DIVIPOLA:** departamento 76 (Valle del Cauca), municipio 001 (Cali). Aportaron la codificación oficial de la división político-administrativa.
