"""Descarga las tablas de referencia oficiales (IDESC, Alcaldia de Cali) para
traducir los codigos del dataset de edificios.

Uso (desde la raiz del repo):
    uv run --no-project --with requests python -I scripts/download_referencias.py

Escribe en data/external/referencias/ (ignorado por git):
    comunas.geojson, barrios_divipola.geojson, catastro_terrenos_predios.json,
    catastro_barrios.json y manifest.json
"""
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw" / "Datosprediales.json"
OUT = ROOT / "data" / "external" / "referencias"

WFS = "https://ws-idesc.cali.gov.co/geoserver/ows"
BASE = {"service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "outputFormat": "application/json"}
TZ = ZoneInfo("America/Bogota")
BATCH = 150
TIMEOUT = 90

TERRENOS_FIELDS = ["id_predio", "idbarrio", "nom_barrio", "comuna", "barrio", "manzana",
                   "terreno", "condicion", "edifpred", "pisopred", "predio", "npn",
                   "conexion", "direpred", "tipo_avalu", "destinacio", "uso_princi",
                   "n_construc", "total_pis1", "area_cons1", "last_edite"]

session = requests.Session()
manifest = {}


def now():
    return datetime.now(TZ).isoformat(timespec="seconds")


def wfs(params, retries=3):
    q = {**BASE, "srsName": "EPSG:4326", **params}
    last = None
    for i in range(retries):
        try:
            # POST: el WAF del IDESC responde 403 a los GET cuyos filtros llevan
            # comillas/OR/IN sobre textos; el mismo filtro por POST si pasa.
            r = session.post(WFS, data=q, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # reintento simple
            last = e
            print(f"  reintento {i + 1}/{retries}: {type(e).__name__} {str(e)[:80]}")
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"WFS fallo tras {retries} intentos: {last}")


def save(name, obj, type_name, n, extra=None):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    manifest[name] = {"url_base": WFS, "typeName": type_name,
                      "fecha_consulta": now(), "registros": n, **(extra or {})}
    print(f"{name}: {n} registros")


def s(v):
    return v.strip() if isinstance(v, str) else v


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main():
    gj = json.loads(RAW.read_text(encoding="utf-8"))
    props = [f["properties"] for f in gj["features"]]

    # 1) Capas completas
    com = wfs({"typeNames": "dapm:pdt_dpa_comunas"})
    save("comunas.geojson", com, "dapm:pdt_dpa_comunas", len(com["features"]))
    bar = wfs({"typeNames": "dapm:pdt_dpa_barrios_sectores"})
    save("barrios_divipola.geojson", bar, "dapm:pdt_dpa_barrios_sectores", len(bar["features"]))

    # 2) Catastro por ID_PREDIO valido
    ids = sorted({int(p["ID_PREDIO"]) for p in props
                  if p.get("ID_PREDIO") not in (None, 0, "0", "")})
    # 3) Edificios sin ID valido: conexion = COMUNA+BARRIO+MANZANA+PREDIO
    conex = set()
    for p in props:
        if p.get("ID_PREDIO") not in (None, 0, "0", ""):
            continue
        parts = [(p.get(k) or "").strip() for k in ("COMUNA", "BARRIO", "MANZANA", "PREDIO")]
        if all(len(x) == n and x.isdigit() for x, n in zip(parts, (2, 2, 4, 4))):
            conex.add("".join(parts))
    conex = sorted(conex)
    print(f"ID_PREDIO validos distintos: {len(ids)}; conexion a consultar: {len(conex)}")

    pn = ",".join(TERRENOS_FIELDS)
    feats, found_ids, found_conex = [], set(), set()
    seen = set()  # evita duplicar un mismo registro devuelto por ambas rutas
    for b in chunks(ids, BATCH):
        r = wfs({"typeNames": "catastro:cat_bas_terrenos", "propertyName": pn,
                 "cql_filter": f"id_predio IN ({','.join(map(str, b))})"})
        for f in r["features"]:
            if f["id"] not in seen:
                seen.add(f["id"]); feats.append(f)
            if f["properties"].get("id_predio") is not None:
                found_ids.add(int(f["properties"]["id_predio"]))
    for b in chunks(conex, BATCH):
        lista = ",".join(f"'{c}'" for c in b)
        r = wfs({"typeNames": "catastro:cat_bas_terrenos", "propertyName": pn,
                 "cql_filter": f"conexion IN ({lista})"})
        for f in r["features"]:
            if f["id"] not in seen:
                seen.add(f["id"]); feats.append(f)
            c = s(f["properties"].get("conexion"))
            if c:
                found_conex.add(c)
    for f in feats:
        f.pop("geometry", None)
        f["geometry"] = None
    fc = {"type": "FeatureCollection", "features": feats}
    save("catastro_terrenos_predios.json", fc, "catastro:cat_bas_terrenos", len(feats),
         {"id_predio_consultados": len(ids), "id_predio_encontrados": len(found_ids),
          "conexion_consultadas": len(conex),
          "conexion_encontradas": len(found_conex & set(conex))})

    # 4) Barrios catastrales (nombre) por cada comuna+barrio distinto
    codes = sorted({(p["COMUNA"].strip() + p["BARRIO"].strip()) for p in props
                    if (p.get("COMUNA") or "").strip() and (p.get("BARRIO") or "").strip()})
    recs = []
    for c in codes:
        r = wfs({"typeNames": "catastro:cat_bas_terrenos", "count": 1,
                 "propertyName": "idbarrio,nom_barrio,comuna,barrio",
                 "cql_filter": f"idbarrio='{c}'"})
        if r["features"]:
            p = r["features"][0]["properties"]
            recs.append({"idbarrio": s(p.get("idbarrio")), "nom_barrio": s(p.get("nom_barrio")),
                         "comuna": s(p.get("comuna")), "barrio": s(p.get("barrio"))})
    fc = {"type": "FeatureCollection",
          "features": [{"type": "Feature", "geometry": None, "properties": x} for x in recs]}
    save("catastro_barrios.json", fc, "catastro:cat_bas_terrenos", len(recs),
         {"codigos_consultados": len(codes)})

    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    m = manifest
    ct = m["catastro_terrenos_predios.json"]
    print("\n=== RESUMEN ===")
    print(f"Comunas: {m['comunas.geojson']['registros']}")
    print(f"Barrios DIVIPOLA: {m['barrios_divipola.geojson']['registros']}")
    print(f"ID_PREDIO consultados: {ct['id_predio_consultados']} / encontrados: {ct['id_predio_encontrados']}")
    print(f"conexion consultadas: {ct['conexion_consultadas']} / encontradas: {ct['conexion_encontradas']}")
    print(f"Registros catastro (total): {ct['registros']}")
    print(f"Barrios catastrales con nombre: {sum(1 for x in recs if x['nom_barrio'])} de {len(codes)}")


if __name__ == "__main__":
    main()
