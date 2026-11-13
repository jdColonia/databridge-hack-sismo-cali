#!/usr/bin/env python3
"""Generate DBML data diagrams from docs/data_dictionary.md for ChartDB.

Use it while the tables are still being designed or have not been run in
Databricks yet: the data dictionary is kept in sync with the notebooks, so
the diagrams come from the same source. Once the tables exist in Unity
Catalog, catalog_to_dbml.py (which reads the real catalog) is preferred.

What it reads from each table section (### heading) of the dictionary:
  - the first paragraph, Grano, Llave, Fuente and Notebook -> table note
  - the column table (Columna | Tipo | Descripción [| Origen]) -> columns
  - Llave -> `pk` (one column) or `indexes { (a, b) [pk] }` (composite)
Relationships (medallion lineage and joins) come from
docs/diagrams/relaciones_logicas.json, the same file catalog_to_dbml.py uses.

Views:
  --layers bronze,silver,gold   which layers to include (default: all)
  --keys-only                   keep only key and relationship columns
                                (architecture overview)

Usage:
  python3 scripts/diagrams/dictionary_to_dbml.py --keys-only \
      --out docs/diagrams/medallion_overview.dbml
  python3 scripts/diagrams/dictionary_to_dbml.py --layers silver,gold \
      --out docs/diagrams/silver_gold_model.dbml

Only the Python standard library is required.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

LAYERS = ("bronze", "silver", "gold")
LAYER_COLORS = {"bronze": "#B87333", "silver": "#8A8D91", "gold": "#D4A017"}
LAYER_NOTES = {
    "bronze": "Bronze: dato crudo tal como llega, con metadatos de ingesta",
    "silver": "Silver: limpieza, EPSG:9377 para medidas y cruces espaciales",
    "gold": "Gold: listo para el mapa y las cifras",
}


def q(text: str) -> str:
    """Quote a DBML string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", " ") + "'"


def ident(name: str) -> str:
    return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else f'"{name}"'


def plain(md: str) -> str:
    """Strip Markdown emphasis and code marks for a DBML note."""
    return re.sub(r"\*\*|`", "", md).strip()


def parse_dictionary(path: Path) -> list[dict]:
    tables: list[dict] = []
    cur: dict | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^### `([a-z0-9_]+)`", line)
        if m:
            name = m.group(1)
            cur = {"name": name, "layer": name.split("_")[0], "intro": "", "meta": {},
                   "columns": [], "extra": []}
            tables.append(cur)
            continue
        if line.startswith("## "):
            cur = None
            continue
        if cur is None:
            continue
        m = re.match(r"^- \*\*([^:*]+):\*\*\s*(.*)$", line)
        if m:
            cur["meta"][m.group(1).strip()] = m.group(2).strip()
            continue
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if cells[0] in ("Columna", "---") or set(cells[0]) <= {"-", " "}:
                continue
            col = re.fullmatch(r"`([^`]+)`", cells[0])
            if not col or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col.group(1)):
                cur["extra"].append(plain(f"{cells[0]}: {cells[2]}"))
                continue
            desc = plain(cells[2])
            if len(cells) > 3 and cells[3]:
                desc += f" Origen: {plain(cells[3])}."
            cur["columns"].append({"name": col.group(1), "type": cells[1].lower(), "note": desc})
            continue
        if line.strip() and not cur["intro"] and not line.startswith("-"):
            cur["intro"] = plain(line)
    return [t for t in tables if t["layer"] in LAYERS and t["columns"]]


def key_columns(table: dict) -> list[str]:
    key = table["meta"].get("Llave", "")
    names = {c["name"] for c in table["columns"]}
    # The key is listed before any parenthesis, which holds a remark; a key written
    # whole in parentheses, like (`a`, `b`), is a composite key.
    head = key.split("(")[0]
    keys = re.findall(r"`([^`]+)`", head) or re.findall(r"`([^`]+)`", key)
    return [k for k in keys if k in names]


def build(tables: list[dict], rels: list[dict], keys_only: bool, title: str, commit: str) -> str:
    names = {t["name"] for t in tables}
    rel_cols: dict[str, set[str]] = {}
    refs: list[str] = []
    for r in rels:
        ft, fc = r["from"].split(".")
        tt, tc = r["to"].split(".")
        if ft in names and tt in names:
            rel_cols.setdefault(ft, set()).add(fc)
            rel_cols.setdefault(tt, set()).add(tc)
            note = f" // {r['note']}" if r.get("note") else ""
            refs.append(f"Ref: {ident(ft)}.{ident(fc)} {r.get('type', '-')} {ident(tt)}.{ident(tc)}{note}")

    out = [
        f"// {title}",
        f"// Generado por scripts/diagrams/dictionary_to_dbml.py desde docs/data_dictionary.md (commit {commit}).",
        "// No editar a mano: cambia el diccionario o docs/diagrams/relaciones_logicas.json y regenera.",
        "// Las tablas viven en workspace.<schema>: dev_<usuario> en desarrollo, default en produccion.",
        "// Ninguna relacion es FK declarada en Unity Catalog: son linaje y cruces logicos.",
        "",
        "Project el_equipo_medallion {",
        "  database_type: 'Generic'",
        f"  Note: {q('Databricks Free Edition, Unity Catalog y Delta. Job el_equipo_medallion: setup_volume > bronze (hackathon, IDESC, USGS, manual) > silver (referencias, edificios, microzonas, equipamientos) > gold_map_and_summaries > export_geojson.')}",
        "}",
        "",
    ]
    for t in tables:
        pk = key_columns(t)
        keep = set(pk) | rel_cols.get(t["name"], set())
        cols = [c for c in t["columns"] if not keys_only or c["name"] in keep]
        out.append(f"Table {ident(t['name'])} [headercolor: {LAYER_COLORS[t['layer']]}] {{")
        for c in cols:
            settings = []
            if len(pk) == 1 and c["name"] == pk[0]:
                settings.append("pk")
            if c["note"]:
                settings.append(f"note: {q(c['note'])}")
            typ = ident(c["type"]) if re.fullmatch(r"[a-z_][a-z0-9_]*", c["type"]) else f'"{c["type"]}"'
            out.append(f"  {ident(c['name'])} {typ}" + (f" [{', '.join(settings)}]" if settings else ""))
        if len(pk) > 1 and all(k in {c['name'] for c in cols} for k in pk):
            out.append("  indexes {")
            out.append(f"    ({', '.join(ident(k) for k in pk)}) [pk]")
            out.append("  }")
        parts = [t["intro"]]
        for label in ("Grano", "Llave", "Fuente", "Notebook"):
            if t["meta"].get(label):
                parts.append(f"{label}: {plain(t['meta'][label])}")
        if t["extra"]:
            parts.append("Ademas: " + "; ".join(t["extra"]))
        if keys_only:
            parts.append(f"Vista resumida: {len(cols)} de {len(t['columns'])} columnas; detalle en docs/data_dictionary.md.")
        out.append(f"  Note: {q(' | '.join(p for p in parts if p))}")
        out.append("}")
        out.append("")

    if refs:
        out.append("// Relaciones: '-' uno a uno, '>' muchos a uno (logicas, no FK declaradas).")
        out.extend(refs)
        out.append("")
    for layer in LAYERS:
        members = [t["name"] for t in tables if t["layer"] == layer]
        if members:
            out.append(f"TableGroup {layer} [color: {LAYER_COLORS[layer]}] {{")
            out.extend(f"  {ident(m)}" for m in members)
            out.append(f"  Note: {q(LAYER_NOTES[layer])}")
            out.append("}")
            out.append("")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dictionary", default="docs/data_dictionary.md")
    ap.add_argument("--relaciones", default="docs/diagrams/relaciones_logicas.json")
    ap.add_argument("--layers", default=",".join(LAYERS))
    ap.add_argument("--keys-only", action="store_true")
    ap.add_argument("--title", help="first comment line; by default it describes the view")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    layers = [x.strip() for x in a.layers.split(",") if x.strip()]
    tables = [t for t in parse_dictionary(Path(a.dictionary)) if t["layer"] in layers]
    tables.sort(key=lambda t: LAYERS.index(t["layer"]))  # stable: keeps dictionary order inside a layer
    if not a.title:
        n = len(tables)
        if a.keys_only:
            a.title = f"Vista general de la arquitectura medallion: las {n} tablas con sus llaves y relaciones."
        elif layers == ["bronze"]:
            a.title = "Bronze: esquema crudo de cada fuente (hackathon, IDESC, USGS y manual)."
        else:
            a.title = f"Modelo {' y '.join(layers)}: {n} tablas con columnas completas, llaves y cruces."
    rels = json.loads(Path(a.relaciones).read_text(encoding="utf-8")) if a.relaciones else []
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                                text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        commit = "desconocido"
    Path(a.out).write_text(build(tables, rels, a.keys_only, a.title, commit), encoding="utf-8")
    print(f"Escrito {a.out}: {len(tables)} tablas")


if __name__ == "__main__":
    main()
