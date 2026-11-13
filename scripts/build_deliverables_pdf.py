"""Arma el PDF de respuestas para el correo de entrega.

Junta en un solo documento la captura del catálogo (si existe), el Texto 1,
el Texto 2 y la lista de fuentes de docs/deliverables/, y lo imprime a PDF con
Microsoft Edge en modo headless.

Uso:
    python scripts/build_deliverables_pdf.py
Las capturas del catálogo van en docs/deliverables/images/ (catalog*.png o .jpg).
"""
import base64
import html
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "deliverables"
IMAGES = DOCS / "images"
OUT_HTML = DOCS / "entrega_el_equipo.html"
OUT_PDF = DOCS / "entrega_el_equipo.pdf"

EDGE_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "msedge",
    "google-chrome",
    "chromium",
]

QUESTION = (
    "¿Qué afectaciones generó el sismo del 10 de agosto en la ciudad de Cali y qué "
    "factores contribuyen a reducir el impacto de las réplicas sobre la población, "
    "la infraestructura y los servicios urbanos?"
)


def inline(text: str) -> str:
    t = html.escape(text, quote=False)
    t = re.sub(r"`([^`]+)`", r"<code>\1</code>", t)
    t = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", t)
    def link(m):
        url = m.group(1).rstrip(".,;:")
        return f'<a href="{url}">{url}</a>' + m.group(1)[len(url):]
    t = re.sub(r"(https?://[^\s)<|]+)", link, t)
    return t


def md_to_html(md: str, drop_title: bool = True) -> str:
    md = re.sub(r"<!--.*?-->", "", md, flags=re.S)
    lines = md.splitlines()
    out, para, items, table = [], [], [], []

    def flush():
        nonlocal para, items, table
        if para:
            out.append("<p>" + inline(" ".join(para)) + "</p>")
            para = []
        if items:
            out.append("<ul>" + "".join(f"<li>{inline(i)}</li>" for i in items) + "</ul>")
            items = []
        if table:
            rows = [r for r in table if not re.match(r"^\|\s*-", r)]
            cells = [[c.strip() for c in r.strip().strip("|").split("|")] for r in rows]
            head = "".join(f"<th>{inline(c)}</th>" for c in cells[0])
            body = "".join(
                "<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>" for r in cells[1:]
            )
            out.append(f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>")
            table = []

    for line in lines:
        s = line.strip()
        if not s:
            flush()
            continue
        if s.startswith("# "):
            flush()
            if not drop_title:
                out.append(f"<h2>{inline(s[2:])}</h2>")
            continue
        if s.startswith("## "):
            flush()
            out.append(f"<h3>{inline(s[3:])}</h3>")
            continue
        if s.startswith("|"):
            if para or items:
                flush()
            table.append(s)
            continue
        if s.startswith("- "):
            if para or table:
                flush()
            items.append(s[2:])
            continue
        if table or items:
            flush()
        para.append(s)
    flush()
    return "\n".join(out)


def word_count(md: str) -> int:
    t = re.sub(r"<!--.*?-->", "", md, flags=re.S)
    t = "\n".join(l for l in t.splitlines() if not l.startswith("#"))
    return len(re.findall(r"\S+", t))


CSS = """
@page { size: Letter; margin: 18mm 18mm 18mm 18mm; }
* { box-sizing: border-box; }
body { font-family: 'Segoe UI', Inter, Arial, sans-serif; color: #1B2233; font-size: 10.8pt; line-height: 1.5; margin: 0; }
.bar { height: 5px; background: #B3261E; margin-bottom: 14px; }
.kicker { font-size: 8.5pt; letter-spacing: .12em; color: #8A93A3; font-weight: 600; text-transform: uppercase; }
h1 { font-size: 24pt; font-weight: 800; margin: 4px 0 2px; letter-spacing: -.01em; }
.sub { color: #5B6474; font-size: 11pt; margin: 0 0 12px; }
.question { background: #F3F4F7; border-left: 5px solid #B3261E; padding: 10px 14px; margin: 10px 0 6px; font-size: 10.5pt; }
.question b { display: block; font-size: 8.5pt; letter-spacing: .1em; color: #B3261E; text-transform: uppercase; margin-bottom: 3px; }
.toc { color: #5B6474; font-size: 9.5pt; margin: 8px 0 0; }
section { margin-top: 20px; }
section.break { break-before: page; }
h2 { font-size: 15pt; font-weight: 700; margin: 0 0 4px; padding-bottom: 5px; border-bottom: 2.5px solid var(--c, #B3261E); color: var(--c, #B3261E); }
.meta { color: #8A93A3; font-size: 9pt; margin: 2px 0 10px; }
h3 { font-size: 11.5pt; margin: 14px 0 4px; color: #1B2233; }
p { margin: 0 0 8px; text-align: left; }
ul { margin: 0 0 8px 18px; padding: 0; }
li { margin-bottom: 3px; }
code { font-family: Consolas, 'Cascadia Mono', monospace; font-size: 9.3pt; background: #F3F4F7; padding: 0 3px; border-radius: 3px; color: #3B4458; }
a { color: #2B6CB0; text-decoration: none; word-break: break-all; }
table { width: 100%; border-collapse: collapse; margin: 6px 0 10px; font-size: 9.5pt; }
th { text-align: left; background: #F3F4F7; color: #5B6474; font-weight: 600; padding: 6px 8px; border-bottom: 1.5px solid #D9DCE3; }
td { padding: 6px 8px; border-bottom: 1px solid #E6E8EE; vertical-align: top; }
.shot { display: block; border: 1px solid #D9DCE3; border-radius: 6px; width: 100%; height: auto; margin-bottom: 8px; break-inside: avoid; }
.placeholder { border: 2px dashed #B3261E; color: #B3261E; padding: 28px; text-align: center; border-radius: 8px; font-weight: 600; }
.foot { margin-top: 22px; color: #8A93A3; font-size: 8.5pt; border-top: 1px solid #E6E8EE; padding-top: 6px; }
"""


def build() -> None:
    t1 = (DOCS / "texto1_decisiones.md").read_text(encoding="utf-8")
    t2 = (DOCS / "texto2_respuesta.md").read_text(encoding="utf-8")
    src = (DOCS / "fuentes.md").read_text(encoding="utf-8")
    n1, n2 = word_count(t1), word_count(t2)
    if n1 > 200 or n2 > 500:
        sys.exit(f"Límite de palabras superado: Texto 1 = {n1}, Texto 2 = {n2}")

    shots = sorted(p for p in IMAGES.glob("catalog*") if p.suffix.lower() in (".png", ".jpg", ".jpeg"))
    if shots:
        imgs = []
        for p in shots:
            mime = "image/png" if p.suffix.lower() == ".png" else "image/jpeg"
            b64 = base64.b64encode(p.read_bytes()).decode()
            imgs.append(f'<img class="shot" src="data:{mime};base64,{b64}" alt="Catálogo de Unity Catalog">')
        catalog = "\n".join(imgs)
    else:
        catalog = (
            '<div class="placeholder">Falta la captura: guárdala como '
            "docs/deliverables/images/catalog_1.png y vuelve a correr el script.</div>"
        )

    body = f"""
<div class="bar"></div>
<div class="kicker">DataBridge Hack · IEEE Computer Society Colombia · Sismo de Cali</div>
<h1>Entrega de El Equipo</h1>
<p class="sub">Pipeline medallion en Databricks (bronze, silver y gold) y mapa de daño y respuesta ante réplicas sobre 5.146 edificios de Cali.</p>
<div class="question"><b>Pregunta del reto</b>{html.escape(QUESTION)}</div>
<p class="toc">Contenido: 1. Captura del catálogo · 2. Texto 1, decisiones en silver y gold · 3. Texto 2, respuesta a las dos preguntas · 4. Fuentes externas</p>

<section style="--c:#1B2233">
<h2>1. Captura del catálogo</h2>
<p class="meta">Unity Catalog, esquema <code>workspace.default</code>, 33 tablas: 14 <code>bronze_el_equipo_*</code>, 11 <code>silver_el_equipo_*</code> y 8 <code>gold_el_equipo_*</code> (Catalog Explorer, páginas 1 y 2 del listado).</p>
{catalog}
</section>

<section class="break" style="--c:#A15C2B">
<h2>2. Texto 1: decisiones en silver y gold</h2>
<p class="meta">{n1} palabras (máximo 200)</p>
{md_to_html(t1)}
</section>

<section style="--c:#B3261E">
<h2>3. Texto 2: respuesta a las dos preguntas</h2>
<p class="meta">{n2} palabras (máximo 500) · cada cifra indica la tabla gold de donde sale</p>
{md_to_html(t2)}
</section>

<section style="--c:#2B6CB0">
<h2>4. Fuentes externas</h2>
<p class="meta">Nombre oficial, enlace, qué aportó y fecha del dato (no la fecha de descarga).</p>
{md_to_html(src)}
</section>

<div class="foot">El Equipo · DataBridge Hack 2026 · Las cifras salen de las tablas gold en workspace.default; el daño es una inferencia satelital, no una inspección en sitio.</div>
"""
    doc = f'<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Entrega El Equipo</title><style>{CSS}</style></head><body>{body}</body></html>'
    OUT_HTML.write_text(doc, encoding="utf-8")

    edge = next((c for c in EDGE_CANDIDATES if Path(c).exists() or shutil.which(c)), None)
    if not edge:
        sys.exit(f"No se encontró Edge/Chrome. Abre {OUT_HTML} y usa Imprimir > Guardar como PDF.")
    with tempfile.TemporaryDirectory() as profile:
        subprocess.run(
            [edge, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
             f"--user-data-dir={profile}", f"--print-to-pdf={OUT_PDF}", OUT_HTML.as_uri()],
            check=True, timeout=120,
        )
    if not OUT_PDF.exists():
        sys.exit(f"Edge no generó el PDF. Abre {OUT_HTML} y usa Imprimir > Guardar como PDF.")
    print(f"PDF: {OUT_PDF}  (Texto 1: {n1} palabras, Texto 2: {n2} palabras, "
          f"capturas: {len(shots)})")


if __name__ == "__main__":
    build()
