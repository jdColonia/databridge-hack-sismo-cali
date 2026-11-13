// Validates DBML files the same way ChartDB 1.21.0 imports them.
// ChartDB (src/lib/dbml/dbml-import) removes TableGroup and standalone Note
// blocks, strips table settings such as headercolor, replaces Spanish accents
// and then parses with @dbml/core. A file that passes here imports cleanly.
//
// Usage: node validate_dbml.mjs <file.dbml | directory> [...]
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import pkg from '@dbml/core';

const { Parser } = pkg;

const chartdbPreprocess = (src) =>
  src
    .replace(/TableGroup\s+[^{]*\{[^}]*\}/gs, '')
    .replace(/Note\s+\w+\s*\{[^}]*\}/gs, '')
    .replace(/Table\s+((?:"[^"]+"\."[^"]+")|(?:\w+))\s*\[[^\]]*\]\s*\{/g, 'Table $1 {')
    .replace(/[áàäâ]/g, 'a').replace(/[éèëê]/g, 'e').replace(/[íìïî]/g, 'i')
    .replace(/[óòöô]/g, 'o').replace(/[úùüû]/g, 'u').replace(/ñ/g, 'n')
    .replace(/Á/g, 'A').replace(/É/g, 'E').replace(/Í/g, 'I').replace(/Ó/g, 'O')
    .replace(/Ú/g, 'U').replace(/Ñ/g, 'N');

const files = process.argv.slice(2).flatMap((p) =>
  statSync(p).isDirectory()
    ? readdirSync(p).filter((f) => f.endsWith('.dbml')).map((f) => join(p, f))
    : [p]
);
if (files.length === 0) {
  console.error('No .dbml files found');
  process.exit(1);
}

let failed = 0;
for (const file of files) {
  const src = readFileSync(file, 'utf8');
  for (const [mode, text] of [['dbml', src], ['chartdb', chartdbPreprocess(src)]]) {
    try {
      const db = new Parser().parse(text, 'dbmlv2');
      if (mode === 'chartdb') {
        const tables = db.schemas.flatMap((s) => s.tables).length;
        const refs = db.schemas.flatMap((s) => s.refs).length;
        console.log(`OK  ${file}: ${tables} tablas, ${refs} relaciones (DBML y importacion ChartDB)`);
      }
    } catch (e) {
      failed += 1;
      const diags = e.diags ?? [{ message: e.message }];
      for (const d of diags) {
        const loc = d.location?.start ? `:${d.location.start.line}:${d.location.start.column}` : '';
        console.error(`ERR ${file}${loc} [${mode}] ${d.message}`);
      }
      break;
    }
  }
}
process.exit(failed ? 1 : 0);
