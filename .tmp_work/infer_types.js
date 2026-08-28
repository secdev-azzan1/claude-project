const fs = require('fs');
const path = require('path');

function inferType(values) {
  const nonNull = values.filter(v => v !== null && v !== undefined && v !== '');
  if (nonNull.length === 0) return 'string';

  const isBool = v => typeof v === 'boolean' || v === 'true' || v === 'false';
  if (nonNull.every(isBool)) return 'boolean';

  const isIntLike = v => {
    if (typeof v === 'number') return Number.isInteger(v);
    if (typeof v === 'string') return /^-?\d+$/.test(v.trim());
    return false;
  };
  if (nonNull.every(isIntLike)) return 'long';

  const isFloatLike = v => {
    if (typeof v === 'number') return Number.isFinite(v);
    if (typeof v === 'string') return /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(v.trim()) && v.trim() !== '';
    return false;
  };
  const anyNonInteger = nonNull.some(v => {
    if (typeof v === 'number') return !Number.isInteger(v);
    if (typeof v === 'string') return /^-?\d+\.\d+([eE][-+]?\d+)?$/.test(v.trim());
    return false;
  });
  if (nonNull.every(isFloatLike) && anyNonInteger) return 'double';

  return 'string';
}

function loadEntity(prefix, startVals) {
  let columnNames = null;
  const columns = {}; // name -> array of values
  for (const s of startVals) {
    const file = path.join(__dirname, 'fs_samples', `${prefix}_${s}.json`);
    const d = JSON.parse(fs.readFileSync(file));
    if (!columnNames) {
      columnNames = d.columnNames;
      columnNames.forEach(c => columns[c] = []);
    }
    for (const row of d.data) {
      columnNames.forEach((c, i) => columns[c].push(row[i]));
    }
  }
  return { columnNames, columns };
}

const entities = {
  report: [0,500,1000,1500,2000,2500,3000,3500,4000,4500],
  task: [0,500,1000,1500,2000,2500],
  monitor: [0,500,1000,1500,2000,2500,3000,3500,4000,4500],
  event_pulling: [0,500],
};

for (const [name, starts] of Object.entries(entities)) {
  const { columnNames, columns } = loadEntity(name, starts);
  const totalRows = columns[columnNames[0]].length;
  console.log(`\n=== ${name} (sampled ${totalRows} rows) ===`);
  for (const c of columnNames) {
    const vals = columns[c];
    const nonNullCount = vals.filter(v => v !== null && v !== undefined && v !== '').length;
    const t = inferType(vals);
    const distinctSample = [...new Set(vals.filter(v=>v!==null && v!=='').map(v=>JSON.stringify(v)))].slice(0,4);
    console.log(`  ${c}: inferred=${t}  nonNull=${nonNullCount}/${totalRows}  sample=${distinctSample.join(', ')}`);
  }
}
