// docs/site/scripts/check-nav.js
//
// Fails when a Markdown file under docs/ is not reachable from SUMMARY.md,
// or when SUMMARY.md links to a file that does not exist (Issue #1170).

const fs = require('fs');
const path = require('path');
const { parseSummary, listedFiles, DOCS_DIR } = require('../summary');

// Directories under docs/ that are not documentation pages.
const IGNORED_DIRS = new Set(['site', 'node_modules']);

function walk(dir, rel = '') {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const relPath = rel ? `${rel}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      return IGNORED_DIRS.has(relPath) ? [] : walk(path.join(dir, entry.name), relPath);
    }
    return /\.mdx?$/.test(entry.name) ? [relPath] : [];
  });
}

const listed = parseSummary().flatMap((section) => listedFiles(section.items));
const listedSet = new Set(listed);
const errors = [];

for (const file of walk(DOCS_DIR)) {
  if (!listedSet.has(file)) errors.push(`docs/${file} is not linked from docs/site/SUMMARY.md`);
}
for (const file of listed) {
  if (!fs.existsSync(path.join(DOCS_DIR, file))) errors.push(`SUMMARY.md links to missing docs/${file}`);
}
const seen = new Set();
for (const file of listed) {
  if (seen.has(file)) errors.push(`docs/${file} is linked more than once in SUMMARY.md`);
  seen.add(file);
}

if (errors.length) {
  errors.forEach((e) => console.error(`error: ${e}`));
  process.exit(1);
}
console.log(`All ${listed.length} docs are reachable from the navigation.`);
