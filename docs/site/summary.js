// @ts-check
// docs/site/summary.js
//
// Parses docs/site/SUMMARY.md — the canonical site map — into Docusaurus
// sidebars (Issue #1170). Shared by sidebars.js and scripts/check-nav.js.
//
//   ## Section           → one sidebar (and navbar entry)
//   ### Group            → collapsible category inside that sidebar
//   - [Label](../x.md)   → doc item
//     - [Child](../y.md) → nested items turn the parent into a category
//                          whose landing page is the parent doc

const fs = require('fs');
const path = require('path');

const SUMMARY_PATH = path.join(__dirname, 'SUMMARY.md');
const DOCS_DIR = path.join(__dirname, '..');

/** Doc ID Docusaurus assigns to a file under docs/ (path without extension). */
function docIdFor(relPath) {
  return relPath.replace(/\\/g, '/').replace(/\.mdx?$/, '');
}

function slugify(title) {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

/**
 * @returns {{ title: string, id: string, items: any[] }[]}
 */
function parseSummary(source = fs.readFileSync(SUMMARY_PATH, 'utf8')) {
  const sections = [];
  let section = null;
  let group = null;
  // Stack of [indent, items[]] for nested list items.
  let stack = [];

  const target = () => (group ? group.items : section.items);

  for (const line of source.replace(/<!--[\s\S]*?-->/g, '').split('\n')) {
    const h2 = line.match(/^## (.+)$/);
    const h3 = line.match(/^### (.+)$/);
    const li = line.match(/^(\s*)- \[([^\]]+)\]\(([^)]+)\)\s*$/);

    if (h2) {
      section = { title: h2[1].trim(), id: slugify(h2[1]), items: [] };
      sections.push(section);
      group = null;
      stack = [];
    } else if (h3 && section) {
      group = { type: 'category', label: h3[1].trim(), collapsed: true, items: [] };
      section.items.push(group);
      stack = [];
    } else if (li && section) {
      const indent = li[1].length;
      const relPath = path.posix.normalize(path.posix.join('site', li[3])).replace(/^\.\.\//, '');
      const item = { type: 'doc', id: docIdFor(relPath), label: li[2].trim(), file: relPath };

      while (stack.length && stack[stack.length - 1][0] >= indent) stack.pop();
      if (stack.length) {
        const parent = stack[stack.length - 1][1];
        // Promote the parent doc to a category on its first child.
        if (parent.type === 'doc') {
          parent.type = 'category';
          parent.link = { type: 'doc', id: parent.id };
          parent.collapsed = true;
          parent.items = [];
          delete parent.id;
        }
        parent.items.push(item);
      } else {
        target().push(item);
      }
      stack.push([indent, item]);
    }
  }
  return sections;
}

/** Strip the helper `file` field so the result is a valid sidebar config. */
function toSidebarItems(items) {
  return items.map(({ file, ...item }) =>
    item.items ? { ...item, items: toSidebarItems(item.items) } : item,
  );
}

/** Every `file` referenced from SUMMARY.md, relative to docs/. */
function listedFiles(items, out = []) {
  for (const item of items) {
    if (item.file) out.push(item.file);
    if (item.items) listedFiles(item.items, out);
  }
  return out;
}

module.exports = { parseSummary, toSidebarItems, listedFiles, DOCS_DIR };
