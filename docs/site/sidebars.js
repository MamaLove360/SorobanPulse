// @ts-check
// docs/site/sidebars.js
// One sidebar per top-level section of SUMMARY.md (Issue #1170). Edit
// SUMMARY.md, not this file.

const { parseSummary, toSidebarItems } = require('./summary');

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = Object.fromEntries(
  parseSummary().map((section) => [section.id, toSidebarItems(section.items)]),
);

module.exports = sidebars;
