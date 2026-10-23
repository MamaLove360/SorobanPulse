// @ts-check
// docs/site/docusaurus.config.js
//
// SorobanPulse documentation site — Docusaurus 3 configuration.
// Design tokens are in docs/design-tokens.md; CSS overrides in src/css/custom.css.

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'SorobanPulse',
  tagline: 'Real-time Soroban smart contract event indexing for the Stellar network',
  favicon: 'img/favicon.svg',

  // Production URL — update when the site is deployed
  url: 'https://docs.sorobanpulse.dev',
  baseUrl: '/',

  organizationName: 'Soroban-Pulse',
  projectName: 'SorobanPulse',

  // Many docs link to repository files outside docs/ (README.md, helm/, …),
  // which are not site pages; the lychee job in docs.yml checks real links.
  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // ---------------------------------------------------------------------------
  // Plugins
  // ---------------------------------------------------------------------------
  plugins: [
    // Pages merged into another doc keep working via client-side redirects
    // (Issue #1170).
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          { from: '/docs/property-testing', to: '/docs/property-based-testing' },
          { from: '/docs/api-contract-testing', to: '/docs/contract-testing' },
        ],
      },
    ],
  ],

  // Offline full-text search, indexed at build time (no external service).
  themes: [
    '@docusaurus/theme-mermaid',
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        indexBlog: false,
        docsRouteBasePath: '/docs',
        docsDir: '../../docs',
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          // The primary "Getting Started" section uses the default docs plugin
          // Single docs instance over the whole docs/ tree; every sidebar is
          // built from docs/site/SUMMARY.md.
          path: '../../docs',
          routeBasePath: 'docs',
          sidebarPath: require.resolve('./sidebars.js'),
          exclude: ['site/**', '**/node_modules/**'],
          // Keep ADR numbers (adr/0001-…) in doc IDs and URLs.
          numberPrefixParser: false,
          editUrl:
            'https://github.com/Soroban-Pulse/SorobanPulse/edit/main/docs/',
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          // Enable Mermaid diagrams in MDX
          remarkPlugins: [],
          rehypePlugins: [],
        },
        blog: false, // No blog section
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.5,
          ignorePatterns: ['/tags/**'],
        },
      }),
    ],
  ],

  // ---------------------------------------------------------------------------
  // Mermaid diagrams
  // ---------------------------------------------------------------------------
  markdown: {
    mermaid: true,
    // Parse .md files as CommonMark and only .mdx files as MDX, so plain
    // Markdown docs with `<` or `{` in prose do not break the build.
    format: 'detect',
  },

  // ---------------------------------------------------------------------------
  // Theme configuration
  // ---------------------------------------------------------------------------
  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Social card used for og:image
      image: 'img/sorobanpulse-social-card.png',

      // Announcement bar (remove or update as needed)
      announcementBar: {
        id: 'api_v1',
        content:
          '🚀 SorobanPulse v1 API is live — unversioned routes are deprecated. <a href="/docs/api-deprecation">Migrate now →</a>',
        backgroundColor: '#1a1d2e',
        textColor: '#a78bfa',
        isCloseable: true,
      },

      // -----------------------------------------------------------------
      // Colour mode (dark default, user-switchable)
      // -----------------------------------------------------------------
      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },

      // -----------------------------------------------------------------
      // Navigation bar
      // -----------------------------------------------------------------
      navbar: {
        title: 'SorobanPulse',
        logo: {
          alt: 'SorobanPulse logo',
          src: 'img/logo.svg',
          srcDark: 'img/logo-dark.svg',
          href: '/',
          target: '_self',
        },
        style: 'dark',
        hideOnScroll: false,
        items: [
          // One entry per top-level section of docs/site/SUMMARY.md.
          { type: 'docSidebar', sidebarId: 'getting-started', position: 'left', label: 'Getting Started' },
          { type: 'docSidebar', sidebarId: 'guides', position: 'left', label: 'Guides' },
          {
            type: 'dropdown',
            label: 'API',
            position: 'left',
            items: [
              { type: 'docSidebar', sidebarId: 'api', label: 'API Guides' },
              { label: 'Interactive API Reference', to: '/api-reference' },
              { label: 'OpenAPI JSON', href: 'pathname:///openapi.json' },
            ],
          },
          { type: 'docSidebar', sidebarId: 'operations', position: 'left', label: 'Operations' },
          { type: 'docSidebar', sidebarId: 'integrations', position: 'left', label: 'Integrations' },
          { type: 'docSidebar', sidebarId: 'contributing', position: 'left', label: 'Contributing' },
          { type: 'docSidebar', sidebarId: 'reference', position: 'left', label: 'Reference' },
          // ── Right-side items ─────────────────────────────────────────
          {
            href: 'https://github.com/Soroban-Pulse/SorobanPulse',
            position: 'right',
            className: 'navbar-github-link',
            'aria-label': 'GitHub repository',
          },
          {
            type: 'search',
            position: 'right',
          },
        ],
      },

      // -----------------------------------------------------------------
      // Footer
      // -----------------------------------------------------------------
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              { label: 'Getting Started', to: '/docs/onboarding' },
              { label: 'API Guide', to: '/docs/api-guide' },
              { label: 'SDK Integration', to: '/docs/sdk-integration-guide' },
              { label: 'FAQ', to: '/docs/FAQ' },
            ],
          },
          {
            title: 'API',
            items: [
              { label: 'API Reference', to: '/api-reference' },
              { label: 'OpenAPI JSON', href: 'pathname:///openapi.json' },
              { label: 'API Changelog', to: '/docs/api-changelog' },
              { label: 'API Versioning', to: '/docs/api-versioning' },
            ],
          },
          {
            title: 'Operations',
            items: [
              { label: 'Deployment', to: '/docs/deployment' },
              { label: 'Metrics', to: '/docs/metrics-reference' },
              { label: 'Alerts', to: '/docs/alerting' },
              { label: 'Runbooks', to: '/docs/runbooks/operator-runbook' },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/Soroban-Pulse/SorobanPulse',
              },
              {
                label: 'Issues',
                href: 'https://github.com/Soroban-Pulse/SorobanPulse/issues',
              },
              {
                label: 'CONTRIBUTING.md',
                href: 'https://github.com/Soroban-Pulse/SorobanPulse/blob/main/CONTRIBUTING.md',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} SorobanPulse contributors. Built with Docusaurus.`,
      },

      // -----------------------------------------------------------------
      // Algolia DocSearch (fill in your own keys when ready)
      // -----------------------------------------------------------------
      // algolia: {
      //   appId: 'YOUR_APP_ID',
      //   apiKey: 'YOUR_SEARCH_API_KEY',
      //   indexName: 'sorobanpulse',
      //   contextualSearch: true,
      // },

      // -----------------------------------------------------------------
      // Prism syntax highlighting
      // -----------------------------------------------------------------
      prism: {
        theme: require('prism-react-renderer').themes.nightOwlLight,
        darkTheme: require('prism-react-renderer').themes.nightOwl,
        additionalLanguages: [
          'bash',
          'json',
          'toml',
          'yaml',
          'sql',
          'rust',
          'typescript',
          'python',
          'go',
          'lua',
          'docker',
        ],
      },

      // -----------------------------------------------------------------
      // Table of contents
      // -----------------------------------------------------------------
      tableOfContents: {
        minHeadingLevel: 2,
        maxHeadingLevel: 4,
      },

      // -----------------------------------------------------------------
      // Mermaid theme
      // -----------------------------------------------------------------
      mermaid: {
        theme: { light: 'default', dark: 'dark' },
        options: {
          // Global Mermaid init — see diagram-style-guide.md for per-diagram
          // %%{init} blocks that apply the SorobanPulse palette.
          fontFamily: 'Inter, system-ui, sans-serif',
          fontSize: 14,
        },
      },
    }),
};

module.exports = config;
