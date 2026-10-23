/**
 * docs/site/src/pages/index.tsx
 *
 * SorobanPulse documentation landing page.
 *
 * Sections:
 *   1. Hero — headline, sub-headline, primary CTA + secondary CTA
 *   2. Stats strip — three headline numbers
 *   3. Feature grid — Indexing, Streaming, Webhooks, Notifications
 *   4. Quick-start — code snippet + "Read the docs" CTA
 *   5. Bottom CTA banner
 *
 * Styling comes from docs/site/src/css/custom.css (.sp-* classes).
 * No external CSS dependencies beyond what Docusaurus already loads.
 */

import React from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import clsx from 'clsx';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Feature {
  icon: string;
  title: string;
  description: string;
  href: string;
  linkLabel: string;
}

interface Stat {
  value: string;
  label: string;
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

const FEATURES: Feature[] = [
  {
    icon: '⚡',
    title: 'Real-time Indexing',
    description:
      'Continuously polls the Stellar Soroban RPC and persists every contract event to PostgreSQL. Multi-replica safe via advisory locks — only one indexer leads, standbys take over automatically.',
    href: '/docs/architecture',
    linkLabel: 'Architecture →',
  },
  {
    icon: '📡',
    title: 'Live Streaming (SSE)',
    description:
      'Subscribe to a contract or all events over a single long-lived Server-Sent Events connection. Missed events are replayed on reconnect using Last-Event-ID.',
    href: '/docs/sse-reconnection',
    linkLabel: 'SSE docs →',
  },
  {
    icon: '🔔',
    title: 'Webhooks',
    description:
      'Push event notifications to any HTTP endpoint with HMAC-SHA256 signatures, automatic retries with exponential back-off, circuit breaker, and per-endpoint rate limiting.',
    href: '/docs/webhook-verification',
    linkLabel: 'Webhook guide →',
  },
  {
    icon: '📬',
    title: 'Notifications',
    description:
      'Fan out to email, Slack, Discord, PagerDuty, Telegram, and SMS. Deduplication, batching, content filters, and delivery receipts are built in.',
    href: '/docs/notification-channels',
    linkLabel: 'Notification channels →',
  },
];

const STATS: Stat[] = [
  { value: '< 200 ms', label: 'p99 API latency at 100 req/s' },
  { value: '< 1 %',    label: 'target error rate' },
  { value: '15 s',     label: 'SSE keep-alive interval' },
];

const QUICK_START_CODE = `# 1. Clone and configure
git clone https://github.com/Soroban-Pulse/SorobanPulse.git
cd SorobanPulse
cp .env.example .env  # fill in DATABASE_URL + STELLAR_RPC_URL

# 2. Start the stack (Postgres + service)
make docker-up

# 3. Query indexed events
curl http://localhost:3000/v1/events | jq '.data[0]'

# 4. Stream events live
curl -N http://localhost:3000/v1/events/stream`;

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function HeroSection(): JSX.Element {
  return (
    <section className="sp-hero">
      <div className="sp-hero__eyebrow">Open-source · Rust · Stellar Soroban</div>

      <h1 className="sp-hero__title">
        Index. Stream. React.<br />
        <span style={{ color: 'var(--sp-brand-400)' }}>In Real Time.</span>
      </h1>

      <p className="sp-hero__subtitle">
        SorobanPulse indexes every Soroban smart contract event on the Stellar network
        and exposes them via a REST API, live SSE stream, webhooks, and notifications —
        so your app always has the freshest on-chain data.
      </p>

      <div className="sp-hero__actions">
        <Link className="sp-btn sp-btn--primary" to="/docs/onboarding">
          🚀 Get Started
        </Link>
        <Link className="sp-btn sp-btn--secondary" to="/docs/api-guide">
          API Reference
        </Link>
        <Link
          className="sp-btn sp-btn--secondary"
          href="https://github.com/Soroban-Pulse/SorobanPulse"
        >
          GitHub ↗
        </Link>
      </div>
    </section>
  );
}

function StatsStrip(): JSX.Element {
  return (
    <div className="sp-stats">
      {STATS.map((stat) => (
        <div key={stat.label} style={{ textAlign: 'center' }}>
          <span className="sp-stat__value">{stat.value}</span>
          <span className="sp-stat__label">{stat.label}</span>
        </div>
      ))}
    </div>
  );
}

function FeatureCard({ feature }: { feature: Feature }): JSX.Element {
  return (
    <div className="sp-feature-card">
      <div className="sp-feature-card__icon" role="img" aria-label={feature.title}>
        {feature.icon}
      </div>
      <h3 className="sp-feature-card__title">{feature.title}</h3>
      <p className="sp-feature-card__desc">{feature.description}</p>
      <Link
        to={feature.href}
        style={{
          display: 'inline-block',
          marginTop: '1rem',
          fontSize: '0.9375rem',
          fontWeight: 600,
          color: 'var(--sp-brand-400)',
          textDecoration: 'none',
        }}
      >
        {feature.linkLabel}
      </Link>
    </div>
  );
}

function FeaturesSection(): JSX.Element {
  return (
    <section className="sp-features">
      <div style={{ textAlign: 'center', marginBottom: '3rem' }}>
        <span className="sp-section__label">Core capabilities</span>
        <h2 className="sp-section__title">Everything you need for on-chain events</h2>
        <p className="sp-section__subtitle">
          From raw ledger data to your application in one service — no custom pipelines,
          no polling logic, no missed events.
        </p>
      </div>

      <div className="sp-features__grid">
        {FEATURES.map((f) => (
          <FeatureCard key={f.title} feature={f} />
        ))}
      </div>
    </section>
  );
}

function QuickStartSection(): JSX.Element {
  return (
    <section className="sp-quickstart">
      <span className="sp-section__label">Quick start</span>
      <h2 className="sp-section__title">Up and running in minutes</h2>
      <p className="sp-section__subtitle">
        Docker Compose bundles Postgres and the service. One command and you have a
        fully-indexed local node ready to query.
      </p>

      <div style={{ maxWidth: 720, margin: '0 auto 2.5rem', textAlign: 'left' }}>
        <CodeBlock language="bash" title="Terminal">{QUICK_START_CODE}</CodeBlock>
      </div>

      <div style={{ display: 'flex', gap: '1rem', justifyContent: 'center', flexWrap: 'wrap' }}>
        <Link className="sp-btn sp-btn--primary" to="/docs/onboarding">
          Full onboarding guide →
        </Link>
        <Link className="sp-btn sp-btn--secondary" to="/docs/development-setup">
          Dev environment setup
        </Link>
      </div>
    </section>
  );
}

function BottomCTA(): JSX.Element {
  return (
    <section
      style={{
        padding: '4rem 2rem',
        textAlign: 'center',
        background: 'var(--ifm-background-surface-color)',
        borderTop: '1px solid var(--ifm-hr-border-color)',
      }}
    >
      <h2
        className="sp-section__title"
        style={{ marginBottom: '0.75rem' }}
      >
        Ready to build on Soroban events?
      </h2>
      <p
        className="sp-section__subtitle"
        style={{ marginBottom: '2rem' }}
      >
        Check out the SDK integration guide or explore the interactive API docs.
      </p>
      <div style={{ display: 'flex', gap: '1rem', justifyContent: 'center', flexWrap: 'wrap' }}>
        <Link className="sp-btn sp-btn--primary" to="/docs/sdk-integration-guide">
          SDK Integration Guide
        </Link>
        <Link className="sp-btn sp-btn--secondary" to="/api-reference">
          Open API Explorer ↗
        </Link>
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();

  return (
    <Layout
      title={siteConfig.title}
      description={siteConfig.tagline}
    >
      <main>
        <HeroSection />
        <StatsStrip />
        <FeaturesSection />
        <QuickStartSection />
        <BottomCTA />
      </main>
    </Layout>
  );
}
