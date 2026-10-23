import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { CopyButton } from "./CopyButton.tsx";
import { useRelatedEvents, type Event } from "../api/hooks.ts";

// ---------------------------------------------------------------------------
// Collapsible JSON tree
// ---------------------------------------------------------------------------

interface JsonNodeProps {
  data: unknown;
  depth?: number;
}

function JsonNode({ data, depth = 0 }: JsonNodeProps) {
  const [open, setOpen] = useState(depth < 2);
  const indent = depth * 16;

  if (data === null) return <span className="json-null">null</span>;
  if (typeof data === "boolean")
    return <span className="json-bool">{String(data)}</span>;
  if (typeof data === "number")
    return <span className="json-num">{String(data)}</span>;
  if (typeof data === "string")
    return <span className="json-str">&quot;{data}&quot;</span>;

  if (Array.isArray(data)) {
    if (data.length === 0) return <span className="json-bracket">{"[]"}</span>;
    return (
      <span>
        <button
          className="json-toggle"
          style={{ marginLeft: indent }}
          onClick={() => setOpen((o) => !o)}
          aria-expanded={open}
        >
          {open ? "▾" : "▸"} [{data.length}]
        </button>
        {open && (
          <span className="json-block">
            {data.map((v, i) => (
              <div key={i} style={{ paddingLeft: indent + 16 }}>
                <JsonNode data={v} depth={depth + 1} />
                {i < data.length - 1 && <span className="json-comma">,</span>}
              </div>
            ))}
          </span>
        )}
      </span>
    );
  }

  if (typeof data === "object") {
    const entries = Object.entries(data as Record<string, unknown>);
    if (entries.length === 0)
      return <span className="json-bracket">{"{}"}</span>;
    return (
      <span>
        <button
          className="json-toggle"
          style={{ marginLeft: indent }}
          onClick={() => setOpen((o) => !o)}
          aria-expanded={open}
        >
          {open ? "▾" : "▸"} {"{"}
          {entries.length}
          {"}"}
        </button>
        {open && (
          <span className="json-block">
            {entries.map(([k, v], i) => (
              <div key={k} style={{ paddingLeft: indent + 16 }}>
                <span className="json-key">&quot;{k}&quot;</span>
                <span className="json-colon">: </span>
                <JsonNode data={v} depth={depth + 1} />
                {i < entries.length - 1 && (
                  <span className="json-comma">,</span>
                )}
              </div>
            ))}
          </span>
        )}
      </span>
    );
  }

  return <span>{String(data)}</span>;
}

// ---------------------------------------------------------------------------
// Format toggle
// ---------------------------------------------------------------------------

type DataFormat = "native" | "json" | "xdr";

interface DataViewProps {
  data: unknown;
}

function DataView({ data }: DataViewProps) {
  const [fmt, setFmt] = useState<DataFormat>("native");

  const jsonStr = JSON.stringify(data, null, 2);
  // XDR representation: without a server-side XDR decoder we show the
  // base64-encoded UTF-8 JSON as a reasonable stand-in; the toggle is
  // architecturally ready for the real value once the format API ships.
  const xdrStr = btoa(
    Array.from(new TextEncoder().encode(jsonStr), (b) =>
      String.fromCodePoint(b),
    ).join(""),
  );

  return (
    <div className="data-view">
      <div className="data-view__toolbar">
        {(["native", "json", "xdr"] as DataFormat[]).map((f) => (
          <button
            key={f}
            className={`data-view__tab${fmt === f ? " data-view__tab--active" : ""}`}
            onClick={() => setFmt(f)}
          >
            {f === "xdr" ? "Raw XDR" : f.charAt(0).toUpperCase() + f.slice(1)}
          </button>
        ))}
      </div>

      <div className="data-view__body mono">
        {fmt === "native" && <JsonNode data={data} depth={0} />}
        {fmt === "json" && <pre>{jsonStr}</pre>}
        {fmt === "xdr" && (
          <>
            <p className="data-view__xdr-note text-muted">
              Base64-encoded JSON (XDR decode requires server-side format=xdr support)
            </p>
            <pre style={{ wordBreak: "break-all", whiteSpace: "pre-wrap" }}>
              {xdrStr}
            </pre>
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// External link helper
// ---------------------------------------------------------------------------

interface ExternalLinkProps {
  href: string;
  children: React.ReactNode;
}

function ExternalLink({ href, children }: ExternalLinkProps) {
  return (
    <a href={href} target="_blank" rel="noreferrer noopener" className="ext-link">
      {children} ↗
    </a>
  );
}

// ---------------------------------------------------------------------------
// Related events mini-list
// ---------------------------------------------------------------------------

interface RelatedEventsProps {
  txHash: string;
  currentEventId: string;
  onSelect: (e: Event) => void;
}

function RelatedEvents({ txHash, currentEventId, onSelect }: RelatedEventsProps) {
  const { data, isLoading, isError } = useRelatedEvents(txHash);

  const others = (data ?? []).filter((e) => e.id !== currentEventId);

  if (isLoading)
    return <p className="text-muted" style={{ fontSize: "0.8125rem" }}>Loading related events…</p>;
  if (isError)
    return <p className="text-muted" style={{ fontSize: "0.8125rem" }}>Could not load related events.</p>;
  if (others.length === 0)
    return <p className="text-muted" style={{ fontSize: "0.8125rem" }}>No other events in this transaction.</p>;

  return (
    <ul className="related-list">
      {others.map((e) => (
        <li key={e.id} className="related-list__item">
          <button className="related-list__btn" onClick={() => onSelect(e)}>
            <span className={`badge badge--${e.event_type}`}>{e.event_type}</span>
            <span className="mono related-list__id" title={e.id}>
              {e.id.slice(0, 12)}…
            </span>
            <span className="mono related-list__contract text-muted" title={e.contract_id}>
              {e.contract_id.slice(0, 12)}…
            </span>
          </button>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// Field row
// ---------------------------------------------------------------------------

interface FieldRowProps {
  label: string;
  value: string;
  mono?: boolean;
  copyLabel?: string;
}

function FieldRow({ label, value, mono, copyLabel }: FieldRowProps) {
  return (
    <div className="drawer-field">
      <span className="drawer-field__label">{label}</span>
      <span className={`drawer-field__value${mono ? " mono" : ""}`} title={value}>
        {value}
      </span>
      {copyLabel !== undefined && (
        <CopyButton value={value} label={copyLabel} />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main drawer
// ---------------------------------------------------------------------------

export interface EventDetailDrawerProps {
  event: Event;
  network?: "mainnet" | "testnet" | "futurenet";
  onClose: () => void;
  onNavigate?: (eventId: string) => void;
}

/**
 * Side drawer that shows all fields of a Soroban event:
 * - Metadata fields with copy buttons and external links
 * - Collapsible JSON tree (native), raw JSON, and raw XDR toggle for event_data
 * - Related events in this transaction
 * - Deep-linkable — callers should update the URL to /events/:id when opening
 */
export function EventDetailDrawer({
  event,
  network = "mainnet",
  onClose,
  onNavigate,
}: EventDetailDrawerProps) {
  const navigate = useNavigate();
  const drawerRef = useRef<HTMLElement>(null);

  // Focus trap: move focus into the drawer when it opens
  useEffect(() => {
    const prev = document.activeElement as HTMLElement | null;
    drawerRef.current?.focus();
    return () => {
      prev?.focus();
    };
  }, [event.id]);

  // Close on Escape
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  const stellarExpertBase =
    network === "mainnet"
      ? "https://stellar.expert/explorer/public"
      : `https://stellar.expert/explorer/${network}`;

  const stellarChainBase =
    network === "mainnet"
      ? "https://stellar.chain/mainnet"
      : `https://stellar.chain/${network}`;

  function handleRelatedSelect(e: Event) {
    if (onNavigate) {
      onNavigate(e.id);
    } else {
      navigate(`/events/${e.id}`, { state: { event: e } });
    }
  }

  return (
    <>
      {/* Backdrop */}
      <div
        className="drawer-backdrop"
        onClick={onClose}
        aria-hidden="true"
      />

      {/* Panel */}
      <aside
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-label="Event detail"
        ref={drawerRef}
        tabIndex={-1}
      >
        {/* Header */}
        <div className="drawer__header">
          <h2 className="drawer__title">Event Detail</h2>
          <button
            className="drawer__close"
            onClick={onClose}
            aria-label="Close event detail"
          >
            ✕
          </button>
        </div>

        <div className="drawer__body">
          {/* ── Identifiers ── */}
          <section className="drawer-section">
            <h3 className="drawer-section__title">Identifiers</h3>

            <FieldRow
              label="Event ID"
              value={event.id}
              mono
              copyLabel="event ID"
            />
            <div className="drawer-field">
              <span className="drawer-field__label">Contract ID</span>
              <button
                className="drawer-field__value mono"
                onClick={() => navigate(`/contracts/${event.contract_id}`)}
                style={{ cursor: "pointer", textDecoration: "underline" }}
              >
                {event.contract_id}
              </button>
              <CopyButton value={event.contract_id} label="contract ID" />
            </div>
            <FieldRow
              label="Tx Hash"
              value={event.tx_hash}
              mono
              copyLabel="tx hash"
            />

            <div className="drawer-field drawer-field--links">
              <span className="drawer-field__label">View on</span>
              <div className="drawer-field__value" style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
                <ExternalLink
                  href={`${stellarExpertBase}/tx/${event.tx_hash}`}
                >
                  stellar.expert
                </ExternalLink>
                <ExternalLink
                  href={`${stellarChainBase}/tx/${event.tx_hash}`}
                >
                  stellar.chain
                </ExternalLink>
              </div>
            </div>
          </section>

          {/* ── Metadata ── */}
          <section className="drawer-section">
            <h3 className="drawer-section__title">Metadata</h3>

            <FieldRow label="Type" value={event.event_type} />
            <FieldRow label="Ledger" value={event.ledger.toLocaleString()} />
            <FieldRow
              label="Timestamp"
              value={new Date(event.timestamp).toLocaleString()}
            />
            <FieldRow
              label="Indexed at"
              value={new Date(event.created_at).toLocaleString()}
            />
          </section>

          {/* ── Event data ── */}
          <section className="drawer-section">
            <h3 className="drawer-section__title">
              Event Data
              <span className="drawer-section__subtitle">
                topics &amp; payload
              </span>
            </h3>
            <DataView data={event.event_data} />
          </section>

          {/* ── Related events ── */}
          <section className="drawer-section">
            <h3 className="drawer-section__title">
              Related events in this transaction
            </h3>
            <RelatedEvents
              txHash={event.tx_hash}
              currentEventId={event.id}
              onSelect={handleRelatedSelect}
            />
          </section>
        </div>
      </aside>
    </>
  );
}
