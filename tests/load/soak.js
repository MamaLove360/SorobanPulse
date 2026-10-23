// k6 soak test for Soroban Pulse — issue #811
//
// Runs a sustained baseline load for up to 24 hours to detect memory leaks,
// connection pool exhaustion, and gradual performance degradation.
//
// Default duration is 24h; override with SOAK_DURATION env var.
//
// Run:
//   k6 run tests/load/soak.js                          # full 24-hour soak
//   k6 run -e SOAK_DURATION=30m tests/load/soak.js     # short validation run
//
// Env:
//   BASE_URL      (default http://localhost:3000)
//   API_KEY       (optional)
//   SOAK_DURATION (default 24h)

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate, Counter, Gauge } from "k6/metrics";

// ── Custom metrics ─────────────────────────────────────────────────────────
const latency         = new Trend("soak_latency_ms", true);
const errorRate       = new Rate("soak_error_rate");
const throughput      = new Counter("soak_requests_total");
const latencyDrift    = new Trend("soak_latency_drift_ms", true);   // rolling window comparison
const sseDuration     = new Trend("soak_sse_duration_ms", true);
const healthFailRate  = new Rate("soak_health_failure_rate");
const failuresByCause = new Counter("soak_failures_total");         // tagged by `cause`

const SOAK_DURATION = __ENV.SOAK_DURATION || "24h";

// ── Scenario ───────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    // Sustained REST API load — mirrors production baseline
    rest_sustained: {
      executor: "constant-arrival-rate",
      rate: 100,
      timeUnit: "1s",
      duration: SOAK_DURATION,
      preAllocatedVUs: 30,
      maxVUs: 80,
      exec: "restLoad",
    },
    // A small cohort of long-lived SSE connections
    sse_sustained: {
      executor: "constant-vus",
      vus: 20,
      duration: SOAK_DURATION,
      exec: "sseLoad",
    },
    // Periodic health checks to confirm liveness throughout
    health_probe: {
      executor: "constant-arrival-rate",
      rate: 1,
      timeUnit: "10s",        // 1 probe every 10 seconds
      duration: SOAK_DURATION,
      preAllocatedVUs: 2,
      maxVUs: 5,
      exec: "healthCheck",
    },
  },
  thresholds: {
    // SLOs must hold for the entire duration of the soak.
    soak_latency_ms: [
      { threshold: "p(95)<300",  abortOnFail: true  },
      { threshold: "p(99)<500",  abortOnFail: false },
    ],
    soak_error_rate: [
      { threshold: "rate<0.01",  abortOnFail: true  },
    ],
    soak_sse_duration_ms: [
      { threshold: "p(99)<1000", abortOnFail: false },
    ],
    soak_health_failure_rate: [
      { threshold: "rate<0.05",  abortOnFail: false },
    ],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:3000";
const HEADERS  = __ENV.API_KEY ? { "X-Api-Key": __ENV.API_KEY } : {};

// Rotating endpoint mix to exercise all read paths over time
const ENDPOINTS = [
  "/v1/events?page=1&limit=20",
  "/v1/events?page=2&limit=20",
  "/v1/events?event_type=contract&limit=20",
  "/v1/events?event_type=diagnostic&limit=20",
  "/v1/events?from_ledger=1000000&to_ledger=1100000&limit=20",
  "/v1/events?exact_count=true&limit=20",
  "/v1/events?page=1&limit=100",
];

let requestIndex = 0;

// Cap per-VU failure logging so a sustained outage doesn't flood the output.
const MAX_FAILURE_LOGS = 20;
let failureLogs = 0;

function bodySnippet(res) {
  return (res.body || "").toString().slice(0, 200).replace(/\s+/g, " ");
}

// Classify a failed response so the summary says *why* the soak failed
// (timeouts, rate limiting, pool exhaustion, RPC/indexer trouble, ...).
function failureCause(res) {
  if (res.status === 0)   return res.error_code ? `network_${res.error_code}` : "network";
  if (res.status === 429) return "rate_limited";
  if (res.status === 503) return "unavailable";
  if (res.status >= 500)  return "server_error";
  if (res.status >= 400)  return "client_error";
  return "bad_body";
}

function reportFailure(kind, path, res) {
  const cause = failureCause(res);
  failuresByCause.add(1, { cause, kind });
  if (failureLogs++ < MAX_FAILURE_LOGS) {
    console.error(
      `[soak] ${kind} FAILED path=${path} status=${res.status} cause=${cause} ` +
      `duration=${res.timings.duration.toFixed(0)}ms error=${res.error || "-"} body="${bodySnippet(res)}"`
    );
  }
}

export function restLoad() {
  // Round-robin through endpoints so every path is exercised equally.
  const path = ENDPOINTS[requestIndex % ENDPOINTS.length];
  requestIndex++;

  const res = http.get(`${BASE_URL}${path}`, { headers: HEADERS, timeout: "10s" });

  latency.add(res.timings.duration);
  throughput.add(1);

  const ok = check(res, {
    "rest: status is 200":            (r) => r.status === 200,
    "rest: body has `data` field":    (r) => {
      try { return JSON.parse(r.body).data !== undefined; }
      catch { return false; }
    },
  }, { endpoint: path });
  errorRate.add(!ok);
  if (!ok) reportFailure("rest", path, res);
}

export function sseLoad() {
  const start = Date.now();

  const res = http.get(`${BASE_URL}/v1/events/stream`, {
    headers: { ...HEADERS, Accept: "text/event-stream" },
    timeout: "120s",
  });

  sseDuration.add(Date.now() - start);

  const ok = check(res, {
    "sse: status is 200":                  (r) => r.status === 200,
    "sse: content-type text/event-stream": (r) =>
      (r.headers["Content-Type"] || "").includes("text/event-stream"),
  });
  if (!ok) reportFailure("sse", "/v1/events/stream", res);

  // Hold for a random duration between 30 s and 5 min to simulate real clients.
  sleep(30 + Math.random() * 270);
}

export function healthCheck() {
  const res = http.get(`${BASE_URL}/healthz/ready`, {
    headers: HEADERS,
    timeout: "5s",
  });
  const ok = check(res, {
    "health: status is 200":      (r) => r.status === 200,
    "health: db is ok":           (r) => {
      try { return JSON.parse(r.body).db === "ok"; }
      catch { return false; }
    },
    "health: indexer is ok":      (r) => {
      try { return JSON.parse(r.body).indexer === "ok"; }
      catch { return false; }
    },
  });
  healthFailRate.add(!ok);
  if (!ok) reportFailure("health", "/healthz/ready", res);
}

export function handleSummary(data) {
  const p95   = data.metrics.soak_latency_ms?.values?.["p(95)"] ?? "n/a";
  const p99   = data.metrics.soak_latency_ms?.values?.["p(99)"] ?? "n/a";
  const err   = data.metrics.soak_error_rate?.values?.rate       ?? "n/a";
  const total = data.metrics.soak_requests_total?.values?.count  ?? 0;
  const healthFail = data.metrics.soak_health_failure_rate?.values?.rate ?? "n/a";
  const failures   = data.metrics.soak_failures_total?.values?.count     ?? 0;

  console.log("\n=== SOAK TEST SUMMARY ===");
  console.log(`Duration       : ${SOAK_DURATION}`);
  console.log(`Total requests : ${total}`);
  console.log(`p95 latency    : ${typeof p95 === "number" ? p95.toFixed(1) : p95} ms`);
  console.log(`p99 latency    : ${typeof p99 === "number" ? p99.toFixed(1) : p99} ms`);
  console.log(`Error rate     : ${typeof err === "number" ? (err * 100).toFixed(2) : err} %`);
  console.log(`Health fails   : ${typeof healthFail === "number" ? (healthFail * 100).toFixed(2) : healthFail} %`);
  console.log(`Failed requests: ${failures} (see [soak] lines above for status/cause)`);

  // Spell out which SLO thresholds were breached instead of a bare non-zero exit.
  const breached = [];
  for (const [name, metric] of Object.entries(data.metrics)) {
    for (const [threshold, result] of Object.entries(metric.thresholds || {})) {
      if (!result.ok) breached.push(`${name}: ${threshold}`);
    }
  }
  if (breached.length) {
    console.error("\nFAILED thresholds:");
    for (const b of breached) console.error(`  - ${b}`);
    console.error(
      "Check soroban_pulse_process_memory_bytes (memory growth), " +
      "soroban_pulse_db_pool_size / soroban_pulse_db_pool_idle (pool exhaustion) and " +
      "soroban_pulse_rpc_errors_total (RPC errors) on /metrics."
    );
  }

  return {
    "tests/load/results/soak_summary.json": JSON.stringify(data, null, 2),
  };
}
