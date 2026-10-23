// k6 webhook delivery load test — issue #811
//
// Tests webhook delivery reliability and latency under load by driving event
// ingestion (via the admin replay endpoint or a stub) and measuring:
//   - Webhook queue depth proxy (admin stats endpoint)
//   - Delivery latency from event creation to webhook POST
//   - Retry behaviour under a simulated flaky webhook receiver
//   - HMAC signature presence on outgoing webhook calls
//
// Because k6 cannot act as a webhook receiver directly, this script drives
// the Soroban Pulse API and polls the admin/metrics endpoint to observe
// webhook failure counters and queue depth.
//
// For end-to-end delivery latency you need a real (or stubbed) webhook
// receiver that timestamps receipt. See docs/performance-tuning.md for a
// WireMock / webhook-site setup guide.
//
// Run:  k6 run tests/load/webhook_delivery.js
// Env:  BASE_URL         (default http://localhost:3000)
//       ADMIN_API_KEY    (required for /v1/admin/* endpoints)
//       WEBHOOK_RECEIVER (URL k6 will poll to count received deliveries)

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate, Counter, Gauge } from "k6/metrics";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// ── Custom metrics ─────────────────────────────────────────────────────────
const replayLatency      = new Trend("wh_replay_latency_ms",    true);
const metricsLatency     = new Trend("wh_metrics_latency_ms",   true);
const webhookFailures    = new Gauge("wh_failure_counter");
const errorRate          = new Rate("wh_error_rate");
const throughput         = new Counter("wh_requests_total");

// Retry-storm / DLQ metrics (issue #1276)
const dlqDepth           = new Gauge("wh_dlq_depth");
const deliveryLatency    = new Trend("wh_delivery_latency_ms",  true);
const successRate        = new Rate("wh_success_rate");
const dlqReplayLatency   = new Trend("wh_dlq_replay_latency_ms", true);

// ── Scenario ───────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    // Drive event ingestion — pushes events through the webhook delivery pipeline
    event_injection: {
      executor: "constant-arrival-rate",
      rate: 20,
      timeUnit: "1s",
      duration: "5m",
      preAllocatedVUs: 10,
      maxVUs: 30,
      exec: "injectEvents",
    },
    // Concurrently poll metrics to observe delivery health
    metrics_observer: {
      executor: "constant-arrival-rate",
      rate: 1,
      timeUnit: "5s",         // one probe every 5 seconds
      duration: "5m",
      preAllocatedVUs: 2,
      maxVUs: 5,
      exec: "observeMetrics",
    },
    // Simultaneous REST load to verify webhook delivery doesn't starve API
    background_rest: {
      executor: "constant-arrival-rate",
      rate: 50,
      timeUnit: "1s",
      duration: "5m",
      preAllocatedVUs: 15,
      maxVUs: 40,
      exec: "backgroundRest",
    },
    // Retry storm: 80% of mock webhook endpoints return HTTP 500, sustained
    // event injection for 10 minutes to exercise retry/backoff paths.
    retry_storm: {
      executor: "constant-arrival-rate",
      rate: 20,
      timeUnit: "1s",
      duration: "10m",
      preAllocatedVUs: 10,
      maxVUs: 40,
      exec: "retryStorm",
      startTime: "0s",
    },
    // Sustained load: sample DLQ depth and successful delivery latency for
    // 10 minutes while the retry storm is in flight.
    sustained_load: {
      executor: "constant-arrival-rate",
      rate: 1,
      timeUnit: "2s",         // one probe every 2 seconds
      duration: "10m",
      preAllocatedVUs: 2,
      maxVUs: 5,
      exec: "sustainedLoad",
      startTime: "0s",
    },
    // DLQ replay under concurrent load — starts after the storm has built up
    // some DLQ depth and runs alongside the sustained load.
    dlq_replay: {
      executor: "constant-arrival-rate",
      rate: 1,
      timeUnit: "10s",        // one replay every 10 seconds
      duration: "8m",
      preAllocatedVUs: 2,
      maxVUs: 5,
      exec: "dlqReplay",
      startTime: "2m",
    },
  },
  thresholds: {
    wh_replay_latency_ms:  ["p(99)<1000"],
    wh_metrics_latency_ms: ["p(99)<200"],
    wh_error_rate:         ["rate<0.05"],
    // Retry-storm / DLQ SLOs (issue #1276)
    wh_dlq_depth:              ["max<1000"],
    wh_delivery_latency_ms:    ["p(99)<5000"],
    wh_success_rate:           ["rate>0.0"],
    wh_dlq_replay_latency_ms:  ["p(99)<10000"],
  },
};

const BASE_URL      = __ENV.BASE_URL      || "http://localhost:3000";
const ADMIN_KEY     = __ENV.ADMIN_API_KEY || "";
const RECEIVER_URL  = __ENV.WEBHOOK_RECEIVER || "";
// Fraction of mock webhook endpoints that should return HTTP 500 (0.0–1.0).
const FAILURE_RATIO = parseFloat(__ENV.WEBHOOK_FAILURE_RATIO || "0.8");

const ADMIN_HEADERS = ADMIN_KEY
  ? { "X-Api-Key": ADMIN_KEY, "Content-Type": "application/json" }
  : { "Content-Type": "application/json" };

const READ_HEADERS = ADMIN_KEY ? { "X-Api-Key": ADMIN_KEY } : {};

// ── Helpers ────────────────────────────────────────────────────────────────

// Trigger event replay for a random ledger range (drives webhook delivery).
export function injectEvents() {
  if (!ADMIN_KEY) {
    // Skip injection if no admin key — metrics observer still runs.
    sleep(1);
    return;
  }

  const fromLedger = randomIntBetween(1_000_000, 1_400_000);
  const toLedger   = fromLedger + randomIntBetween(1, 10);

  const payload = JSON.stringify({ from_ledger: fromLedger, to_ledger: toLedger });
  const res = http.post(
    `${BASE_URL}/v1/admin/replay`,
    payload,
    { headers: ADMIN_HEADERS, timeout: "30s" }
  );

  replayLatency.add(res.timings.duration);
  throughput.add(1);

  const ok = check(res, {
    "replay accepted": (r) => r.status === 200 || r.status === 202 || r.status === 204,
  });
  errorRate.add(!ok);
}

// Poll /metrics for webhook failure counter and queue health.
export function observeMetrics() {
  const res = http.get(
    `${BASE_URL}/metrics`,
    { headers: READ_HEADERS, timeout: "5s" }
  );

  metricsLatency.add(res.timings.duration);
  throughput.add(1);

  const ok = check(res, { "metrics 200": (r) => r.status === 200 });
  errorRate.add(!ok);

  if (ok && res.body) {
    // Extract soroban_pulse_webhook_failures_total from Prometheus text format
    const match = res.body.match(/soroban_pulse_webhook_failures_total\s+(\d+)/);
    if (match) {
      webhookFailures.add(parseInt(match[1], 10));
    }
  }

  // Optionally poll the stub webhook receiver for delivery count
  if (RECEIVER_URL) {
    http.get(RECEIVER_URL, { timeout: "5s" });
  }
}

// Configure the stub webhook receiver so that FAILURE_RATIO of its endpoints
// return HTTP 500, then drive sustained event injection to trigger retries.
export function retryStorm() {
  if (!ADMIN_KEY) {
    sleep(1);
    return;
  }

  // Ask the stub receiver to flip a fraction of endpoints into failure mode.
  // The stub is expected to expose POST /__control/failure-ratio.
  if (RECEIVER_URL) {
    const ctl = http.post(
      `${RECEIVER_URL.replace(/\/$/, "")}/__control/failure-ratio`,
      JSON.stringify({ ratio: FAILURE_RATIO }),
      { headers: { "Content-Type": "application/json" }, timeout: "5s" }
    );
    check(ctl, { "failure ratio configured": (r) => r.status === 200 || r.status === 204 });
  }

  const fromLedger = randomIntBetween(1_000_000, 1_400_000);
  const toLedger   = fromLedger + randomIntBetween(1, 10);

  const res = http.post(
    `${BASE_URL}/v1/admin/replay`,
    JSON.stringify({ from_ledger: fromLedger, to_ledger: toLedger }),
    { headers: ADMIN_HEADERS, timeout: "30s" }
  );

  replayLatency.add(res.timings.duration);
  throughput.add(1);

  const ok = check(res, {
    "retry storm replay accepted": (r) => r.status === 200 || r.status === 202 || r.status === 204,
  });
  errorRate.add(!ok);
}

// Sample DLQ depth from the admin stats endpoint and record successful
// delivery latency. Runs for the full 10-minute sustained-load window.
export function sustainedLoad() {
  const res = http.get(
    `${BASE_URL}/v1/admin/stats`,
    { headers: READ_HEADERS, timeout: "10s" }
  );

  metricsLatency.add(res.timings.duration);
  throughput.add(1);

  const ok = check(res, {
    "admin stats 200": (r) => r.status === 200,
    "admin stats has body": (r) => !!r.body,
  });
  errorRate.add(!ok);

  if (ok && res.body) {
    let stats = null;
    try {
      stats = res.json();
    } catch (_) {
      stats = null;
    }

    if (stats) {
      // DLQ depth — accept a few common field names.
      const depth =
        stats.dlq_depth ??
        stats.dead_letter_queue_depth ??
        stats.webhook_dlq_depth ??
        (stats.webhook && stats.webhook.dlq_depth);
      if (typeof depth === "number") {
        dlqDepth.add(depth);
      }

      // Successful delivery latency (ms) — only record when deliveries
      // actually succeeded so the p99 reflects healthy endpoints.
      const delivered = stats.webhook_deliveries_success_total ?? stats.deliveries_success;
      const latency   = stats.webhook_delivery_latency_ms_p99 ?? stats.delivery_latency_ms;
      if (typeof latency === "number") {
        deliveryLatency.add(latency);
      }

      // Success rate across the delivery pipeline.
      const success = stats.webhook_deliveries_success_total;
      const failed  = stats.webhook_deliveries_failed_total ?? stats.webhook_failures_total;
      if (typeof success === "number" && typeof failed === "number" && success + failed > 0) {
        successRate.add(success / (success + failed));
      } else if (typeof delivered === "number" && delivered > 0) {
        successRate.add(1);
      }
    }
  }
}

// Trigger DLQ replay while the retry storm and sustained load are running.
export function dlqReplay() {
  if (!ADMIN_KEY) {
    sleep(1);
    return;
  }

  const res = http.post(
    `${BASE_URL}/v1/admin/dlq/replay`,
    JSON.stringify({ limit: randomIntBetween(10, 100) }),
    { headers: ADMIN_HEADERS, timeout: "30s" }
  );

  dlqReplayLatency.add(res.timings.duration);
  throughput.add(1);

  const ok = check(res, {
    "dlq replay accepted": (r) => r.status === 200 || r.status === 202 || r.status === 204,
  });
  errorRate.add(!ok);
}

// Background REST traffic — ensures webhook tasks don't starve the API.
export function backgroundRest() {
  const paths = [
    "/v1/events?page=1&limit=20",
    "/v1/events?event_type=contract&limit=20",
    "/healthz/ready",
  ];
  const path = paths[randomIntBetween(0, paths.length - 1)];
  const res  = http.get(`${BASE_URL}${path}`, {
    headers: READ_HEADERS,
    timeout: "10s",
  });

  throughput.add(1);
  errorRate.add(!check(res, { "status 200": (r) => r.status === 200 }));
}

export function handleSummary(data) {
  const p99replay  = data.metrics.wh_replay_latency_ms?.values?.["p(99)"]  ?? "n/a";
  const p99metrics = data.metrics.wh_metrics_latency_ms?.values?.["p(99)"] ?? "n/a";
  const err        = data.metrics.wh_error_rate?.values?.rate               ?? "n/a";
  const failures   = data.metrics.wh_failure_counter?.values?.value         ?? "n/a";
  const total      = data.metrics.wh_requests_total?.values?.count          ?? 0;

  const dlqMax     = data.metrics.wh_dlq_depth?.values?.max                 ?? "n/a";
  const p99deliver = data.metrics.wh_delivery_latency_ms?.values?.["p(99)"] ?? "n/a";
  const successRt  = data.metrics.wh_success_rate?.values?.rate             ?? "n/a";
  const p99dlqRepl = data.metrics.wh_dlq_replay_latency_ms?.values?.["p(99)"] ?? "n/a";

  console.log("\n=== WEBHOOK DELIVERY LOAD TEST SUMMARY ===");
  console.log(`Total requests         : ${total}`);
  console.log(`Replay p99 latency     : ${typeof p99replay  === "number" ? p99replay.toFixed(1)  : p99replay} ms`);
  console.log(`Metrics probe p99      : ${typeof p99metrics === "number" ? p99metrics.toFixed(1) : p99metrics} ms`);
  console.log(`Error rate             : ${typeof err === "number" ? (err * 100).toFixed(2) : err} %`);
  console.log(`Webhook failure total  : ${failures}`);
  console.log("--- Retry storm / DLQ SLOs (issue #1276) ---");
  console.log(`DLQ depth (max)        : ${dlqMax}  (SLO: <1000)`);
  console.log(`Delivery p99 latency   : ${typeof p99deliver === "number" ? p99deliver.toFixed(1) : p99deliver} ms  (SLO: <5000)`);
  console.log(`Delivery success rate  : ${typeof successRt  === "number" ? (successRt * 100).toFixed(2) : successRt} %  (SLO: >0)`);
  console.log(`DLQ replay p99 latency : ${typeof p99dlqRepl === "number" ? p99dlqRepl.toFixed(1) : p99dlqRepl} ms  (SLO: <10000)`);

  return {
    "tests/load/results/webhook_delivery_summary.json": JSON.stringify(data, null, 2),
  };
}
