#!/usr/bin/env ts-node
/**
 * sdk/conformance/runner_ts/runner.ts
 *
 * TypeScript SDK conformance runner.
 * Reads scenarios.yaml, executes each scenario against the live server, and
 * reports pass/fail in GitHub Actions step-summary format.
 *
 * Usage:
 *   CONFORMANCE_BASE_URL=http://localhost:3000 ts-node runner.ts
 *
 * Exit code: 0 = all pass, 1 = one or more failures.
 */

import * as fs from "fs";
import * as path from "path";
import * as http from "http";
import * as https from "https";
import { URL } from "url";

// ── Minimal YAML parser for our simple schema ────────────────────────────────
// We avoid adding a heavy dep; the schema only uses two levels of nesting.
import { parse as parseYaml } from "js-yaml";

// ── Types mirroring scenarios.yaml ───────────────────────────────────────────

interface Assertion {
  op: "eq" | "gte" | "lte" | "type" | "exists" | "len_gte";
  field: string;
  value?: unknown;
}

interface ScenarioExpect {
  status: number;
  headers?: Record<string, string>;
  body?: Assertion[];
}

interface Scenario {
  id: string;
  description: string;
  method: "GET" | "POST" | "DELETE" | "PUT" | "PATCH";
  path?: string;
  path_template?: string;
  query?: Record<string, string | number | boolean>;
  headers?: Record<string, string>;
  body?: unknown;
  expect: ScenarioExpect;
  sse?: boolean;
  sse_timeout_secs?: number;
  sse_min_events?: number;
  depends_on?: string;
  // Webhook-signature verification scenario support
  webhook_signature?: boolean;
  webhook_secret_env?: string;
  webhook_sink_env?: string;
  webhook_timeout_secs?: number;
}

interface ScenariosFile {
  base_url_env: string;
  scenarios: Scenario[];
}

// ── Result tracking ──────────────────────────────────────────────────────────

interface Result {
  id: string;
  description: string;
  passed: boolean;
  error?: string;
  durationMs: number;
}

/**
 * Normalized result shape shared across the TypeScript, Python and Go runners.
 * The CI job diffs the JSON lines emitted by each runner, so the field names
 * and ordering must stay identical between all three implementations.
 */
interface NormalizedResult {
  scenario: string;
  passed: boolean;
  error: string | null;
  duration_ms: number;
}

const results: Result[] = [];

// State shared between scenarios (e.g. subscription ID created by a prior step)
const state: Record<string, string> = {};

// ── Helpers ──────────────────────────────────────────────────────────────────

function resolveField(obj: unknown, fieldPath: string): unknown {
  // Supports paths like "data[0].id", "data", "total"
  const parts = fieldPath
    .replace(/\[(\d+)\]/g, ".$1")
    .split(".")
    .filter(Boolean);

  let current: unknown = obj;
  for (const part of parts) {
    if (current == null || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

function checkAssertion(body: unknown, assertion: Assertion): string | null {
  const value = resolveField(body, assertion.field);

  switch (assertion.op) {
    case "exists":
      if (value === undefined || value === null) {
        return `field "${assertion.field}" does not exist or is null`;
      }
      return null;

    case "type": {
      const actual =
        Array.isArray(value) ? "array" : typeof value;
      if (actual !== assertion.value) {
        return `field "${assertion.field}" expected type "${assertion.value}", got "${actual}"`;
      }
      return null;
    }

    case "eq": {
      const expectedJson = JSON.stringify(assertion.value);
      const actualJson = JSON.stringify(value);
      if (actualJson !== expectedJson) {
        return `field "${assertion.field}" expected ${expectedJson}, got ${actualJson}`;
      }
      return null;
    }

    case "gte":
      if (typeof value !== "number" || value < (assertion.value as number)) {
        return `field "${assertion.field}" expected >= ${assertion.value}, got ${value}`;
      }
      return null;

    case "lte":
      if (typeof value !== "number" || value > (assertion.value as number)) {
        return `field "${assertion.field}" expected <= ${assertion.value}, got ${value}`;
      }
      return null;

    case "len_gte": {
      if (!Array.isArray(value) || value.length < (assertion.value as number)) {
        const len = Array.isArray(value) ? value.length : "N/A";
        return `field "${assertion.field}" expected array length >= ${assertion.value}, got ${len}`;
      }
      return null;
    }

    default:
      return `unknown assertion op "${(assertion as Assertion).op}"`;
  }
}

/**
 * Verify an HMAC-SHA256 webhook signature in constant time.
 * Mirrors the Python (hmac.compare_digest) and Go (hmac.Equal) runners.
 */
function verifyWebhookSignature(
  secret: string,
  payload: string,
  signatureHeader: string
): boolean {
  const crypto = require("crypto") as typeof import("crypto");
  const expected =
    "sha256=" +
    crypto.createHmac("sha256", secret).update(payload, "utf8").digest("hex");
  const a = Buffer.from(expected, "utf8");
  const b = Buffer.from(signatureHeader.trim(), "utf8");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

/**
 * Poll the webhook sink until it receives a delivery, then verify the
 * signature of the captured payload. The sink is expected to expose
 * `GET {sink}/last` returning `{ "body": "<raw>", "signature": "<header>" }`.
 */
async function runWebhookSignatureScenario(
  scenario: Scenario,
  baseUrl: string
): Promise<{ passed: boolean; error?: string }> {
  const secretEnv = scenario.webhook_secret_env ?? "WEBHOOK_SECRET";
  const sinkEnv = scenario.webhook_sink_env ?? "WEBHOOK_SINK_URL";
  const secret = process.env[secretEnv];
  const sink = process.env[sinkEnv];

  if (!secret) return { passed: false, error: `missing env ${secretEnv}` };
  if (!sink) return { passed: false, error: `missing env ${sinkEnv}` };

  const timeoutSecs = scenario.webhook_timeout_secs ?? 15;
  const deadline = Date.now() + timeoutSecs * 1000;

  let last: { body?: string; signature?: string } | null = null;
  while (Date.now() < deadline) {
    try {
      const res = await makeRequest(
        buildUrl(sink, "/last"),
        "GET",
        {},
        undefined
      );
      if (res.status === 200 && res.body) {
        const parsed = JSON.parse(res.body) as {
          body?: string;
          signature?: string;
        };
        if (parsed && typeof parsed.body === "string") {
          last = parsed;
          break;
        }
      }
    } catch {
      // sink not ready yet; keep polling
    }
    await new Promise((r) => setTimeout(r, 250));
  }

  if (!last || typeof last.body !== "string" || !last.signature) {
    return {
      passed: false,
      error: `webhook sink received no delivery within ${timeoutSecs}s`,
    };
  }

  if (!verifyWebhookSignature(secret, last.body, last.signature)) {
    return {
      passed: false,
      error: `webhook signature mismatch (header="${last.signature}")`,
    };
  }

  return { passed: true };
}

function buildUrl(
  base: string,
  scenarioPath: string,
  query?: Record<string, string | number | boolean>
): string {
  const url = new URL(scenarioPath, base);
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      url.searchParams.set(k, String(v));
    }
  }
  return url.toString();
}

function makeRequest(
  urlStr: string,
  method: string,
  headers: Record<string, string>,
  body?: unknown
): Promise<{ status: number; headers: Record<string, string>; body: string }> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const isHttps = url.protocol === "https:";
    const lib = isHttps ? https : http;

    const bodyStr = body != null ? JSON.stringify(body) : undefined;
    const reqHeaders: Record<string, string> = {
      "User-Agent": "soroban-pulse-conformance-ts/1.0",
      Accept: "application/json",
      ...headers,
    };
    if (bodyStr) {
      reqHeaders["Content-Length"] = String(Buffer.byteLength(bodyStr));
    }

    const options: http.RequestOptions = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname + url.search,
      method,
      headers: reqHeaders,
    };

    const req = lib.request(options, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk: Buffer) => chunks.push(chunk));
      res.on("end", () => {
        const responseHeaders: Record<string, string> = {};
        for (const [k, v] of Object.entries(res.headers)) {
          if (v != null) responseHeaders[k] = Array.isArray(v) ? v[0] : v;
        }
        resolve({
          status: res.statusCode ?? 0,
          headers: responseHeaders,
          body: Buffer.concat(chunks).toString("utf8"),
        });
      });
      res.on("error", reject);
    });

    req.on("error", reject);

    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

/**
 * Connect to an SSE stream and collect events until we hit the timeout or the
 * minimum event count.
 */
function collectSSE(
  urlStr: string,
  timeoutSecs: number,
  minEvents: number
): Promise<{ events: string[]; headers: Record<string, string>; status: number }> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const isHttps = url.protocol === "https:";
    const lib = isHttps ? https : http;

    const options: http.RequestOptions = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname + url.search,
      method: "GET",
      headers: {
        Accept: "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": "soroban-pulse-conformance-ts/1.0",
      },
    };

    const events: string[] = [];
    let responseHeaders: Record<string, string> = {};
    let statusCode = 0;

    const deadline = setTimeout(() => {
      resolve({ events, headers: responseHeaders, status: statusCode });
    }, timeoutSecs * 1000);

    const req = lib.request(options, (res) => {
      statusCode = res.statusCode ?? 0;
      for (const [k, v] of Object.entries(res.headers)) {
        if (v != null)
          responseHeaders[k] = Array.isArray(v) ? v[0] : v;
      }

      let buffer = "";
      res.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          if (line.startsWith("data: ")) {
            events.push(line.slice(6));
            if (events.length >= minEvents) {
              clearTimeout(deadline);
              req.destroy();
              resolve({ events, headers: responseHeaders, status: statusCode });
              return;
            }
          }
        }
      });
      res.on("end", () => {
        clearTimeout(deadline);
        resolve({ events, headers: responseHeaders, status: statusCode });
      });
      res.on("error", (err) => {
        clearTimeout(deadline);
        reject(err);
      });
    });

    req.on("error", (err) => {
      clearTimeout(deadline);
      // If the connection was destroyed by us after collecting enough events, ignore.
      if ((err as NodeJS.ErrnoException).code === "ECONNRESET") {
        resolve({ events, headers: responseHeaders, status: statusCode });
      } else {
        reject(err);
      }
    });

    req.end();
  });
}

// ── Scenario runner ──────────────────────────────────────────────────────────

async function runScenario(
  scenario: Scenario,
  baseUrl: string
): Promise<Result> {
  const start = Date.now();

  try {
    // Resolve path (template substitution for depends_on results)
    let resolvedPath = scenario.path ?? "";
    if (scenario.path_template) {
      resolvedPath = scenario.path_template.replace(
        /\{(\w+)\}/g,
        (_, key) => state[key] ?? `{${key}}`
      );
    }

    // ── Webhook-signature scenario ──────────────────────────────────────────
    if (scenario.webhook_signature) {
      const { passed, error } = await runWebhookSignatureScenario(
        scenario,
        baseUrl
      );
      return {
        id: scenario.id,
        description: scenario.description,
        passed,
        error,
        durationMs: Date.now() - start,
      };
    }

    // ── SSE scenario ────────────────────────────────────────────────────────
    if (scenario.sse) {
      const url = buildUrl(baseUrl, resolvedPath, scenario.query);
      const timeoutSecs = scenario.sse_timeout_secs ?? 10;
      const minEvents = scenario.sse_min_events ?? 1;
      const { events, headers, status } = await collectSSE(
        url,
        timeoutSecs,
        minEvents
      );

      const errors: string[] = [];

      if (status !== scenario.expect.status) {
        errors.push(
          `status: expected ${scenario.expect.status}, got ${status}`
        );
      }

      if (scenario.expect.headers) {
        for (const [k, v] of Object.entries(scenario.expect.headers)) {
          const actual = headers[k.toLowerCase()] ?? "";
          if (!actual.includes(v)) {
            errors.push(
              `header "${k}": expected to include "${v}", got "${actual}"`
            );
          }
        }
      }

      if (events.length < minEvents) {
        errors.push(
          `SSE: expected at least ${minEvents} event(s), got ${events.length} within ${timeoutSecs}s`
        );
      }

      return {
        id: scenario.id,
        description: scenario.description,
        passed: errors.length === 0,
        error: errors.join("; ") || undefined,
        durationMs: Date.now() - start,
      };
    }

    // ── HTTP scenario ────────────────────────────────────────────────────────
    const url = buildUrl(baseUrl, resolvedPath, scenario.query);
    const reqHeaders: Record<string, string> = scenario.headers ?? {};

    const response = await makeRequest(
      url,
      scenario.method,
      reqHeaders,
      scenario.body
    );

    const errors: string[] = [];

    // Status check
    if (response.status !== scenario.expect.status) {
      errors.push(
        `status: expected ${scenario.expect.status}, got ${response.status}`
      );
    }

    // Header checks
    if (scenario.expect.headers) {
      for (const [k, v] of Object.entries(scenario.expect.headers)) {
        const actual = response.headers[k.toLowerCase()] ?? "";
        if (!actual.includes(v)) {
          errors.push(
            `header "${k}": expected to include "${v}", got "${actual}"`
          );
        }
      }
    }

    // Body assertion checks
    if (scenario.expect.body && scenario.expect.body.length > 0) {
      let parsed: unknown = null;
      try {
        parsed = JSON.parse(response.body);
      } catch {
        errors.push(`body is not valid JSON: ${response.body.slice(0, 200)}`);
      }

      if (parsed !== null) {
        for (const assertion of scenario.expect.body) {
          const err = checkAssertion(parsed, assertion);
          if (err) errors.push(err);
        }

        // Store subscription ID for delete_subscription scenario
        if (
          scenario.id === "create_subscription" &&
          errors.length === 0 &&
          parsed !== null
        ) {
          const id = resolveField(parsed, "id");
          if (typeof id === "string") state["subscription_id"] = id;
        }
      }
    }

    return {
      id: scenario.id,
      description: scenario.description,
      passed: errors.length === 0,
      error: errors.join("; ") || undefined,
      durationMs: Date.now() - start,
    };
  } catch (err: unknown) {
    return {
      id: scenario.id,
      description: scenario.description,
      passed: false,
      error: err instanceof Error ? err.message : String(err),
      durationMs: Date.now() - start,
    };
  }
}

// ── Output helpers ────────────────────────────────────────────────────────────

function normalize(r: Result): NormalizedResult {
  return {
    scenario: r.id,
    passed: r.passed,
    error: r.error ?? null,
    duration_ms: r.durationMs,
  };
}

function printResult(r: Result): void {
  const icon = r.passed ? "✅" : "❌";
  const duration = `${r.durationMs}ms`;
  console.log(`${icon} [${r.id}] ${r.description} (${duration})`);
  if (!r.passed && r.error) {
    console.log(`   └─ ${r.error}`);
  }
}

/**
 * Emit the normalized JSON result line. The CI job greps for the
 * `CONFORMANCE_RESULT` prefix and diffs the payloads across SDKs, so the
 * prefix and JSON shape must match the Python and Go runners exactly.
 */
function emitNormalized(r: Result): void {
  console.log(`CONFORMANCE_RESULT ${JSON.stringify(normalize(r))}`);
}

function writeSummary(results: Result[], sdk: string): void {
  const summaryPath = process.env["GITHUB_STEP_SUMMARY"];
  if (!summaryPath) return;

  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;
  const total = results.length;
  const badge = failed === 0 ? "🟢" : "🔴";

  let md = `## ${badge} SDK Conformance — ${sdk} (${passed}/${total} passed)\n\n`;
  md += `| # | Scenario | Status | Duration | Error |\n`;
  md += `|---|----------|--------|----------|-------|\n`;
  for (const r of results) {
    const status = r.passed ? "✅ Pass" : "❌ Fail";
    const error = r.error ? r.error.replace(/\|/g, "\\|") : "";
    md += `| | \`${r.id}\` | ${status} | ${r.durationMs}ms | ${error} |\n`;
  }
  md += `\n**Total:** ${total} &nbsp;|&nbsp; **Passed:** ${passed} &nbsp;|&nbsp; **Failed:** ${failed}\n`;

  // Machine-readable block so the CI job can diff normalized results across SDKs.
  md += `\n<details><summary>Normalized results (JSON)</summary>\n\n`;
  md += "```json\n";
  for (const r of results) {
    md += JSON.stringify(normalize(r)) + "\n";
  }
  md += "```\n\n</details>\n";

  fs.appendFileSync(summaryPath, md);
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const scenariosPath = path.resolve(__dirname, "../scenarios.yaml");
  const raw = fs.readFileSync(scenariosPath, "utf8");
  const parsed = parseYaml(raw) as ScenariosFile;

  const envKey = parsed.base_url_env ?? "CONFORMANCE_BASE_URL";
  const baseUrl = process.env[envKey] ?? "http://localhost:3000";

  console.log(`\n=== SorobanPulse SDK Conformance — TypeScript ===`);
  console.log(`Base URL: ${baseUrl}`);
  console.log(`Scenarios: ${parsed.scenarios.length}\n`);

  for (const scenario of parsed.scenarios) {
    const result = await runScenario(scenario, baseUrl);
    results.push(result);
    printResult(result);
    emitNormalized(result);
  }

  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;

  console.log(`\n--- Summary ---`);
  console.log(`Total: ${results.length} | Passed: ${passed} | Failed: ${failed}`);

  writeSummary(results, "typescript");

  if (failed > 0) {
    console.error(`\n${failed} scenario(s) failed.`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
