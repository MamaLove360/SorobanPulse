// Token transfer alert bot: receives SorobanPulse webhooks, keeps verified
// SEP-41 transfers above a threshold, and posts them to Discord.
// See docs/tutorials/token-alert-bot.md.

import { createServer, type IncomingMessage } from "node:http";
import { Configuration, EventsApi, verifyWebhookSignature } from "@soroban-pulse/sdk";
import { postTransferAlert } from "./discord.js";
import { decodeTransfer, formatAmount, toBaseUnits, type WebhookEvent } from "./transfer.js";

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    console.error(`Missing required environment variable ${name} (see .env.example)`);
    process.exit(1);
  }
  return value;
}

const port = Number(process.env.PORT ?? 8787);
const webhookSecret = required("WEBHOOK_SECRET");
const discordWebhookUrl = required("DISCORD_WEBHOOK_URL");
const contractId = required("TOKEN_CONTRACT_ID");
const symbol = process.env.TOKEN_SYMBOL ?? "XLM";
const decimals = Number(process.env.TOKEN_DECIMALS ?? 7);
const minAmount = toBaseUnits(process.env.MIN_AMOUNT ?? "100", decimals);
const pulseUrl = process.env.SOROBAN_PULSE_URL ?? "http://localhost:3000";
const explorerTxUrl =
  process.env.EXPLORER_TX_URL ?? "https://stellar.expert/explorer/testnet/tx/";

// Webhook deliveries are retried on failure, so the same event can arrive
// more than once. Remember recently alerted events to avoid double posts.
const seen = new Set<string>();
const SEEN_LIMIT = 10_000;

function readBody(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

async function handleEvent(event: WebhookEvent): Promise<void> {
  if (event.contractId !== contractId || event.inSuccessfulContractCall === false) return;

  const transfer = decodeTransfer(event);
  if (!transfer || transfer.amount < minAmount) return;

  const key = `${event.txHash}:${event.ledger}:${JSON.stringify(event.topic)}:${transfer.amount}`;
  if (seen.has(key)) return;
  if (seen.size >= SEEN_LIMIT) seen.clear();
  seen.add(key);

  console.log(
    `transfer ${formatAmount(transfer.amount, decimals)} ${symbol} ${transfer.from} → ${transfer.to} (tx ${event.txHash})`,
  );
  await postTransferAlert(transfer, event, {
    webhookUrl: discordWebhookUrl,
    symbol,
    decimals,
    explorerTxUrl,
  });
}

const server = createServer(async (req, res) => {
  if (req.method !== "POST" || req.url !== "/webhook") {
    res.writeHead(404).end();
    return;
  }

  // Verify against the raw bytes: re-serialising parsed JSON changes the signature.
  const body = await readBody(req);
  const signature = req.headers["x-signature-256"];
  let valid = false;
  try {
    valid =
      typeof signature === "string" &&
      verifyWebhookSignature(body, signature, webhookSecret).isValid;
  } catch {
    valid = false; // malformed header
  }
  if (!valid) {
    console.warn("rejected delivery with a missing or invalid X-Signature-256 header");
    res.writeHead(401).end();
    return;
  }

  // Acknowledge first so slow Discord calls never trigger SorobanPulse retries.
  res.writeHead(204).end();

  try {
    await handleEvent(JSON.parse(body.toString("utf8")) as WebhookEvent);
  } catch (err) {
    console.error("failed to process event:", err);
  }
});

/** Confirms SorobanPulse is reachable and indexing the token, using the SDK. */
async function checkSorobanPulse(): Promise<void> {
  const api = new EventsApi(
    new Configuration({ basePath: pulseUrl, apiKey: process.env.SOROBAN_PULSE_API_KEY }),
  );
  try {
    const res = await api.getEventsByContractRaw({ contractId, limit: 5 });
    const page = (await res.raw.json()) as { data?: unknown[] };
    console.log(
      `SorobanPulse at ${pulseUrl} has ${page.data?.length ?? 0} recent event(s) for ${contractId}`,
    );
  } catch (err) {
    console.warn(`could not reach SorobanPulse at ${pulseUrl}:`, (err as Error).message);
  }
}

server.listen(port, () => {
  console.log(
    `listening on http://localhost:${port}/webhook — alerting on ${symbol} transfers ≥ ${formatAmount(minAmount, decimals)}`,
  );
  void checkSorobanPulse();
});
