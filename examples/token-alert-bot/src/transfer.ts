// Decode SEP-41 `transfer` events from SorobanPulse webhook payloads.
//
// SorobanPulse forwards the RPC event as-is, so topics and value usually
// arrive as base64 XDR ScVals (either a bare string or `{ "xdr": "..." }`).
// Already-decoded JSON shapes are accepted too.

import { scValToNative, xdr } from "@stellar/stellar-sdk";

/** The fields of a SorobanPulse webhook payload this bot uses. */
export interface WebhookEvent {
  contractId: string;
  type: string;
  txHash: string;
  ledger: number;
  ledgerClosedAt: string;
  inSuccessfulContractCall?: boolean;
  topic?: unknown[] | null;
  value: unknown;
}

export interface Transfer {
  from: string;
  to: string;
  /** Raw i128 amount in the token's smallest unit. */
  amount: bigint;
  /** Trailing asset topic of Stellar Asset Contract transfers (e.g. "native"). */
  asset?: string;
}

function decodeScVal(raw: unknown): unknown {
  const b64 =
    typeof raw === "string"
      ? raw
      : raw && typeof raw === "object" && typeof (raw as { xdr?: unknown }).xdr === "string"
        ? (raw as { xdr: string }).xdr
        : undefined;
  if (b64 !== undefined) {
    try {
      return scValToNative(xdr.ScVal.fromXDR(b64, "base64"));
    } catch {
      // Not XDR — fall through and treat it as already-decoded JSON.
    }
  }
  return raw;
}

function asAmount(v: unknown): bigint | undefined {
  if (typeof v === "bigint") return v;
  if (typeof v === "number" && Number.isInteger(v)) return BigInt(v);
  if (typeof v === "string" && /^-?\d+$/.test(v)) return BigInt(v);
  if (v && typeof v === "object") {
    // CAP-67 muxed transfers carry `{ amount, to_muxed_id }`; JSON i128 as `{ i128 }`.
    const o = v as Record<string, unknown>;
    return asAmount(o.amount ?? o.i128);
  }
  return undefined;
}

function asText(v: unknown): string | undefined {
  if (typeof v === "string") return v;
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    const s = o.symbol ?? o.address ?? o.string;
    return typeof s === "string" ? s : undefined;
  }
  return undefined;
}

/** Returns the transfer, or `undefined` when the event is not a SEP-41 transfer. */
export function decodeTransfer(event: WebhookEvent): Transfer | undefined {
  const topics = (event.topic ?? []).map(decodeScVal);
  if (topics.length < 3 || asText(topics[0]) !== "transfer") return undefined;

  const from = asText(topics[1]);
  const to = asText(topics[2]);
  const amount = asAmount(decodeScVal(event.value));
  if (!from || !to || amount === undefined) return undefined;

  return { from, to, amount, asset: asText(topics[3]) };
}

/** Converts a whole-token threshold such as "100" or "0.5" into base units. */
export function toBaseUnits(amount: string, decimals: number): bigint {
  const match = /^(\d+)(?:\.(\d+))?$/.exec(amount.trim());
  if (!match) throw new Error(`invalid amount: ${amount}`);
  const [, whole, frac = ""] = match;
  if (frac.length > decimals) throw new Error(`${amount} has more than ${decimals} decimals`);
  return BigInt(whole + frac.padEnd(decimals, "0"));
}

/** Formats a base-unit amount for display, e.g. 1234500000n with 7 decimals → "123.45". */
export function formatAmount(amount: bigint, decimals: number): string {
  const negative = amount < 0n;
  const digits = (negative ? -amount : amount).toString().padStart(decimals + 1, "0");
  const whole = digits.slice(0, digits.length - decimals);
  const frac = digits.slice(digits.length - decimals).replace(/0+$/, "");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${negative ? "-" : ""}${grouped}${frac ? `.${frac}` : ""}`;
}
