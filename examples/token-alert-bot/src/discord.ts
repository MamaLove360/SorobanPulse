// Minimal Discord channel-webhook client.

import type { Transfer, WebhookEvent } from "./transfer.js";
import { formatAmount } from "./transfer.js";

export interface AlertOptions {
  webhookUrl: string;
  symbol: string;
  decimals: number;
  explorerTxUrl: string;
}

const shorten = (address: string) => `${address.slice(0, 6)}…${address.slice(-6)}`;

export async function postTransferAlert(
  transfer: Transfer,
  event: WebhookEvent,
  opts: AlertOptions,
): Promise<void> {
  const body = {
    username: "SorobanPulse",
    embeds: [
      {
        title: `🚨 ${formatAmount(transfer.amount, opts.decimals)} ${opts.symbol} transferred`,
        url: `${opts.explorerTxUrl}${event.txHash}`,
        color: 0x7c3aed,
        fields: [
          { name: "From", value: `\`${shorten(transfer.from)}\``, inline: true },
          { name: "To", value: `\`${shorten(transfer.to)}\``, inline: true },
          { name: "Ledger", value: String(event.ledger), inline: true },
        ],
        timestamp: event.ledgerClosedAt,
      },
    ],
  };

  const res = await fetch(opts.webhookUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`Discord responded ${res.status}: ${await res.text()}`);
  }
}
