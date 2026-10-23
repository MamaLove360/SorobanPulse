# Tutorial: Build a Token Transfer Alert Bot

In this tutorial you index a SEP-41 token on Stellar testnet with SorobanPulse,
receive a signed webhook for each transfer, and post every transfer above an
amount threshold to a Discord channel.

```
Stellar testnet ──► SorobanPulse ──(signed webhook)──► alert bot ──► Discord
   (Soroban RPC)     index + filter     X-Signature-256    verify + threshold
```

**Time:** about 30 minutes. **Code:** [`examples/token-alert-bot/`](https://github.com/Soroban-Pulse/SorobanPulse/tree/main/examples/token-alert-bot)
(TypeScript, using the SorobanPulse SDK).

## What you need

- Rust (stable) and Docker, to run SorobanPulse and PostgreSQL
- Node.js 20.6 or newer
- The [Stellar CLI](https://developers.stellar.org/docs/tools/cli/install-cli), to make a testnet transfer
- A Discord server where you can create a channel webhook

## 1. Pick the token to watch

Any SEP-41 token works. This tutorial uses the **native XLM Stellar Asset
Contract (SAC)** on testnet, because every testnet account already holds XLM.
Print its contract ID:

```bash
stellar contract id asset --asset native --network testnet
# CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
```

To watch your own token, use its contract ID instead. A SEP-41 `transfer`
event has topics `["transfer", from, to]` (a SAC adds a trailing asset topic)
and an `i128` amount as its value.

## 2. Run SorobanPulse locally against testnet

From the repository root, start PostgreSQL:

```bash
docker compose up -d db
```

Create `.env` from the template and set these values (everything else can
keep its default; see the [configuration reference](../configuration.md)):

```bash
cp .env.example .env
```

```ini
DATABASE_URL=postgres://soroban:soroban@localhost:5432/soroban_pulse
STELLAR_RPC_URL=https://soroban-testnet.stellar.org
CHAIN_ID=testnet
START_LEDGER=0
INDEXER_EVENT_TYPES=contract

# Deliver every indexed event of the token to the bot, signed with HMAC-SHA256.
WEBHOOK_URL=http://localhost:8787/webhook
WEBHOOK_SECRET=change-me-to-a-long-random-string
WEBHOOK_CONTRACT_FILTER=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
```

`WEBHOOK_CONTRACT_FILTER` makes SorobanPulse deliver only this token's events,
and `WEBHOOK_SECRET` makes it sign each delivery with an `X-Signature-256`
header. `START_LEDGER=0` starts from the latest ledger, so you only see new
transfers.

Start the service:

```bash
cargo run
```

Wait for the indexer to log that it is polling, then check it is up:

```bash
curl http://localhost:3000/healthz/ready
```

> **Why the webhook channel and not `POST /v1/subscriptions`?** Per-subscription
> callbacks deliver every event unfiltered and refuse loopback URLs such as
> `localhost` (SSRF protection), so they need a public URL. The webhook channel
> above supports a contract filter, signing, and local delivery, which is what
> this bot needs. The amount threshold is applied in the bot (step 4), because
> SorobanPulse forwards the amount as raw XDR.

## 3. Create a Discord webhook

In Discord, open the channel's **Settings → Integrations → Webhooks → New
Webhook**, then **Copy Webhook URL**. Anyone with the URL can post to the
channel, so keep it secret.

## 4. Run the bot

```bash
cd examples/token-alert-bot
cp .env.example .env
npm install          # also builds the local SDK in sdk/typescript
```

Edit `.env`:

```ini
WEBHOOK_SECRET=change-me-to-a-long-random-string   # same value as SorobanPulse
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<id>/<token>
TOKEN_CONTRACT_ID=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
TOKEN_SYMBOL=XLM
TOKEN_DECIMALS=7
MIN_AMOUNT=100
```

Start it:

```bash
npm start
# listening on http://localhost:8787/webhook — alerting on XLM transfers ≥ 100
# SorobanPulse at http://localhost:3000 has 5 recent event(s) for CDLZ…CYSC
```

The second line comes from the SDK's `EventsApi`, which the bot uses at
startup to confirm SorobanPulse is reachable and indexing the token.

### How the bot works

Everything lives in three small files in `src/`:

- **`index.ts`** receives `POST /webhook`, verifies the signature against the
  *raw* request body with the SDK's `verifyWebhookSignature`, and replies
  `204` straight away so a slow Discord call never triggers SorobanPulse
  retries. Requests with a missing or wrong signature get `401`.

  ```ts
  const body = await readBody(req);
  const signature = req.headers["x-signature-256"];
  valid = typeof signature === "string" &&
    verifyWebhookSignature(body, signature, webhookSecret).isValid;
  ```

- **`transfer.ts`** decodes the event. Topics and value arrive as base64 XDR
  ScVals, which `@stellar/stellar-sdk` turns into native values. Amounts stay
  `bigint`s, so large `i128` values never lose precision. `MIN_AMOUNT` is
  converted to base units with `TOKEN_DECIMALS`.

  ```ts
  const transfer = decodeTransfer(event);
  if (!transfer || transfer.amount < minAmount) return;
  ```

- **`discord.ts`** posts an embed with the amount, sender, recipient, ledger
  and a block-explorer link to the transaction.

Webhooks can be retried, so the bot remembers recently alerted events and
skips duplicates.

## 5. Send a real testnet transfer

Create and fund two testnet accounts, then transfer 150 XLM through the token
contract (amounts are in stroops, with 7 decimals):

```bash
stellar keys generate alice --network testnet --fund
stellar keys generate bob --network testnet --fund

stellar contract invoke \
  --id CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC \
  --source alice --network testnet \
  -- transfer --from alice --to bob --amount 1500000000
```

Within a few ledgers (about 5–15 seconds) the bot logs the transfer, and your
Discord channel shows the alert. A transfer below `MIN_AMOUNT`, such as
`--amount 10000000` (1 XLM), is delivered to the bot but not posted.

## Troubleshooting

| Symptom | Check |
|---|---|
| Bot logs `rejected delivery … X-Signature-256` | `WEBHOOK_SECRET` must be identical in both `.env` files. Restart SorobanPulse after changing it. |
| Bot receives nothing | SorobanPulse logs `Webhook delivery enabled` at startup when `WEBHOOK_URL` is set. Confirm `WEBHOOK_CONTRACT_FILTER` is the token's contract ID and the bot listens on the port in `WEBHOOK_URL`. |
| SorobanPulse runs in Docker Compose | `localhost` is the container itself. Use `WEBHOOK_URL=http://host.docker.internal:8787/webhook` (on Linux, add `extra_hosts: ["host.docker.internal:host-gateway"]` to the `app` service). |
| No events indexed | `START_LEDGER=0` only picks up new ledgers. Check `curl localhost:3000/status` shows the indexer advancing and that `STELLAR_RPC_URL` points to testnet. |
| `could not reach SorobanPulse` at bot startup | Only the startup check failed. Set `SOROBAN_PULSE_URL`, and `SOROBAN_PULSE_API_KEY` if `API_KEY` is set on the server. |
| Discord returns `401`/`404` | The webhook was deleted or the URL is incomplete; copy it again. |
| Transfers appear but never alert | Lower `MIN_AMOUNT`, and check `TOKEN_DECIMALS` matches the token (`decimals()` on the contract). |

Run the bot's unit tests with `npm test`.

## Next steps

- **Watch several tokens:** list more contract IDs in `WEBHOOK_CONTRACT_FILTER`
  and keep a per-contract threshold map in the bot.
- **Deploy it:** run the bot behind HTTPS and set `WEBHOOK_REQUIRE_HTTPS=true`;
  see [webhook signing](../webhook-signing.md) and [webhook verification](../webhook-verification.md).
- **Use Discord natively:** SorobanPulse can post to Discord itself; see the
  [Discord integration](../discord-integration.md).
- **Stream instead of push:** subscribe to `GET /v1/events/contract/{contract_id}/stream`
  with the SDK's SSE client; see [SSE reconnection](../sse-reconnection.md).
- **Decode more token events:** `mint`, `burn` and `clawback` share the same
  shape; see [contract event schemas](../contract-event-schemas.md).
