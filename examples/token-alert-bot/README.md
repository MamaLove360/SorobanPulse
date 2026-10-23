# Token transfer alert bot

Receives signed SorobanPulse webhooks, keeps SEP-41 `transfer` events above an
amount threshold, and posts them to a Discord channel.

Follow the full walkthrough in [docs/tutorials/token-alert-bot.md](../../docs/tutorials/token-alert-bot.md).

```bash
cp .env.example .env   # fill in WEBHOOK_SECRET and DISCORD_WEBHOOK_URL
npm install            # also builds the local SDK in sdk/typescript
npm start
npm test
```
