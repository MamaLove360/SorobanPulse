# Design Specs / RFCs

Proposals and design specifications. These describe planned or partially
implemented work and are not guaranteed to match the current code.

- [Webhook delivery reliability & observability](webhook-delivery-improvements/spec.md)
  — [executive summary](webhook-delivery-improvements/executive-summary.md),
  [quick start](webhook-delivery-improvements/quick-start.md),
  [implementation guide](../archive/IMPLEMENTATION_GUIDE.md) (archived),
  [testing & validation](webhook-delivery-improvements/testing-and-validation.md).
  [`proposed-migration.sql`](webhook-delivery-improvements/proposed-migration.sql)
  is the schema proposed by this spec. It is **not** part of `migrations/` and
  is never applied by the service; real schema changes go in `migrations/`.
