import assert from "node:assert/strict";
import { test } from "node:test";
import { Address, Keypair, nativeToScVal, xdr } from "@stellar/stellar-sdk";
import { decodeTransfer, formatAmount, toBaseUnits } from "../src/transfer.js";

const from = Keypair.random().publicKey();
const to = Keypair.random().publicKey();
const b64 = (v: xdr.ScVal) => v.toXDR("base64");

function event(topic: unknown[], value: unknown) {
  return {
    contractId: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
    type: "contract",
    txHash: "abc",
    ledger: 1,
    ledgerClosedAt: "2026-01-01T00:00:00Z",
    topic,
    value,
  };
}

test("decodes an XDR transfer with a trailing SAC asset topic", () => {
  const t = decodeTransfer(
    event(
      [
        b64(xdr.ScVal.scvSymbol("transfer")),
        { xdr: b64(new Address(from).toScVal()) },
        b64(new Address(to).toScVal()),
        b64(nativeToScVal("native")),
      ],
      { xdr: b64(nativeToScVal(1_500_000_000n, { type: "i128" })) },
    ),
  );
  assert.deepEqual(t, { from, to, amount: 1_500_000_000n, asset: "native" });
});

test("ignores non-transfer events", () => {
  assert.equal(
    decodeTransfer(event([b64(xdr.ScVal.scvSymbol("mint")), from, to], "1")),
    undefined,
  );
});

test("accepts already-decoded JSON shapes", () => {
  const t = decodeTransfer(event(["transfer", from, to], { i128: "42" }));
  assert.equal(t?.amount, 42n);
});

test("converts and formats amounts exactly", () => {
  assert.equal(toBaseUnits("100", 7), 1_000_000_000n);
  assert.equal(toBaseUnits("0.5", 7), 5_000_000n);
  assert.equal(formatAmount(12_345_000_000n, 7), "1,234.5");
  assert.throws(() => toBaseUnits("1.12345678", 7));
});
