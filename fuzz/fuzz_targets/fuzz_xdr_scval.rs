#![no_main]
//! Fuzz target: XDR / ScVal decoding (Issue #1154)
//!
//! Feed arbitrary bytes into every ScVal decoding and rendering path.
//! The target must NOT:
//! - panic
//! - abort
//! - overflow the stack
//!
//! Invariants checked:
//! 1. `decode_scval` never panics on arbitrary JSON input (deep nesting is
//!    rejected before decoding, issue #1226).
//! 2. `validate_xdr` never panics on arbitrary input.
//! 3. Every successfully parsed ScVal can be rendered in every `ScValFormat`
//!    without panicking.
//! 4. Determinism: same bytes → same Ok/Err outcome.
//!
//! ## Running
//!
//! ```bash
//! cargo +nightly fuzz run fuzz_xdr_scval -- -max_total_time=300 -rss_limit_mb=512
//! ```
//!
//! Seeds are in `fuzz/corpus/fuzz_xdr_scval/`.

use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use soroban_pulse::scval_format::{decode_scval, render, ScValFormat};
use soroban_pulse::xdr_validation::{validate_contract_id, validate_tx_hash, validate_xdr};

fuzz_target!(|data: &[u8]| {
    // ── 1. Parse as JSON Value ────────────────────────────────────────────
    let value: Value = match serde_json::from_slice(data) {
        Ok(v) => v,
        Err(_) => return,
    };

    // ── 2. Try to deserialize as ScVal ────────────────────────────────────
    let sc_val_result = decode_scval(&value);

    // ── 3. Render in every ScValFormat — must not panic ───────────────────
    for format in [ScValFormat::Json, ScValFormat::Native, ScValFormat::Xdr] {
        let _ = render(&value, format);
    }

    // ── 4. validate_xdr on arbitrary string slices ────────────────────────
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = validate_contract_id(s);
        let _ = validate_tx_hash(s);
        let _ = validate_xdr(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
            12345,
            &value,
            None,
        );
    }

    // ── 5. Determinism ────────────────────────────────────────────────────
    let sc_val_result2 = decode_scval(&value);
    assert_eq!(sc_val_result.is_some(), sc_val_result2.is_some());
});
