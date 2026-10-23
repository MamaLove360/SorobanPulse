//! Issue #816: schema registry developer CLI.
//!
//! Usage:
//!   schema_cli validate <schema.json> <event.json>
//!   schema_cli compat <old-schema.json> <new-schema.json> [backward|forward|full]
//!   schema_cli gen <schema.json>
//!   schema_cli doc <contract_id> <version> <schema.json>
// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use serde_json::Value;
use soroban_pulse::schema_validator::{
    check_compatibility, document_schema, generate_test_data, CompatibilityMode,
};
use std::process::ExitCode;

fn read_json(path: &str) -> Result<Value, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("{}: {}", path, e))?;
    serde_json::from_str(&raw).map_err(|e| format!("{}: {}", path, e))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(output) => {
            println!("{}", output);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<String, String> {
    match args.first().map(String::as_str) {
        Some("validate") => {
            let schema = read_json(args.get(1).ok_or("missing <schema.json>")?)?;
            let event = read_json(args.get(2).ok_or("missing <event.json>")?)?;
            let compiled = jsonschema::JSONSchema::options()
                .with_draft(jsonschema::Draft::Draft7)
                .compile(&schema)
                .map_err(|e| format!("invalid schema: {}", e))?;

            match compiled.validate(&event) {
                Ok(()) => Ok("valid".to_string()),
                Err(errors) => {
                    let messages: Vec<String> = errors
                        .map(|e| format!("  {} at {}", e, e.instance_path))
                        .collect();
                    Err(format!("invalid:\n{}", messages.join("\n")))
                }
            }
        }
        Some("compat") => {
            let old = read_json(args.get(1).ok_or("missing <old-schema.json>")?)?;
            let new = read_json(args.get(2).ok_or("missing <new-schema.json>")?)?;
            let mode = match args.get(3).map(String::as_str).unwrap_or("backward") {
                "backward" => CompatibilityMode::Backward,
                "forward" => CompatibilityMode::Forward,
                "full" => CompatibilityMode::Full,
                "none" => CompatibilityMode::None,
                other => return Err(format!("unknown compatibility mode: {}", other)),
            };

            let report = check_compatibility(&old, &new, mode);
            let rendered = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
            if report.compatible {
                Ok(rendered)
            } else {
                Err(rendered)
            }
        }
        Some("gen") => {
            let schema = read_json(args.get(1).ok_or("missing <schema.json>")?)?;
            serde_json::to_string_pretty(&generate_test_data(&schema)).map_err(|e| e.to_string())
        }
        Some("doc") => {
            let contract_id = args.get(1).ok_or("missing <contract_id>")?;
            let version: i32 = args
                .get(2)
                .ok_or("missing <version>")?
                .parse()
                .map_err(|_| "version must be an integer".to_string())?;
            let schema = read_json(args.get(3).ok_or("missing <schema.json>")?)?;
            Ok(document_schema(contract_id, version, &schema))
        }
        _ => Err(
            "usage: schema_cli <validate|compat|gen|doc> ... (see module docs)".to_string(),
        ),
    }
}
