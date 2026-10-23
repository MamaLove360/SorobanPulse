//! Postman Collection v2.1 generator.
//!
//! Reads an OpenAPI 3.0 JSON spec and emits:
//!   - A Postman Collection with requests grouped by tag, example bodies,
//!     and authentication presets.
//!   - Postman Environment files for local, testnet, and mainnet.
//!
//! Usage:
//!   # Pipe from the OpenAPI generator
//!   cargo run --bin gen_openapi | cargo run --bin gen_postman
//!
//!   # From a saved spec file
//!   cargo run --bin gen_postman -- --input openapi.json
//!
//!   # Custom output directory
//!   cargo run --bin gen_postman -- --input openapi.json --output-dir postman/
//!
//!   # Dry-run: print collection JSON to stdout
//!   cargo run --bin gen_postman -- --dry-run
// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
};
use uuid::Uuid;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return Ok(());
    }

    let input_file = flag_value(&args, "--input");
    let output_dir = flag_value(&args, "--output-dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("postman"));
    let dry_run = args.iter().any(|a| a == "--dry-run");

    // Read OpenAPI spec
    let spec: Value = if let Some(ref path) = input_file {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read '{}': {e}", path))?;
        serde_json::from_str(&content)?
    } else {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        serde_json::from_str(&buf)?
    };

    let collection = build_collection(&spec);
    let environments = build_environments();

    if dry_run {
        println!("{}", serde_json::to_string_pretty(&collection)?);
        return Ok(());
    }

    write_outputs(&collection, &environments, &output_dir)?;

    let item_count = count_requests(&collection);
    eprintln!(
        "✓ Postman collection ({item_count} requests) → {}/Soroban_Pulse.postman_collection.json",
        output_dir.display()
    );
    eprintln!("✓ Environments → {}/environments/", output_dir.display());

    Ok(())
}

// ---------------------------------------------------------------------------
// Collection builder
// ---------------------------------------------------------------------------

fn build_collection(spec: &Value) -> Value {
    let title = spec
        .pointer("/info/title")
        .and_then(Value::as_str)
        .unwrap_or("Soroban Pulse API");
    let description = spec
        .pointer("/info/description")
        .and_then(Value::as_str)
        .unwrap_or("");
    let version = spec
        .pointer("/info/version")
        .and_then(Value::as_str)
        .unwrap_or("0.1.0");

    // Group operations by tag
    let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let empty_map = Map::new();
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .unwrap_or(&empty_map);

    for (path, path_item) in paths {
        let path_item = match path_item.as_object() {
            Some(o) => o,
            None => continue,
        };
        for method in &["get", "post", "put", "patch", "delete"] {
            let Some(op) = path_item.get(*method) else { continue };
            let tag = op
                .pointer("/tags/0")
                .and_then(Value::as_str)
                .unwrap_or("Other")
                .to_string();

            let item = build_request_item(path, method, op, spec);
            groups.entry(tag).or_default().push(item);
        }
    }

    // Build folder items
    let folders: Vec<Value> = groups
        .into_iter()
        .map(|(tag, items)| {
            json!({
                "name": tag,
                "item": items
            })
        })
        .collect();

    json!({
        "info": {
            "_postman_id": Uuid::new_v4().to_string(),
            "name": title,
            "description": format!("{description}\n\nVersion: {version}"),
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
        },
        "auth": collection_auth(),
        "variable": collection_variables(),
        "item": folders
    })
}

fn build_request_item(path: &str, method: &str, op: &Value, spec: &Value) -> Value {
    let name = op
        .get("summary")
        .or_else(|| op.get("operationId"))
        .and_then(Value::as_str)
        .unwrap_or(path)
        .to_string();

    let description = op
        .get("description")
        .or_else(|| op.get("summary"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let (url, path_params, query_params) = build_url(path, op);
    let headers = build_headers(op);
    let auth = build_auth(path, op);
    let body = build_body(op, spec);

    let mut request = json!({
        "method": method.to_uppercase(),
        "url": url,
        "header": headers,
        "description": description,
    });

    if let Some(a) = auth {
        request["auth"] = a;
    }
    if !body.is_null() {
        request["body"] = body;
    }

    let example_response = build_example_response(op);

    json!({
        "name": name,
        "request": request,
        "response": [example_response]
    })
}

// ---------------------------------------------------------------------------
// URL builder
// ---------------------------------------------------------------------------

fn build_url(path: &str, op: &Value) -> (Value, Vec<Value>, Vec<Value>) {
    let raw = format!("{{{{base_url}}}}{path}");

    // Path segments — replace {param} with :param for Postman
    let path_segments: Vec<Value> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.starts_with('{') && s.ends_with('}') {
                let name = &s[1..s.len() - 1];
                json!(format!(":{name}"))
            } else {
                json!(s)
            }
        })
        .collect();

    // Collect parameters
    let empty = vec![];
    let params = op
        .get("parameters")
        .and_then(Value::as_array)
        .unwrap_or(&empty);

    let mut path_params: Vec<Value> = Vec::new();
    let mut query_params: Vec<Value> = Vec::new();

    for param in params {
        let name = param.get("name").and_then(Value::as_str).unwrap_or("");
        let r#in = param.get("in").and_then(Value::as_str).unwrap_or("");
        let required = param.get("required").and_then(Value::as_bool).unwrap_or(false);
        let description = param.get("description").and_then(Value::as_str).unwrap_or("");
        let example = param
            .pointer("/schema/example")
            .or_else(|| param.get("example"))
            .cloned()
            .unwrap_or(Value::Null);

        match r#in {
            "path" => {
                path_params.push(json!({
                    "key": name,
                    "value": if example.is_null() { json!("") } else { json!(example.to_string().trim_matches('"').to_string()) },
                    "description": description
                }));
            }
            "query" => {
                query_params.push(json!({
                    "key": name,
                    "value": if example.is_null() { json!("") } else { json!(example.to_string().trim_matches('"').to_string()) },
                    "description": description,
                    "disabled": !required
                }));
            }
            _ => {}
        }
    }

    let url = json!({
        "raw": raw,
        "host": ["{{base_url}}"],
        "path": path_segments,
        "variable": path_params,
        "query": query_params
    });

    (url, path_params, query_params)
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

fn build_headers(op: &Value) -> Vec<Value> {
    let mut headers = vec![
        json!({"key": "Content-Type", "value": "application/json", "type": "text"}),
    ];

    // Add any explicit header parameters
    let empty = vec![];
    let params = op
        .get("parameters")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    for param in params {
        if param.get("in").and_then(Value::as_str) == Some("header") {
            let name = param.get("name").and_then(Value::as_str).unwrap_or("");
            headers.push(json!({
                "key": name,
                "value": "",
                "description": param.get("description").and_then(Value::as_str).unwrap_or("")
            }));
        }
    }

    headers
}

// ---------------------------------------------------------------------------
// Authentication presets
// ---------------------------------------------------------------------------

fn build_auth(path: &str, op: &Value) -> Option<Value> {
    // Check if the operation explicitly requires no security
    if let Some(sec) = op.get("security") {
        if sec.as_array().map(|a| a.is_empty()).unwrap_or(false) {
            return Some(json!({"type": "noauth"}));
        }
    }

    // Admin endpoints use the admin key variable
    if path.starts_with("/admin") {
        return Some(json!({
            "type": "apikey",
            "apikey": [
                {"key": "key",   "value": "x-api-key",         "type": "string"},
                {"key": "value", "value": "{{admin_api_key}}", "type": "string"},
                {"key": "in",    "value": "header",            "type": "string"}
            ]
        }));
    }

    // Public endpoints (health, docs, unsubscribe) inherit no auth
    let no_auth_paths: HashSet<&str> = [
        "/health", "/healthz/live", "/healthz/ready", "/metrics",
        "/openapi.json", "/docs", "/unsubscribe",
        "/notifications/email/track/{token}", "/notifications/email/click/{token}",
    ]
    .into();

    if no_auth_paths.contains(path) {
        return Some(json!({"type": "noauth"}));
    }

    // Everything else inherits collection-level auth (api_key)
    None
}

fn collection_auth() -> Value {
    json!({
        "type": "apikey",
        "apikey": [
            {"key": "key",   "value": "x-api-key",  "type": "string"},
            {"key": "value", "value": "{{api_key}}", "type": "string"},
            {"key": "in",    "value": "header",      "type": "string"}
        ]
    })
}

fn collection_variables() -> Vec<Value> {
    vec![
        json!({"key": "base_url",       "value": "http://localhost:3000"}),
        json!({"key": "api_key",        "value": ""}),
        json!({"key": "admin_api_key",  "value": ""}),
        json!({"key": "contract_id",    "value": ""}),
        json!({"key": "tx_hash",        "value": ""}),
        json!({"key": "subscription_id","value": ""}),
    ]
}

// ---------------------------------------------------------------------------
// Request body
// ---------------------------------------------------------------------------

fn build_body(op: &Value, spec: &Value) -> Value {
    let body_schema = op.pointer("/requestBody/content/application~1json/schema");
    let Some(schema) = body_schema else { return Value::Null };

    let example = generate_example(schema, spec, 0);

    json!({
        "mode": "raw",
        "raw": serde_json::to_string_pretty(&example).unwrap_or_default(),
        "options": {
            "raw": {"language": "json"}
        }
    })
}

/// Recursively generate an example value from a JSON Schema node.
fn generate_example(schema: &Value, spec: &Value, depth: u8) -> Value {
    if depth > 4 { return json!({}); }

    // Resolve $ref
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        let pointer = r.trim_start_matches('#').replace('/', "/");
        if let Some(resolved) = spec.pointer(&pointer) {
            return generate_example(resolved, spec, depth + 1);
        }
    }

    // Use provided example if available
    if let Some(ex) = schema.get("example") {
        return ex.clone();
    }

    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            let mut obj = Map::new();
            let empty = Map::new();
            let props = schema.get("properties").and_then(Value::as_object).unwrap_or(&empty);
            for (key, val) in props {
                obj.insert(key.clone(), generate_example(val, spec, depth + 1));
            }
            Value::Object(obj)
        }
        Some("array") => {
            let item_schema = schema.get("items").unwrap_or(&Value::Null);
            json!([generate_example(item_schema, spec, depth + 1)])
        }
        Some("string")  => json!("string"),
        Some("integer") => json!(0),
        Some("number")  => json!(0.0),
        Some("boolean") => json!(false),
        _ => {
            // Try anyOf / oneOf
            if let Some(variants) = schema.get("anyOf").or_else(|| schema.get("oneOf"))
                .and_then(Value::as_array)
            {
                if let Some(first) = variants.first() {
                    return generate_example(first, spec, depth + 1);
                }
            }
            json!({})
        }
    }
}

// ---------------------------------------------------------------------------
// Example response stub
// ---------------------------------------------------------------------------

fn build_example_response(op: &Value) -> Value {
    let status = op
        .get("responses")
        .and_then(Value::as_object)
        .and_then(|r| r.keys().next().cloned())
        .unwrap_or_else(|| "200".into());

    let status_code: u16 = status.parse().unwrap_or(200);

    json!({
        "name": format!("{status_code} OK"),
        "originalRequest": {},
        "status": status_name(status_code),
        "code": status_code,
        "header": [{"key": "Content-Type", "value": "application/json"}],
        "body": ""
    })
}

fn status_name(code: u16) -> &'static str {
    match code {
        200 => "OK", 201 => "Created", 204 => "No Content",
        400 => "Bad Request", 401 => "Unauthorized", 403 => "Forbidden",
        404 => "Not Found", 409 => "Conflict", 422 => "Unprocessable Entity",
        429 => "Too Many Requests", 500 => "Internal Server Error",
        _ => "OK",
    }
}

// ---------------------------------------------------------------------------
// Environment files
// ---------------------------------------------------------------------------

fn build_environments() -> Vec<(&'static str, Value)> {
    vec![
        ("local",    build_env("Soroban Pulse — Local",    "http://localhost:3000")),
        ("testnet",  build_env("Soroban Pulse — Testnet",  "https://api.testnet.sorobanpulse.io")),
        ("mainnet",  build_env("Soroban Pulse — Mainnet",  "https://api.sorobanpulse.io")),
    ]
}

fn build_env(name: &str, base_url: &str) -> Value {
    json!({
        "_postman_variable_scope": "environment",
        "id": Uuid::new_v4().to_string(),
        "name": name,
        "values": [
            {"key": "base_url",        "value": base_url, "type": "default", "enabled": true},
            {"key": "api_key",         "value": "",       "type": "secret",  "enabled": true},
            {"key": "admin_api_key",   "value": "",       "type": "secret",  "enabled": true},
            {"key": "contract_id",     "value": "",       "type": "default", "enabled": true},
            {"key": "tx_hash",         "value": "",       "type": "default", "enabled": true},
            {"key": "subscription_id", "value": "",       "type": "default", "enabled": true}
        ]
    })
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

fn write_outputs(
    collection: &Value,
    environments: &[(&str, Value)],
    output_dir: &Path,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(output_dir.join("environments"))?;

    std::fs::write(
        output_dir.join("Soroban_Pulse.postman_collection.json"),
        serde_json::to_string_pretty(collection)?,
    )?;

    for (name, env) in environments {
        std::fs::write(
            output_dir.join("environments").join(format!("{name}.postman_environment.json")),
            serde_json::to_string_pretty(env)?,
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn count_requests(collection: &Value) -> usize {
    let mut count = 0;
    if let Some(folders) = collection.get("item").and_then(Value::as_array) {
        for folder in folders {
            if let Some(items) = folder.get("item").and_then(Value::as_array) {
                count += items.len();
            }
        }
    }
    count
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
}

fn print_usage() {
    eprintln!(
        "Usage: gen_postman [OPTIONS]

Reads OpenAPI 3.0 JSON from stdin (or --input) and writes a Postman Collection
v2.1 + environment files to --output-dir (default: postman/).

Options:
  --input <file>       OpenAPI JSON file (default: stdin)
  --output-dir <path>  Output directory (default: postman/)
  --dry-run            Print collection JSON to stdout, do not write files
  -h, --help           Show this message

Examples:
  cargo run --bin gen_openapi | cargo run --bin gen_postman
  cargo run --bin gen_postman -- --input openapi.json --output-dir postman/
  cargo run --bin gen_postman -- --dry-run < openapi.json
"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal_spec() -> Value {
        json!({
            "openapi": "3.0.3",
            "info": {"title": "Test API", "version": "1.0.0"},
            "paths": {
                "/health": {
                    "get": {
                        "tags": ["Health"],
                        "summary": "Health check",
                        "parameters": [],
                        "responses": {"200": {"description": "OK"}}
                    }
                },
                "/v1/events": {
                    "get": {
                        "tags": ["Events"],
                        "summary": "List events",
                        "parameters": [
                            {"name": "limit", "in": "query", "required": false,
                             "schema": {"type": "integer", "example": 25}}
                        ],
                        "security": [{"ApiKey": []}],
                        "responses": {"200": {"description": "OK"}}
                    }
                },
                "/admin/replay": {
                    "post": {
                        "tags": ["Admin"],
                        "summary": "Replay events",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "from_ledger": {"type": "integer"},
                                            "to_ledger":   {"type": "integer"}
                                        }
                                    }
                                }
                            }
                        },
                        "responses": {"200": {"description": "OK"}}
                    }
                }
            }
        })
    }

    #[test]
    fn collection_has_info_and_auth() {
        let coll = build_collection(&minimal_spec());
        assert_eq!(coll["info"]["name"], "Test API");
        assert_eq!(coll["auth"]["type"], "apikey");
    }

    #[test]
    fn collection_groups_by_tag() {
        let coll = build_collection(&minimal_spec());
        let items = coll["item"].as_array().unwrap();
        let names: Vec<&str> = items.iter()
            .map(|i| i["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"Health"));
        assert!(names.contains(&"Events"));
        assert!(names.contains(&"Admin"));
    }

    #[test]
    fn health_endpoint_gets_noauth() {
        let auth = build_auth("/health", &json!({}));
        assert_eq!(auth.unwrap()["type"], "noauth");
    }

    #[test]
    fn admin_endpoint_gets_admin_key() {
        let auth = build_auth("/admin/replay", &json!({}));
        let auth = auth.unwrap();
        assert_eq!(auth["type"], "apikey");
        let val = auth["apikey"].as_array().unwrap()
            .iter()
            .find(|v| v["key"] == "value")
            .unwrap();
        assert_eq!(val["value"], "{{admin_api_key}}");
    }

    #[test]
    fn query_params_added_to_url() {
        let op = json!({
            "parameters": [
                {"name": "limit", "in": "query", "required": false,
                 "schema": {"type": "integer", "example": 25}}
            ]
        });
        let (url, _, _) = build_url("/v1/events", &op);
        let query = url["query"].as_array().unwrap();
        assert_eq!(query[0]["key"], "limit");
        assert_eq!(query[0]["disabled"], true); // not required → disabled by default
    }

    #[test]
    fn path_params_extracted_from_template() {
        let op = json!({
            "parameters": [
                {"name": "contract_id", "in": "path", "required": true,
                 "schema": {"type": "string"}}
            ]
        });
        let (url, _, _) = build_url("/v1/events/contract/{contract_id}", &op);
        let vars = url["variable"].as_array().unwrap();
        assert_eq!(vars[0]["key"], "contract_id");
    }

    #[test]
    fn body_generated_from_schema() {
        let op = json!({
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "from_ledger": {"type": "integer"},
                                "to_ledger":   {"type": "integer"}
                            }
                        }
                    }
                }
            }
        });
        let body = build_body(&op, &json!({}));
        assert_eq!(body["mode"], "raw");
        let raw: Value = serde_json::from_str(body["raw"].as_str().unwrap()).unwrap();
        assert!(raw.get("from_ledger").is_some());
        assert!(raw.get("to_ledger").is_some());
    }

    #[test]
    fn environments_have_three_targets() {
        let envs = build_environments();
        assert_eq!(envs.len(), 3);
        let names: Vec<&str> = envs.iter().map(|(n, _)| *n).collect();
        assert!(names.contains(&"local"));
        assert!(names.contains(&"testnet"));
        assert!(names.contains(&"mainnet"));
    }

    #[test]
    fn environment_contains_required_variables() {
        let (_, env) = &build_environments()[0];
        let keys: Vec<&str> = env["values"].as_array().unwrap()
            .iter()
            .map(|v| v["key"].as_str().unwrap())
            .collect();
        assert!(keys.contains(&"base_url"));
        assert!(keys.contains(&"api_key"));
        assert!(keys.contains(&"admin_api_key"));
    }

    #[test]
    fn generate_example_handles_nested_objects() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "count": {"type": "integer"},
                "active": {"type": "boolean"}
            }
        });
        let ex = generate_example(&schema, &json!({}), 0);
        assert_eq!(ex["name"], "string");
        assert_eq!(ex["count"], 0);
        assert_eq!(ex["active"], false);
    }
}
