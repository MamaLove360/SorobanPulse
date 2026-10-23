// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]
/// Dumps the generated OpenAPI spec to stdout as JSON.
/// Used by CI to validate and diff the committed docs/openapi.json.
fn main() -> Result<(), serde_json::Error> {
    use soroban_pulse::routes::ApiDoc;
    use utoipa::OpenApi;
    let spec = ApiDoc::openapi();
    println!("{}", serde_json::to_string_pretty(&spec)?);
    Ok(())
}
