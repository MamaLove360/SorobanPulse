// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]
use soroban_pulse::routes::ApiDoc;
use utoipa::OpenApi;

fn main() -> Result<(), serde_json::Error> {
    let openapi_json = ApiDoc::openapi().to_pretty_json()?;
    println!("{}", openapi_json);
    Ok(())
}
