use std::process::Command;

fn main() {
    if cfg!(all(feature = "otel", feature = "zipkin")) {
        panic!("Features 'otel' and 'zipkin' cannot be enabled together");
    }

    emit_build_metadata();
}

/// Issue #1259: expose compiled Cargo features and the build SHA to the crate
/// so `GET /v1/meta` can report them without reading deployment config.
fn emit_build_metadata() {
    // Cargo sets CARGO_FEATURE_<NAME> for every enabled feature, with the
    // name upper-cased and `-` replaced by `_`.
    let mut features: Vec<String> = std::env::vars()
        .filter_map(|(k, _)| k.strip_prefix("CARGO_FEATURE_").map(str::to_owned))
        .map(|f| f.to_lowercase().replace('_', "-"))
        .collect();
    features.sort();
    println!("cargo:rustc-env=SOROBAN_PULSE_FEATURES={}", features.join(","));

    // Prefer an explicit BUILD_SHA (set by CI / Docker builds without .git),
    // then fall back to the local git checkout.
    println!("cargo:rerun-if-env-changed=BUILD_SHA");
    let sha = std::env::var("BUILD_SHA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short=12", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=SOROBAN_PULSE_BUILD_SHA={sha}");

    if std::path::Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
        println!("cargo:rerun-if-changed=.git/refs/heads");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
