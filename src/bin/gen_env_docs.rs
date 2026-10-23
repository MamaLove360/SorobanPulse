//! Environment-variable documentation generator for SorobanPulse.
//!
//! Parses the `@env`, `@default`, and `@description` doc-comment tags from
//! `src/config.rs` and emits a Markdown reference table to stdout or writes
//! it to `docs/configuration.md` when `--write` is passed.
//!
//! # Usage
//!
//! ```text
//! # Print the generated table to stdout
//! cargo run --bin gen_env_docs
//!
//! # Write directly to docs/configuration.md
//! cargo run --bin gen_env_docs -- --write
//!
//! # CI check: fail if docs/configuration.md is out of sync
//! cargo run --bin gen_env_docs -- --check
//! ```

use std::{
    fs,
    path::{Path, PathBuf},
    process,
};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// One row in the generated Markdown table.
#[derive(Debug, Default)]
struct EnvEntry {
    env: String,
    default: String,
    description: String,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Extract all `@env` / `@default` / `@description` triples from the source.
fn parse_entries(source: &str) -> Vec<EnvEntry> {
    let mut entries: Vec<EnvEntry> = Vec::new();
    let mut current: Option<EnvEntry> = None;

    for raw_line in source.lines() {
        let line = raw_line.trim();

        // Only look inside `///` doc comments.
        let Some(comment) = line.strip_prefix("///") else {
            // When we hit a non-comment line, flush any pending entry.
            if let Some(entry) = current.take() {
                if !entry.env.is_empty() {
                    entries.push(entry);
                }
            }
            continue;
        };

        let comment = comment.trim();

        if let Some(val) = comment.strip_prefix("@env") {
            // Start a new entry, flushing the previous one.
            if let Some(entry) = current.take() {
                if !entry.env.is_empty() {
                    entries.push(entry);
                }
            }
            current = Some(EnvEntry {
                env: val.trim().to_string(),
                ..Default::default()
            });
        } else if let Some(val) = comment.strip_prefix("@default") {
            if let Some(ref mut e) = current {
                e.default = val.trim().to_string();
            }
        } else if let Some(val) = comment.strip_prefix("@description") {
            if let Some(ref mut e) = current {
                e.description = val.trim().to_string();
            }
        }
        // Other doc-comment lines are ignored.
    }

    // Flush the final entry.
    if let Some(entry) = current.take() {
        if !entry.env.is_empty() {
            entries.push(entry);
        }
    }

    entries
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn render_markdown(entries: &[EnvEntry]) -> String {
    let mut out = String::new();

    out.push_str("# Environment Variable Reference\n\n");
    out.push_str(
        "This file is **auto-generated** from `src/config.rs`. \
         Do not edit it by hand — run `cargo run --bin gen_env_docs -- --write` instead.\n\n",
    );
    out.push_str(
        "| Variable | Default | Description |\n\
         |---|---|---|\n",
    );

    for entry in entries {
        // Escape pipe characters inside cells so they don't break the table.
        let env = entry.env.replace('|', "\\|");
        let default = entry.default.replace('|', "\\|");
        let description = entry.description.replace('|', "\\|");

        out.push_str(&format!(
            "| `{env}` | `{default}` | {description} |\n"
        ));
    }

    out.push('\n');
    out
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    // Walk upward from the binary's directory until we find Cargo.toml.
    let mut dir = std::env::current_exe()
        .unwrap_or_else(|_| PathBuf::from("."))
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    // In a typical `cargo run` the exe lives in target/debug/ — walk up.
    for _ in 0..6 {
        if dir.join("Cargo.toml").exists() {
            return dir;
        }
        match dir.parent() {
            Some(p) => dir = p.to_path_buf(),
            None => break,
        }
    }

    // Fallback: use the current working directory.
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let write_mode = args.iter().any(|a| a == "--write");
    let check_mode = args.iter().any(|a| a == "--check");

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!(
            "Usage: gen_env_docs [--write] [--check]\n\
             \n\
             Without flags: print Markdown to stdout.\n\
             --write : Write output to docs/configuration.md.\n\
             --check : Exit 1 if docs/configuration.md is out of sync (CI mode).\n"
        );
        process::exit(0);
    }

    let root = workspace_root();
    let config_path = root.join("src").join("config.rs");

    let source = fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("Error: cannot read {}: {e}", config_path.display());
        process::exit(1);
    });

    let entries = parse_entries(&source);

    if entries.is_empty() {
        eprintln!(
            "Warning: no @env tags found in {}. \
             Make sure the config fields have `/// @env ...` doc comments.",
            config_path.display()
        );
    }

    let markdown = render_markdown(&entries);
    let output_path = root.join("docs").join("configuration.md");

    if check_mode {
        // Read the existing file and compare.
        let existing = fs::read_to_string(&output_path).unwrap_or_default();
        if existing == markdown {
            eprintln!("docs/configuration.md is up to date.");
            process::exit(0);
        } else {
            eprintln!(
                "ERROR: docs/configuration.md is out of sync with src/config.rs.\n\
                 Run `cargo run --bin gen_env_docs -- --write` and commit the result."
            );
            process::exit(1);
        }
    } else if write_mode {
        fs::write(&output_path, &markdown).unwrap_or_else(|e| {
            eprintln!("Error: cannot write {}: {e}", output_path.display());
            process::exit(1);
        });
        eprintln!("Written {} entries to {}", entries.len(), output_path.display());
    } else {
        print!("{markdown}");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
    /// @env DATABASE_URL
    /// @default (required)
    /// @description PostgreSQL connection string.
    pub database_url: String,

    /// @env PORT
    /// @default 3000
    /// @description HTTP server listening port.
    pub port: u16,
"#;

    #[test]
    fn parses_two_entries() {
        let entries = parse_entries(SAMPLE);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].env, "DATABASE_URL");
        assert_eq!(entries[0].default, "(required)");
        assert_eq!(entries[0].description, "PostgreSQL connection string.");
        assert_eq!(entries[1].env, "PORT");
        assert_eq!(entries[1].default, "3000");
    }

    #[test]
    fn renders_markdown_table() {
        let entries = parse_entries(SAMPLE);
        let md = render_markdown(&entries);
        assert!(md.contains("| `DATABASE_URL` |"));
        assert!(md.contains("| `PORT` |"));
        assert!(md.contains("auto-generated"));
    }

    #[test]
    fn ignores_non_tagged_comments() {
        let src = r#"
    /// This is a plain comment with no tags.
    /// Another plain line.
    pub foo: u32,
"#;
        let entries = parse_entries(src);
        assert!(entries.is_empty());
    }

    #[test]
    fn handles_pipe_escaping() {
        let src = r#"
    /// @env PIPE_TEST
    /// @default a|b
    /// @description Has a | pipe in it.
    pub pipe: String,
"#;
        let entries = parse_entries(src);
        let md = render_markdown(&entries);
        // Pipe characters inside cells must be escaped.
        assert!(md.contains("a\\|b"));
        assert!(md.contains("Has a \\| pipe in it."));
    }
}
