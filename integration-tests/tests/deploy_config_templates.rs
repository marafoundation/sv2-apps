// The `config/*.toml.template` files are what the ECR images actually run: each
// Dockerfile's entrypoint pipes one through `envsubst` and hands the result to the
// binary. Nothing else in the tree type-checks them, and the two failure modes are
// both invisible until a container starts:
//
//   1. A `${VAR}` the template uses but `docker-compose.yml` never sets. envsubst substitutes the
//      empty string, so `verify_payout = ${TPROXY_VERIFY_PAYOUT}` renders as `verify_payout = ` — a
//      TOML syntax error.
//   2. A value whose rendered TOML type does not match the config struct, e.g. `pool_port =
//      "${JDC_POOL_PORT}"` quoting a u16.
//
// So: render each template with the compose environment for its service and
// deserialize it into the real config struct the binary uses.

use std::{collections::BTreeMap, fs, path::PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("integration-tests has a parent")
        .to_path_buf()
}

/// `environment:` entries per compose service.
///
/// ponytail: line scan, not a YAML parser — the file is ours and flat. If
/// docker-compose.yml ever grows anchors or nested env forms, take a yaml dep.
fn compose_environments() -> BTreeMap<String, BTreeMap<String, String>> {
    let text = fs::read_to_string(repo_root().join("docker-compose.yml")).expect("read compose");
    let mut services = BTreeMap::new();
    let mut service = None;
    let mut in_env = false;

    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // `  servicename:` — two-space indent under `services:`.
        if indent == 2 && trimmed.ends_with(':') {
            service = Some(trimmed.trim_end_matches(':').to_string());
            in_env = false;
        } else if indent == 4 && trimmed == "environment:" {
            in_env = true;
        } else if indent == 4 {
            in_env = false;
        } else if in_env && indent == 6 {
            if let Some((key, value)) = trimmed.trim_start_matches("- ").split_once('=') {
                services
                    .entry(service.clone().expect("env block inside a service"))
                    .or_insert_with(BTreeMap::new)
                    .insert(key.to_string(), value.to_string());
            }
        }
    }
    services
}

/// What `envsubst` does, except an unset variable is an error instead of "".
fn render(template: &str, env: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find('}').expect("unterminated ${...} in template");
        let name = &after[..end];
        let value = env.get(name).unwrap_or_else(|| {
            panic!(
                "template uses ${{{name}}} but docker-compose.yml never sets it; \
                 envsubst would render it as the empty string"
            )
        });
        out.push_str(value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

fn rendered(service: &str, template: &str) -> String {
    let envs = compose_environments();
    let env = envs
        .get(service)
        .unwrap_or_else(|| panic!("no `{service}` service in docker-compose.yml"));
    let text = fs::read_to_string(repo_root().join("config").join(template))
        .unwrap_or_else(|e| panic!("read config/{template}: {e}"));
    render(&text, env)
}

#[test]
fn pool_template_matches_the_pool_config_schema() {
    let toml = rendered("pool", "pool-jds-config.toml.template");
    toml::from_str::<pool_sv2::config::PoolConfig>(&toml).expect("pool template deserializes");
}

#[test]
fn jdc_template_matches_the_jdc_config_schema() {
    let toml = rendered("jdc", "jdc-config.toml.template");
    toml::from_str::<jd_client_sv2::config::JobDeclaratorClientConfig>(&toml)
        .expect("jdc template deserializes");
}

#[test]
fn tproxy_template_matches_the_translator_config_schema() {
    let toml = rendered("tproxy", "translator-proxy-config.toml.template");
    toml::from_str::<translator_sv2::config::TranslatorConfig>(&toml)
        .expect("tproxy template deserializes");
}

/// Mirrors the upstream `every_example_documents_the_cap_commented_out` test, which
/// only walks each crate's `config-examples/`. These templates are the ones we
/// deploy, so the knob has to be discoverable here too — commented out, so the
/// library default stays in force.
#[test]
fn templates_document_max_past_jobs_commented_out() {
    for template in [
        "pool-jds-config.toml.template",
        "jdc-config.toml.template",
        "translator-proxy-config.toml.template",
    ] {
        let path = repo_root().join("config").join(template);
        let text = fs::read_to_string(&path).expect("read template");
        assert!(
            text.contains("# max_past_jobs"),
            "{} does not document max_past_jobs (commented out)",
            path.display()
        );
    }
}
