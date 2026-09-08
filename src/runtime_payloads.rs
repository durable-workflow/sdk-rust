use super::*;

const SCHEMA: &str = "durable-workflow.v2.runtime-external-payload-reference.v1";

#[derive(Deserialize, Eq, PartialEq, Hash)]
#[serde(deny_unknown_fields)]
struct Reference {
    schema: String,
    codec: String,
    reference_id: String,
    size_bytes: usize,
    sha256: String,
}

fn invalid_reference() -> Error {
    Error::Codec("external_payload_unsupported: invalid runtime payload reference".to_owned())
}

fn oversized() -> Error {
    Error::Codec(
        "external_payload_oversized: runtime payload exceeds the response byte limit".to_owned(),
    )
}

fn integrity_mismatch() -> Error {
    Error::Codec(
        "external_payload_integrity_mismatch: runtime payload differs from its reference"
            .to_owned(),
    )
}

impl Reference {
    fn parse(envelope: &Value) -> Result<Option<Self>> {
        let Some(raw) = envelope.get("external_payload") else {
            return Ok(None);
        };
        if envelope.as_object().map(|value| value.len()) != Some(2) || envelope["codec"] != "avro" {
            return Err(invalid_reference());
        }
        let reference: Self =
            serde_json::from_value(raw.clone()).map_err(|_| invalid_reference())?;
        let id = reference
            .reference_id
            .strip_prefix("ep_")
            .ok_or_else(invalid_reference)?;
        if reference.schema != SCHEMA
            || reference.codec != DEFAULT_CODEC
            || id.len() != 26
            || !id
                .bytes()
                .all(|c| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&c))
            || reference.sha256.len() != 64
            || !reference
                .sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(invalid_reference());
        }
        Ok(Some(reference))
    }
}

impl Client {
    pub(super) async fn resolve_runtime_payloads(
        &self,
        response: &mut Value,
        path: &str,
        protocol: RequestProtocol,
    ) -> Result<()> {
        let mut fetched = HashMap::<Reference, String>::new();
        let mut remaining = self.max_external_payload_bytes;
        for path in envelope_paths(path, protocol) {
            let mut envelopes = Vec::new();
            at(response, &path, &mut envelopes);
            for envelope in envelopes {
                let Some(reference) = Reference::parse(envelope)? else {
                    continue;
                };
                let blob = if let Some(blob) = fetched.get(&reference) {
                    blob.clone()
                } else {
                    if reference.size_bytes > remaining {
                        return Err(oversized());
                    }
                    let blob = self.fetch_runtime_payload(&reference, protocol).await?;
                    remaining -= blob.len();
                    fetched.insert(reference, blob.clone());
                    blob
                };
                // Resolve once per response, not in deterministic workflow code or a global cache.
                *envelope = json!({"codec": DEFAULT_CODEC, "blob": blob});
            }
        }
        Ok(())
    }

    async fn fetch_runtime_payload(
        &self,
        reference: &Reference,
        protocol: RequestProtocol,
    ) -> Result<String> {
        // Only opaque IDs enter the URL; provider URIs and redirects are never followed.
        let mut request = self
            .http
            .get(format!(
                "{}/api/external-payloads/v1/{}",
                self.base_url, reference.reference_id
            ))
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .header("X-Namespace", &self.namespace)
            .header("X-Durable-Workflow-Payload-Codec", DEFAULT_CODEC)
            .header("X-Durable-Workflow-Payload-Size", reference.size_bytes)
            .header("X-Durable-Workflow-Payload-SHA256", &reference.sha256);
        match protocol {
            RequestProtocol::Worker(version) => {
                request = request.header("X-Durable-Workflow-Protocol-Version", version)
            }
            RequestProtocol::ControlPlane => {
                request = request.header(
                    "X-Durable-Workflow-Control-Plane-Version",
                    CONTROL_PLANE_VERSION,
                )
            }
        }
        if let Some(token) = self.auth_token(protocol)? {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await?;
        let status = response.status();
        let limit = if status.is_success() {
            reference.size_bytes
        } else {
            64 * 1024
        };
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(oversized());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err(oversized());
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(Error::Http {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        if bytes.len() != reference.size_bytes
            || format!("{:x}", Sha256::digest(&bytes)) != reference.sha256
        {
            return Err(integrity_mismatch());
        }
        String::from_utf8(bytes).map_err(|_| integrity_mismatch())
    }
}

// These paths mirror Server's runtime payload transport. Do not recurse through
// arbitrary application maps: a business field named external_payload is not a reference.
fn envelope_paths(path: &str, protocol: RequestProtocol) -> Vec<Vec<&'static str>> {
    let path = path.split('?').next().unwrap_or(path);
    let mut paths = Vec::new();
    if matches!(protocol, RequestProtocol::Worker(_)) {
        for field in [
            "arguments",
            "workflow_arguments",
            "query_arguments",
            "signal_arguments",
            "update_arguments",
        ] {
            paths.push(vec!["task", field]);
        }
        history_paths(&["task", "history_events"], &mut paths);
        history_paths(&["history_events"], &mut paths);
        export_paths(&["task", "history_export"], &mut paths);
    } else {
        for field in ["input_envelope", "output_envelope", "result_envelope"] {
            paths.push(vec![field]);
        }
        if path
            .strip_prefix("/activities/")
            .is_some_and(|id| !id.is_empty() && !id.contains('/'))
        {
            paths.push(vec!["result"]);
        }
        if path.starts_with("/schedules") {
            paths.push(vec!["action", "input"]);
            paths.push(vec!["schedules", "*", "action", "input"]);
        }
        if path.ends_with("/history") {
            history_paths(&["events"], &mut paths);
        }
        if path.ends_with("/export") {
            export_paths(&[], &mut paths);
        }
    }
    paths
}

fn history_paths(prefix: &[&'static str], paths: &mut Vec<Vec<&'static str>>) {
    for field in [
        &["arguments"][..],
        &["result"],
        &["output"],
        &["activity", "arguments"],
        &["activity", "result"],
        &["command", "payload"],
        &["exception", "details"],
    ] {
        paths.push([prefix, &["*", "payload"], field].concat());
    }
}

fn export_paths(prefix: &[&'static str], paths: &mut Vec<Vec<&'static str>>) {
    history_paths(&[prefix, &["history_events"]].concat(), paths);
    for field in [
        &["activities", "*", "arguments"][..],
        &["activities", "*", "result"],
        &["commands", "*", "payload"],
        &["payloads", "arguments", "data"],
        &["payloads", "output", "data"],
        &["signals", "*", "arguments"],
        &["timeline", "*", "command", "payload"],
        &["updates", "*", "arguments"],
        &["updates", "*", "result"],
    ] {
        paths.push([prefix, field].concat());
    }
}

fn at<'a>(value: &'a mut Value, path: &[&str], output: &mut Vec<&'a mut Value>) {
    let Some((key, rest)) = path.split_first() else {
        output.push(value);
        return;
    };
    if *key == "*" {
        if let Some(items) = value.as_array_mut() {
            for item in items {
                at(item, rest, output);
            }
        }
    } else if let Some(value) = value.get_mut(*key) {
        at(value, rest, output);
    }
}
