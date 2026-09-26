use super::*;
use runtime_payloads::Reference;

pub(super) type PolicyCache = [Option<(Instant, Policy)>; 2];
const COMPLETION_SCHEMA: &str = "durable-workflow.v2.payload-completion-context.v1";
const COMPLETION_HEADER: &str = "x-durable-workflow-payload-completion";

#[derive(Clone, Debug)]
pub(super) struct Policy {
    threshold: usize,
    max_bytes: usize,
    request_bytes: usize,
    timeout: Duration,
    available: bool,
    completion_context: bool,
}

fn unsupported(message: &str) -> Error {
    Error::Codec(format!("external_payload_unsupported: {message}"))
}

impl Policy {
    fn from_info(info: &Value) -> Result<Self> {
        let positive = |value: &Value| {
            value
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| *n > 0)
        };
        let request_bytes = if let Some(limit) = info.pointer("/limits/max_payload_bytes") {
            positive(limit).ok_or_else(|| unsupported("invalid ordinary request limit"))?
        } else {
            2 * 1024 * 1024
        };
        let storage = &info["namespace"]["external_payload_storage"];
        let manifest = &storage["transport"];
        if manifest.is_null() {
            return Ok(Self {
                threshold: request_bytes,
                max_bytes: request_bytes,
                request_bytes,
                timeout: Duration::from_secs(30),
                available: false,
                completion_context: false,
            });
        }
        if manifest["schema"] != "durable-workflow.v2.runtime-external-payload-transport.v1"
            || manifest["version"] != 1
            || manifest["reference_schema"] != runtime_payloads::SCHEMA
            || manifest["mode"] != "authenticated_namespace_runtime"
            || manifest["upload"]["method"] != "POST"
            || manifest["upload"]["path"] != "/api/external-payloads/v1"
            || manifest["fetch"]["method"] != "GET"
            || manifest["fetch"]["path_template"] != "/api/external-payloads/v1/{referenceId}"
        {
            return Err(unsupported("invalid runtime transport manifest"));
        }
        let threshold = positive(&storage["threshold_bytes"])
            .ok_or_else(|| unsupported("invalid inline threshold"))?;
        let max_bytes = positive(&manifest["limits"]["max_payload_bytes"])
            .filter(|max| *max >= threshold)
            .ok_or_else(|| unsupported("invalid upload limit"))?;
        let timeout = positive(&manifest["limits"]["request_timeout_seconds"])
            .ok_or_else(|| unsupported("invalid upload timeout"))?;
        Ok(Self {
            threshold,
            max_bytes,
            request_bytes,
            timeout: Duration::from_secs(timeout as u64),
            available: storage["status"] == "available",
            completion_context: manifest["upload"]["completion_context"]["schema"]
                == COMPLETION_SCHEMA
                && manifest["upload"]["completion_context"]["header"]
                    == "X-Durable-Workflow-Payload-Completion",
        })
    }
}

struct Upload {
    path: String,
    blob: String,
    sha256: String,
}

impl Client {
    pub(super) async fn externalize_runtime_payloads(
        &self,
        body: &mut Value,
        path: &str,
        protocol: RequestProtocol,
    ) -> Result<()> {
        let mut payloads = Vec::new();
        for path in payload_paths(body, path, protocol) {
            let Some(value) = body.pointer(&path) else {
                continue;
            };
            Reference::parse(value)?;
            let blob = value.as_str().or_else(|| {
                (value.as_object().is_some_and(|map| map.len() == 2)
                    && value["codec"] == DEFAULT_CODEC)
                    .then(|| value["blob"].as_str())
                    .flatten()
            });
            if let Some(blob) = blob {
                payloads.push((path, blob.len()));
            }
        }
        if payloads.is_empty() {
            return Ok(());
        }
        let policy = self.runtime_upload_policy(protocol).await?;
        // Plan every replacement before any upload. Aggregate JSON size matters,
        // even when each individual value is below the namespace threshold.
        payloads.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
        if payloads.iter().any(|(_, size)| *size > policy.max_bytes) {
            return Err(Error::Codec(
                "external_payload_oversized: encoded payload exceeds runtime upload limit".into(),
            ));
        }
        let mut uploads = Vec::new();
        for (path, size) in &payloads {
            if *size > policy.threshold {
                uploads.push(select(body, path)?);
            }
        }
        for (path, _) in &payloads {
            if serde_json::to_vec(body)?.len() <= policy.request_bytes {
                break;
            }
            if !uploads.iter().any(|upload| upload.path == *path) {
                uploads.push(select(body, path)?);
            }
        }
        if serde_json::to_vec(body)?.len() > policy.request_bytes {
            return Err(Error::Codec("payload_too_large: request metadata exceeds ordinary API limit with payloads externalized".into()));
        }
        if !uploads.is_empty() && !policy.available {
            return Err(Error::Codec(
                "external_payload_unavailable: namespace runtime storage is not available".into(),
            ));
        }
        let mut uploaded = HashMap::<(String, usize), Reference>::new();
        for upload in uploads {
            let identity = (upload.sha256.clone(), upload.blob.len());
            let reference = if let Some(reference) = uploaded.get(&identity) {
                reference.clone()
            } else {
                let completion = if policy.completion_context
                    && matches!(protocol, RequestProtocol::Worker(_))
                {
                    completion_context(body, path, &upload.path)
                } else {
                    None
                };
                let request = self
                    .runtime_payload_request(
                        reqwest::Method::POST,
                        "/external-payloads/v1",
                        protocol,
                        false,
                    )?
                    .timeout(policy.timeout)
                    .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                    .header("X-Durable-Workflow-Payload-Codec", DEFAULT_CODEC)
                    .header("X-Durable-Workflow-Payload-Size", upload.blob.len())
                    .header("X-Durable-Workflow-Payload-SHA256", &upload.sha256)
                    .body(upload.blob)
                    .build()?;
                let response = self
                    .runtime_payload_json(request, protocol, 64 * 1024, completion)
                    .await?;
                if response["schema"] != "durable-workflow.v2.runtime-external-payload-upload.v1"
                    || response["transport_version"] != 1
                {
                    return Err(unsupported("invalid upload response"));
                }
                let reference = Reference::parse(
                    &json!({"codec": DEFAULT_CODEC, "external_payload": response["reference"]}),
                )?
                .ok_or_else(|| unsupported("missing upload reference"))?;
                if reference.size_bytes != identity.1 || reference.sha256 != identity.0 {
                    return Err(Error::Codec("external_payload_integrity_mismatch: upload response differs from submitted bytes".into()));
                }
                uploaded.insert(identity, reference.clone());
                reference
            };
            replace(body, &upload.path, serde_json::to_value(reference)?)?;
        }
        Ok(())
    }

    async fn runtime_upload_policy(&self, protocol: RequestProtocol) -> Result<Policy> {
        let role = usize::from(matches!(protocol, RequestProtocol::Worker(_)));
        let cached = self
            .runtime_upload_policy
            .lock()
            .map_err(|_| unsupported("discovery cache poisoned"))?[role]
            .clone();
        if let Some((checked, policy)) = cached {
            if checked.elapsed() < Duration::from_secs(60) {
                return Ok(policy);
            }
        }
        // Discovery uses the control-plane header with this operation's credential
        // role, so a worker never needs the application's client token.
        let request = self
            .runtime_payload_request(reqwest::Method::GET, "/cluster/info", protocol, true)?
            .build()?;
        let info = self
            .runtime_payload_json(request, protocol, 2 * 1024 * 1024, None)
            .await?;
        let policy = Policy::from_info(&info)?;
        self.runtime_upload_policy
            .lock()
            .map_err(|_| unsupported("discovery cache poisoned"))?[role] =
            Some((Instant::now(), policy.clone()));
        Ok(policy)
    }

    fn runtime_payload_request(
        &self,
        method: reqwest::Method,
        path: &str,
        protocol: RequestProtocol,
        discovery: bool,
    ) -> Result<reqwest::RequestBuilder> {
        let mut request = self
            .http
            .request(method, format!("{}/api{path}", self.base_url))
            .header(reqwest::header::ACCEPT, "application/json")
            .header("X-Namespace", &self.namespace);
        request = match protocol {
            RequestProtocol::Worker(version) if !discovery => {
                request.header("X-Durable-Workflow-Protocol-Version", version)
            }
            _ => request.header(
                "X-Durable-Workflow-Control-Plane-Version",
                CONTROL_PLANE_VERSION,
            ),
        };
        if let Some(token) = self.auth_token(protocol)? {
            request = request.bearer_auth(token);
        }
        Ok(request)
    }

    async fn runtime_payload_json(
        &self,
        request: reqwest::Request,
        protocol: RequestProtocol,
        limit: usize,
        completion: Option<reqwest::header::HeaderValue>,
    ) -> Result<Value> {
        let mut retries = 0;
        let mut bound_retry = false;
        loop {
            let mut attempt = request
                .try_clone()
                .ok_or_else(|| unsupported("upload body cannot be retried"))?;
            if bound_retry {
                if let Some(context) = &completion {
                    attempt
                        .headers_mut()
                        .insert(COMPLETION_HEADER, context.clone());
                }
            }
            let mut response = self.http.execute(attempt).await?;
            let status = response.status();
            if response
                .content_length()
                .is_some_and(|size| size > limit as u64)
            {
                return Err(unsupported(
                    "runtime transport response exceeds its byte limit",
                ));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if chunk.len() > limit.saturating_sub(bytes.len()) {
                    return Err(unsupported(
                        "runtime transport response exceeds its byte limit",
                    ));
                }
                bytes.extend_from_slice(&chunk);
            }
            if !status.is_success() {
                let body = String::from_utf8_lossy(&bytes).into_owned();
                if let Some(failure) = protocol_failure(status, &body) {
                    return Err(Error::Protocol(failure));
                }
                let error = Error::Http { status, body };
                if !bound_retry
                    && completion.is_some()
                    && status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                    && worker_storage_admission_body(&error).is_some_and(|body| {
                        body["reason"] == "storage_pressure" && body["storage_state"] == "draining"
                    })
                {
                    bound_retry = true;
                    continue;
                }
                // Upload bytes are content-addressed; a late refusal may safely
                // retry them. Never relax admission for ordinary mutations.
                let retry_error = worker_storage_admission_body(&error).and_then(|mut body| {
                    if request.method() == reqwest::Method::POST
                        && request.url().path().ends_with("/api/external-payloads/v1")
                        && body.get("request_admitted").is_none()
                    {
                        body["request_admitted"] = json!(false);
                        Some(Error::Http {
                            status,
                            body: body.to_string(),
                        })
                    } else {
                        None
                    }
                });
                if self
                    .wait_for_storage_admission(
                        retry_error.as_ref().unwrap_or(&error),
                        protocol,
                        None,
                        None,
                        None,
                        &mut retries,
                    )
                    .await
                {
                    // Try ordinary admission again after capacity recovers.
                    bound_retry = false;
                    continue;
                }
                return Err(error);
            }
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|_| unsupported("runtime transport response is not JSON"))?;
            if !value.is_object() {
                return Err(unsupported("runtime transport response must be an object"));
            }
            return Ok(value);
        }
    }
}

fn completion_context(
    body: &Value,
    path: &str,
    slot: &str,
) -> Option<reqwest::header::HeaderValue> {
    let parts: Vec<_> = path
        .split('?')
        .next()?
        .trim_start_matches('/')
        .split('/')
        .collect();
    let ["worker", family, task_id, operation] = parts.as_slice() else {
        return None;
    };
    let (kind, field) = match *family {
        "activity-tasks" => ("activity", "activity_attempt_id"),
        "workflow-tasks" => ("workflow", "workflow_task_attempt"),
        "query-tasks" => ("query", "query_task_attempt"),
        _ => return None,
    };
    if !matches!(*operation, "complete" | "fail") || task_id.is_empty() {
        return None;
    }
    let owner = body["lease_owner"].as_str().filter(|s| !s.is_empty())?;
    let attempt = &body[field];
    if kind == "activity" {
        attempt.as_str().filter(|s| !s.is_empty())?;
    } else {
        attempt.as_u64().filter(|n| *n > 0)?;
    }
    // These pointers come only from payload_paths, never from application maps.
    let slot: Vec<Value> = slot
        .trim_start_matches('/')
        .split('/')
        .map(|part| {
            part.parse::<u64>()
                .map(Value::from)
                .unwrap_or_else(|_| json!(part))
        })
        .collect();
    let context = json!({"schema": COMPLETION_SCHEMA, "kind": kind, "task_id": task_id,
        "attempt": attempt, "lease_owner": owner, "operation": operation, "slot": slot})
    .to_string();
    if context.len() > 4096 {
        return None;
    }
    reqwest::header::HeaderValue::from_str(&context).ok()
}

fn select(body: &mut Value, path: &str) -> Result<Upload> {
    let value = body
        .pointer_mut(path)
        .ok_or_else(|| unsupported("missing payload slot"))?
        .take();
    let blob = match value {
        Value::String(blob) => blob,
        Value::Object(mut map) => match map.remove("blob") {
            Some(Value::String(blob)) => blob,
            _ => return Err(unsupported("missing encoded blob")),
        },
        _ => return Err(unsupported("invalid payload slot")),
    };
    let sha256 = format!("{:x}", Sha256::digest(blob.as_bytes()));
    replace(
        body,
        path,
        json!({"schema": runtime_payloads::SCHEMA, "codec": DEFAULT_CODEC,
        "reference_id": "ep_00000000000000000000000000", "size_bytes": blob.len(), "sha256": sha256}),
    )?;
    Ok(Upload {
        path: path.to_owned(),
        blob,
        sha256,
    })
}

fn replace(body: &mut Value, path: &str, reference: Value) -> Result<()> {
    let (parent, field) = path
        .rsplit_once('/')
        .ok_or_else(|| unsupported("invalid payload path"))?;
    let object = body
        .pointer_mut(parent)
        .and_then(Value::as_object_mut)
        .ok_or_else(|| unsupported("missing payload container"))?;
    if field == "payload" {
        object.remove("payload");
        object.insert("payload_reference".into(), reference);
        object.insert("payload_codec".into(), json!(DEFAULT_CODEC));
    } else {
        object.insert(
            field.to_owned(),
            json!({"codec": DEFAULT_CODEC, "external_payload": reference}),
        );
        if field == "result_envelope" {
            object.insert("result".into(), Value::Null);
        }
    }
    Ok(())
}

// Only protocol-owned slots are eligible; arbitrary application maps are not envelopes.
fn payload_paths(body: &Value, path: &str, protocol: RequestProtocol) -> Vec<String> {
    let path = path.split('?').next().unwrap_or(path);
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let mut paths = Vec::new();
    if matches!(protocol, RequestProtocol::Worker(_)) {
        match parts.as_slice() {
            ["worker", "workflow-tasks", _, "complete"] => {
                for (index, command) in body["commands"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    let command_type = command["type"].as_str().unwrap_or_default();
                    if let Some(field) = workflow_command_payload_field(command_type) {
                        paths.push(format!("/commands/{index}/{field}"));
                    }
                    if command_type == "record_local_activity" {
                        paths.extend([
                            format!("/commands/{index}/arguments"),
                            format!("/commands/{index}/result"),
                        ]);
                    }
                    if command_type == "fail_workflow" {
                        paths.push(format!("/commands/{index}/exception/details"));
                    }
                    for item in 0..command["workflow_stream"]["items"]
                        .as_array()
                        .map_or(0, Vec::len)
                    {
                        paths.push(format!(
                            "/commands/{index}/workflow_stream/items/{item}/payload"
                        ));
                    }
                }
            }
            ["worker", "activity-tasks", _, "complete"] => paths.push("/result".into()),
            ["worker", "activity-tasks", _, "fail"] => paths.push("/failure/details".into()),
            ["worker", "query-tasks", _, "complete"] => paths.push("/result_envelope".into()),
            _ => {}
        }
    } else {
        match parts.as_slice() {
            ["workflows" | "activities"]
            | ["workflows", _, "signal" | "query" | "update", _]
            | ["workflows", _, "runs", _, "signal" | "query" | "update", _]
            | ["workflows", _, "message-streams", _, "messages"] => paths.push("/input".into()),
            ["schedules"] | ["schedules", _] => paths.push("/action/input".into()),
            ["service-endpoints", _, "services", _, "operations", _, "execute"] => {
                paths.push("/arguments".into())
            }
            ["workflows", _, "runs", _, "streams", _, "items"] => {
                for item in 0..body["items"].as_array().map_or(0, Vec::len) {
                    paths.push(format!("/items/{item}/payload"));
                }
            }
            _ => {}
        }
    }
    paths
}
