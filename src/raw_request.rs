//! Untyped Server requests over the client's configured transport.
//!
//! Typed `Client` methods remain the API for applications. These requests are
//! for tools such as the CLI that must reach every Server route and report the
//! Server's response unchanged. A raw request makes exactly one HTTP attempt
//! and never rewrites its body: no failure-report reduction, no external
//! payload handling, no activity-lease checks and no storage-admission retry.

use std::time::Duration;

use serde_json::Value;

use crate::{
    metrics::ClientObservation, percent_encode_path_segment, protocol_failure, Client, Error,
    ProtocolFailure, RequestProtocol, Result, CONTROL_PLANE_VERSION,
};

const PROTOCOL_VERSION_HEADER: &str = "X-Durable-Workflow-Protocol-Version";
const CONTROL_PLANE_VERSION_HEADER: &str = "X-Durable-Workflow-Control-Plane-Version";

/// HTTP method of a [`RawRequest`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl RawMethod {
    fn as_reqwest(self) -> reqwest::Method {
        match self {
            Self::Get => reqwest::Method::GET,
            Self::Post => reqwest::Method::POST,
            Self::Put => reqwest::Method::PUT,
            Self::Patch => reqwest::Method::PATCH,
            Self::Delete => reqwest::Method::DELETE,
        }
    }
}

/// A Server API request addressed by path segments under `{base_url}/api`.
///
/// Each segment is percent-encoded like PHP `rawurlencode`: RFC 3986
/// unreserved characters stay, everything else (including `/`) is escaped, so
/// an identifier stays inside its own segment. Empty, `.` and `..` segments
/// are refused before any request is sent.
#[derive(Clone, Debug)]
pub struct RawRequest {
    method: RawMethod,
    segments: Vec<String>,
    query: Vec<(String, String)>,
    body: Option<Value>,
    timeout: Option<Duration>,
    protocol_version: Option<String>,
    control_plane_version: Option<String>,
}

impl RawRequest {
    pub fn new<I, S>(method: RawMethod, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            method,
            segments: segments.into_iter().map(Into::into).collect(),
            query: Vec::new(),
            body: None,
            timeout: None,
            protocol_version: None,
            control_plane_version: None,
        }
    }

    pub fn get<I: IntoIterator<Item = S>, S: Into<String>>(segments: I) -> Self {
        Self::new(RawMethod::Get, segments)
    }

    pub fn post<I: IntoIterator<Item = S>, S: Into<String>>(segments: I) -> Self {
        Self::new(RawMethod::Post, segments)
    }

    pub fn put<I: IntoIterator<Item = S>, S: Into<String>>(segments: I) -> Self {
        Self::new(RawMethod::Put, segments)
    }

    pub fn patch<I: IntoIterator<Item = S>, S: Into<String>>(segments: I) -> Self {
        Self::new(RawMethod::Patch, segments)
    }

    pub fn delete<I: IntoIterator<Item = S>, S: Into<String>>(segments: I) -> Self {
        Self::new(RawMethod::Delete, segments)
    }

    /// Append one query pair. Pairs keep their order.
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// Send this JSON value as the request body, unchanged.
    pub fn json(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    /// Override the client's whole-request timeout for this request.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Override the `X-Durable-Workflow-Protocol-Version` header value.
    /// Defaults to the client's worker protocol version.
    pub fn protocol_version(mut self, version: impl Into<String>) -> Self {
        self.protocol_version = Some(version.into());
        self
    }

    /// Override the `X-Durable-Workflow-Control-Plane-Version` header value.
    /// Defaults to [`CONTROL_PLANE_VERSION`].
    pub fn control_plane_version(mut self, version: impl Into<String>) -> Self {
        self.control_plane_version = Some(version.into());
        self
    }

    pub fn method(&self) -> RawMethod {
        self.method
    }

    /// The encoded path relative to `/api`, for example `/workflows/a%2Fb`.
    pub fn path(&self) -> Result<String> {
        if self.segments.is_empty() {
            return Err(invalid_request("at least one path segment is required"));
        }
        let mut path = String::new();
        for segment in &self.segments {
            if segment.is_empty() || segment == "." || segment == ".." {
                return Err(invalid_request(
                    "path segments must not be empty, \".\" or \"..\"",
                ));
            }
            path.push('/');
            path.push_str(&percent_encode_path_segment(segment));
        }
        Ok(path)
    }
}

fn invalid_request(reason: &str) -> Error {
    Error::InvalidRawRequest(reason.to_string())
}

/// A Server response, unchanged, for any HTTP status.
#[derive(Clone, Debug)]
pub struct RawResponse {
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
}

impl RawResponse {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// A response header value, when present and valid visible ASCII.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// Every response header as received. Names are lowercase.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
    }

    /// The worker protocol version the Server echoed, if any.
    pub fn protocol_version(&self) -> Option<&str> {
        self.header(PROTOCOL_VERSION_HEADER)
    }

    /// The control-plane version the Server echoed, if any.
    pub fn control_plane_version(&self) -> Option<&str> {
        self.header(CONTROL_PLANE_VERSION_HEADER)
    }

    /// The exact response body bytes.
    pub fn body_bytes(&self) -> &[u8] {
        &self.body
    }

    /// The body as JSON. An empty body is `null`.
    pub fn json(&self) -> Result<Value> {
        if self.body.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&self.body)?)
    }

    /// The decoded refusal body of a non-2xx response whose body is a JSON
    /// object. `None` for 2xx responses and other bodies.
    pub fn refusal(&self) -> Option<HttpRefusal> {
        if self.is_success() {
            return None;
        }
        HttpRefusal::decode(self.status, serde_json::from_slice(&self.body).ok()?)
    }

    /// Return the response for 2xx. Otherwise return the error a typed call
    /// would: [`Error::Protocol`] for a protocol-version refusal, else
    /// [`Error::Http`] with the exact body text.
    pub fn error_for_status(self) -> Result<Self> {
        if self.is_success() {
            return Ok(self);
        }
        let status = reqwest::StatusCode::from_u16(self.status)
            .map_err(|_| invalid_request("response status is not a valid HTTP status"))?;
        let body = String::from_utf8_lossy(&self.body).into_owned();
        if let Some(failure) = protocol_failure(status, &body) {
            return Err(Error::Protocol(failure));
        }
        Err(Error::Http { status, body })
    }
}

/// The decoded body of a refused (non-2xx) Server request.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct HttpRefusal {
    pub status: u16,
    pub reason: Option<String>,
    pub rejection_reason: Option<String>,
    pub message: Option<String>,
    pub error: Option<String>,
    pub validation_errors: Option<Value>,
    /// The full JSON body, unchanged.
    pub body: Value,
}

impl HttpRefusal {
    fn decode(status: u16, body: Value) -> Option<Self> {
        let object = body.as_object()?;
        let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_owned);
        Some(Self {
            status,
            reason: text("reason"),
            rejection_reason: text("rejection_reason"),
            message: text("message"),
            error: text("error"),
            validation_errors: object
                .get("validation_errors")
                .or_else(|| object.get("errors"))
                .cloned(),
            body,
        })
    }
}

impl Error {
    /// Decode a Server refusal from [`Error::Http`] or [`Error::Protocol`].
    ///
    /// Returns `None` for other errors and for HTTP errors whose body is not
    /// a JSON object.
    pub fn refusal(&self) -> Option<HttpRefusal> {
        match self {
            Self::Http { status, body } => {
                HttpRefusal::decode(status.as_u16(), serde_json::from_str(body).ok()?)
            }
            Self::Protocol(failure) => HttpRefusal::decode(failure.status, failure.body.clone()),
            _ => None,
        }
    }
}

/// Whether a Server worker protocol version is compatible with the client's:
/// same major, and a Server minor at least the client's. Versions that are
/// not `major.minor` integers must match exactly. This is the CLI 2.x rule.
pub fn worker_protocol_compatible(client: &str, server: &str) -> bool {
    fn split(version: &str) -> Option<(u64, u64)> {
        let (major, minor) = version.split_once('.')?;
        let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        if !digits(major) || !digits(minor) {
            return None;
        }
        Some((major.parse().ok()?, minor.parse().ok()?))
    }
    match (split(client), split(server)) {
        (Some(client), Some(server)) => client.0 == server.0 && client.1 <= server.1,
        _ => client == server,
    }
}

fn invalid_worker_response(status: u16, message: String, requested: &str, body: Value) -> Error {
    Error::Protocol(ProtocolFailure {
        status,
        reason: "invalid_worker_protocol_response".to_string(),
        message,
        supported_version: None,
        requested_version: Some(requested.to_string()),
        body,
    })
}

impl Client {
    /// Send a control-plane request with this client's base URL (including
    /// any path prefix), TLS settings, namespace, control token, timeout and
    /// metrics recorder.
    ///
    /// Every HTTP response is returned as a [`RawResponse`], including 4xx
    /// and 5xx. Use [`RawResponse::error_for_status`] or
    /// [`RawResponse::refusal`] to classify a refusal. Transport failures,
    /// including timeouts, are [`Error::Transport`].
    pub async fn control_plane_request(&self, request: RawRequest) -> Result<RawResponse> {
        self.raw_request(request, RequestProtocol::ControlPlane)
            .await
    }

    /// Send a worker-plane request with the worker token. Otherwise like
    /// [`Client::control_plane_request`].
    ///
    /// A 2xx response must echo a compatible protocol version (see
    /// [`worker_protocol_compatible`]) in its header, and its JSON body must
    /// carry the same `protocol_version`; otherwise the result is
    /// [`Error::Protocol`] with reason `invalid_worker_protocol_response`.
    /// A numeric body value is compared in its JSON spelling (`1.2` stays
    /// `"1.2"`, `1.20` becomes `"1.2"`). Servers send it as a string.
    /// Non-2xx responses are not checked.
    pub async fn worker_plane_request(&self, request: RawRequest) -> Result<RawResponse> {
        self.raw_request(request, RequestProtocol::DefaultWorker)
            .await
    }

    async fn raw_request(
        &self,
        request: RawRequest,
        protocol: RequestProtocol,
    ) -> Result<RawResponse> {
        let path = request.path()?;
        let method = request.method.as_reqwest();
        let mut observation = ClientObservation::new(self, &method, &path, protocol);
        let result = self
            .send_raw_request(request, method, &path, protocol, &observation)
            .await;
        observation.finish(&result);
        result
    }

    async fn send_raw_request(
        &self,
        request: RawRequest,
        method: reqwest::Method,
        path: &str,
        protocol: RequestProtocol,
        observation: &ClientObservation,
    ) -> Result<RawResponse> {
        // Both version headers go on every request, on both planes, as the
        // CLI 2.x client sends them.
        // The configured version, with no negotiation or worker-loop checks.
        let worker_version = request
            .protocol_version
            .clone()
            .unwrap_or_else(|| self.worker_protocol_version.as_str().to_string());
        let control_version = request
            .control_plane_version
            .as_deref()
            .unwrap_or(CONTROL_PLANE_VERSION);
        let auth_token = self.auth_token(protocol)?;
        let mut builder = self
            .http
            .request(method, format!("{}/api{}", self.base_url, path))
            .header(reqwest::header::ACCEPT, "application/json")
            .header("X-Namespace", &self.namespace)
            .header(PROTOCOL_VERSION_HEADER, &worker_version)
            .header(CONTROL_PLANE_VERSION_HEADER, control_version);
        if let Some(timeout) = request.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(token) = auth_token {
            builder = builder.bearer_auth(token);
        }
        if !request.query.is_empty() {
            builder = builder.query(&request.query);
        }
        if let Some(body) = &request.body {
            builder = builder.json(body);
        }

        // Timeouts stay Error::Transport, as for typed calls.
        let response = builder.send().await?;
        let status = response.status();
        observation.status(status);
        let response = RawResponse {
            status: status.as_u16(),
            headers: response.headers().clone(),
            body: response.bytes().await?.to_vec(),
        };

        if matches!(protocol, RequestProtocol::ControlPlane) || !response.is_success() {
            return Ok(response);
        }
        let body = response.json().unwrap_or(Value::Null);
        let echoed = response.protocol_version().map(str::trim);
        if !echoed.is_some_and(|echoed| worker_protocol_compatible(&worker_version, echoed)) {
            return Err(invalid_worker_response(
                response.status,
                format!(
                    "worker response version {} is not same-major compatible with {worker_version}",
                    echoed.unwrap_or("missing")
                ),
                &worker_version,
                body,
            ));
        }
        let body_version = body
            .get("protocol_version")
            .and_then(|version| match version {
                Value::String(text) => Some(text.trim().to_string()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            });
        if body_version.as_deref() != echoed {
            return Err(invalid_worker_response(
                response.status,
                "worker response body protocol_version must equal the protocol header".into(),
                &worker_version,
                body,
            ));
        }
        Ok(response)
    }
}
