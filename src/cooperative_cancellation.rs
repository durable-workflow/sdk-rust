use super::*;

const CONTROL_BUDGET: Duration = Duration::from_secs(5);

/// Optional reason and runtime-owned cleanup limit for a cooperative request.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CooperativeCancellationOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_timeout_seconds: Option<u64>,
}

/// The Server's original request identity and immutable cleanup deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationRequest {
    pub request_id: String,
    pub requested_at: String,
    pub cleanup_deadline_at: String,
    pub history_refresh_page_token: String,
}

/// Acknowledgment of a request targeting the current durable run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowCancellationRequest {
    pub workflow_id: String,
    pub run_id: String,
    pub duplicate: bool,
    pub cancellation_request: CancellationRequest,
}

fn invalid(detail: impl Into<String>) -> Error {
    Error::InvalidCooperativeCancellation(detail.into())
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| invalid(format!("{field} must be a non-empty string")))
}

impl CancellationRequest {
    pub(crate) fn from_observation(value: &Value) -> Result<Self> {
        let requested_at = text(value, "requested_at")?;
        let cleanup_deadline_at = text(value, "cleanup_deadline_at")?;
        let requested = DateTime::parse_from_rfc3339(requested_at)
            .map_err(|_| invalid("requested_at must be a timestamp with a timezone"))?;
        let deadline = DateTime::parse_from_rfc3339(cleanup_deadline_at)
            .map_err(|_| invalid("cleanup_deadline_at must be a timestamp with a timezone"))?;
        if deadline <= requested {
            return Err(invalid("cleanup deadline must follow the original request"));
        }
        Ok(Self {
            request_id: text(value, "request_id")?.to_owned(),
            requested_at: requested_at.to_owned(),
            cleanup_deadline_at: cleanup_deadline_at.to_owned(),
            history_refresh_page_token: text(value, "history_refresh_page_token")?.to_owned(),
        })
    }
}

fn supports_protocol(version: &str) -> bool {
    let Some((major, minor)) = version.split_once('.') else {
        return false;
    };
    major == "1"
        && !minor.is_empty()
        && minor.bytes().all(|byte| byte.is_ascii_digit())
        && minor.parse::<u64>().is_ok_and(|minor| minor >= 20)
}

fn require_discovery(info: &Value) -> Result<()> {
    let protocol = &info["worker_protocol"];
    if protocol["server_capabilities"]["cooperative_cancellation"].as_bool() != Some(true)
        || !protocol["version"].as_str().is_some_and(supports_protocol)
    {
        return Err(Error::CooperativeCancellationUnavailable(
            "runtime discovery must explicitly advertise support and compatible protocol 1.20"
                .to_string(),
        ));
    }
    Ok(())
}

fn acknowledgment(
    value: &Value,
    workflow_id: &str,
    selected_run: Option<&str>,
) -> Result<WorkflowCancellationRequest> {
    if value["accepted"].as_bool() != Some(true)
        || text(value, "workflow_id")? != workflow_id
        || selected_run.is_some_and(|run| value["run_id"].as_str() != Some(run))
    {
        return Err(invalid(
            "acknowledgment does not match the accepted workflow/run request",
        ));
    }
    Ok(WorkflowCancellationRequest {
        workflow_id: workflow_id.to_owned(),
        run_id: text(value, "run_id")?.to_owned(),
        duplicate: value["duplicate"]
            .as_bool()
            .ok_or_else(|| invalid("duplicate must be a boolean"))?,
        cancellation_request: CancellationRequest::from_observation(
            &value["cancellation_request"],
        )?,
    })
}

impl Client {
    /// Request bounded workflow-authored cleanup on a capable runtime.
    ///
    /// This is separate from terminal cancellation. Discovery must advertise
    /// cooperative support and compatible protocol 1.20. The Server also
    /// refuses an active workflow claim that cannot deliver cooperation.
    /// Repeated requests retain its original request identity and deadline.
    pub async fn request_workflow_cancellation(
        &self,
        workflow_id: &str,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        self.request_workflow_cancellation_target(workflow_id, None, options)
            .await
    }

    /// Request cleanup only if the selected run remains current.
    pub async fn request_workflow_run_cancellation(
        &self,
        workflow_id: &str,
        run_id: &str,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        self.request_workflow_cancellation_target(workflow_id, Some(run_id), options)
            .await
    }

    async fn request_workflow_cancellation_target(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        if workflow_id.trim().is_empty() || run_id.is_some_and(|id| id.trim().is_empty()) {
            return Err(invalid(
                "workflow and selected run identities must be non-empty",
            ));
        }
        if options
            .cleanup_timeout_seconds
            .is_some_and(|seconds| !(1..=3600).contains(&seconds))
            || options
                .reason
                .as_ref()
                .is_some_and(|reason| reason.chars().count() > 1000)
        {
            return Err(invalid(
                "cleanup timeout must be 1..3600 seconds and reason at most 1000 characters",
            ));
        }

        // Bound discovery, mutation and any storage retry by one total budget.
        tokio::time::timeout(CONTROL_BUDGET, async {
            let info: Value = self
                .request_json(
                    reqwest::Method::GET,
                    "/cluster/info",
                    RequestProtocol::ControlPlane,
                    Option::<&Value>::None,
                )
                .await?;
            require_discovery(&info)?;
            let mut path = format!("/workflows/{}", percent_encode_path_segment(workflow_id));
            if let Some(run_id) = run_id {
                path.push_str(&format!("/runs/{}", percent_encode_path_segment(run_id)));
            }
            path.push_str("/request-cancellation");
            let response: Value = self
                .request_json(
                    reqwest::Method::POST,
                    &path,
                    RequestProtocol::ControlPlane,
                    Some(&options),
                )
                .await?;
            acknowledgment(&response, workflow_id, run_id)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
}

impl WorkflowHandle {
    /// Request bounded cleanup for whichever run is current.
    pub async fn request_cancellation(
        &self,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        self.client
            .request_workflow_cancellation(&self.workflow_id, options)
            .await
    }

    /// Request bounded cleanup only while this handle's selected run is current.
    pub async fn request_selected_run_cancellation(
        &self,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        let run_id = self
            .run_id
            .as_deref()
            .ok_or_else(|| invalid("selected run_id is required"))?;
        self.client
            .request_workflow_run_cancellation(&self.workflow_id, run_id, options)
            .await
    }
}
