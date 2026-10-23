//! Namespace-scoped one-page reads and lazy, bounded-memory enumeration.
//!
//! Iterators fetch only when `next().await` needs another page. They retain one
//! page, follow the Server's opaque token, and stop at a null/empty token. An
//! error is returned to the caller and then the iterator ends. Listing follows
//! the Server's ordering and consistency contract; it is not a snapshot across
//! concurrent changes. No new Server query fields or filter grammar are added.

use crate::{percent_encode_path_segment, Client, HistoryEvent, RequestProtocol, Result, Value};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};

/// Invalid page metadata that would otherwise truncate or misdirect a read.
#[derive(Clone, Debug, thiserror::Error)]
#[error("pagination failed ({reason}): {message}")]
#[non_exhaustive]
pub struct PaginationError {
    pub reason: String,
    pub message: String,
}

fn invalid(reason: &str, message: &str) -> crate::Error {
    PaginationError {
        reason: reason.into(),
        message: message.into(),
    }
    .into()
}

/// Filters accepted by the published `GET /workflows` API.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct WorkflowListOptions {
    pub workflow_type: Option<String>,
    pub status: Option<String>,
    pub query: Option<String>,
    pub page_size: Option<u32>,
    pub next_page_token: Option<String>,
}

impl WorkflowListOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn workflow_type(mut self, value: impl Into<String>) -> Self {
        self.workflow_type = Some(value.into());
        self
    }
    pub fn status(mut self, value: impl Into<String>) -> Self {
        self.status = Some(value.into());
        self
    }
    pub fn query(mut self, value: impl Into<String>) -> Self {
        self.query = Some(value.into());
        self
    }
    /// The Server validates its limit; values are never silently clamped.
    pub fn page_size(mut self, value: u32) -> Self {
        self.page_size = Some(value);
        self
    }
    pub fn next_page_token(mut self, value: impl Into<String>) -> Self {
        self.next_page_token = Some(value.into());
        self
    }
}

/// Options accepted by the published run-history API.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct WorkflowHistoryOptions {
    pub page_size: Option<u32>,
    pub next_page_token: Option<String>,
    pub wait_new_event: Option<bool>,
}

impl WorkflowHistoryOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn page_size(mut self, value: u32) -> Self {
        self.page_size = Some(value);
        self
    }
    pub fn next_page_token(mut self, value: impl Into<String>) -> Self {
        self.next_page_token = Some(value.into());
        self
    }
    /// Wait according to the Server's existing long-poll contract. Iteration
    /// still terminates when the Server returns no continuation token.
    pub fn wait_new_event(mut self, value: bool) -> Self {
        self.wait_new_event = Some(value);
        self
    }
}

/// Typed metadata for one listed run. Unknown response fields remain in `raw`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WorkflowSummary {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub status: String,
    pub status_bucket: Option<String>,
    pub business_key: Option<String>,
    pub task_queue: Option<String>,
    pub is_terminal: Option<bool>,
    pub compatibility: Option<String>,
    pub started_at: Option<String>,
    pub closed_at: Option<String>,
    #[serde(default, deserialize_with = "attribute_map")]
    pub search_attributes: BTreeMap<String, Value>,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

// PHP can encode an empty attribute map as []; treat only that empty array
// (or a legacy null) as empty, retaining real values without type conversion.
fn attribute_map<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, Value>, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::Object(map) => Ok(map.into_iter().collect()),
        Value::Null => Ok(BTreeMap::new()),
        Value::Array(values) if values.is_empty() => Ok(BTreeMap::new()),
        _ => Err(serde::de::Error::custom("expected a search-attribute map")),
    }
}

/// Exactly one list response. `next_page_token` indicates more results.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WorkflowListPage {
    pub workflows: Vec<WorkflowSummary>,
    /// Server-reported number of rows in this page.
    pub workflow_count: Option<u64>,
    pub next_page_token: Option<String>,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

/// Exactly one run-history response, with the existing lossless event model.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WorkflowHistoryPage {
    pub workflow_id: String,
    pub run_id: String,
    pub events: Vec<HistoryEvent>,
    pub next_page_token: Option<String>,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

impl Client {
    /// Read one page, preserving the Server's filters, validation and ordering.
    /// Published Server2.11 defaults to50 rows and accepts1..=200 per page.
    pub async fn list_workflows(&self, options: WorkflowListOptions) -> Result<WorkflowListPage> {
        let path = query_path(
            "/workflows",
            [
                ("workflow_type", options.workflow_type),
                ("status", options.status),
                ("query", options.query),
                ("page_size", options.page_size.map(|n| n.to_string())),
                ("next_page_token", options.next_page_token),
            ],
        );
        read_page(self, &path).await
    }

    /// Lazily enumerate list results, retaining the original namespace/filters.
    pub fn iter_workflows(&self, options: WorkflowListOptions) -> WorkflowIterator {
        let cursor = PageCursor::new(options.next_page_token.clone());
        WorkflowIterator {
            client: self.clone(),
            options,
            cursor,
        }
    }

    /// Read one page of a selected run's history. This is not a complete export.
    /// Published Server2.11 defaults to100 events and accepts1..=1000 per page.
    /// Use `export_workflow_history` for the self-contained replay bundle.
    pub async fn get_workflow_history(
        &self,
        workflow_id: &str,
        run_id: &str,
        options: WorkflowHistoryOptions,
    ) -> Result<WorkflowHistoryPage> {
        let path = query_path(
            &format!(
                "/workflows/{}/runs/{}/history",
                percent_encode_path_segment(workflow_id),
                percent_encode_path_segment(run_id)
            ),
            [
                ("page_size", options.page_size.map(|n| n.to_string())),
                ("next_page_token", options.next_page_token),
                (
                    "wait_new_event",
                    options
                        .wait_new_event
                        .map(|wait| if wait { "1".into() } else { "0".into() }),
                ),
            ],
        );
        let page: WorkflowHistoryPage = read_page(self, &path).await?;
        if page.workflow_id != workflow_id || page.run_id != run_id {
            return Err(invalid(
                "response_identity_mismatch",
                "history response does not match the requested workflow and run",
            ));
        }
        Ok(page)
    }

    /// Lazily enumerate one selected run. It does not follow continue-as-new
    /// into another run or subscribe indefinitely after history is exhausted.
    pub fn iter_workflow_history(
        &self,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        options: WorkflowHistoryOptions,
    ) -> WorkflowHistoryIterator {
        let cursor = PageCursor::new(options.next_page_token.clone());
        WorkflowHistoryIterator {
            client: self.clone(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            options,
            cursor,
        }
    }
}

// Only the query is serialized with this static URL; caller paths never pass
// through a second path parser or get percent-encoded twice here.
fn query_path<const N: usize>(path: &str, fields: [(&str, Option<String>); N]) -> String {
    let mut url =
        reqwest::Url::parse("http://sdk.invalid/").expect("static query serializer origin");
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in fields {
            if let Some(value) = value {
                pairs.append_pair(key, &value);
            }
        }
    }
    match url.query().filter(|query| !query.is_empty()) {
        Some(query) => format!("{path}?{query}"),
        None => path.into(),
    }
}

async fn read_page<T: DeserializeOwned>(client: &Client, path: &str) -> Result<T> {
    let response: Value = client
        .request_json(
            reqwest::Method::GET,
            path,
            RequestProtocol::ControlPlane,
            Option::<&Value>::None,
        )
        .await?;
    if response.get("next_page_token").is_none() {
        return Err(invalid(
            "missing_continuation",
            "page response omitted next_page_token",
        ));
    }
    Ok(serde_json::from_value(response)?)
}

/// Lazy workflow-list enumeration. An error is propagated once, then it ends.
pub struct WorkflowIterator {
    client: Client,
    options: WorkflowListOptions,
    cursor: PageCursor<WorkflowSummary>,
}

impl WorkflowIterator {
    /// Adapt to the standard async Stream API without eager fetching.
    pub fn into_stream(self) -> impl futures_util::Stream<Item = Result<WorkflowSummary>> + Send {
        futures_util::stream::try_unfold(self, |mut iterator| async move {
            Ok(iterator.next().await?.map(|item| (item, iterator)))
        })
    }

    pub async fn next(&mut self) -> Result<Option<WorkflowSummary>> {
        loop {
            if let Some(item) = self.cursor.items.pop_front() {
                return Ok(Some(item));
            }
            if self.cursor.done {
                return Ok(None);
            }
            self.cursor.items = VecDeque::new();
            let mut options = self.options.clone();
            options.next_page_token = self.cursor.token.clone();
            match self.client.list_workflows(options).await {
                Ok(page) => self.cursor.accept(page.workflows, page.next_page_token)?,
                Err(error) => {
                    self.cursor.done = true;
                    return Err(error);
                }
            }
        }
    }
}

/// Lazy run-history enumeration. An error is propagated once, then it ends.
pub struct WorkflowHistoryIterator {
    client: Client,
    workflow_id: String,
    run_id: String,
    options: WorkflowHistoryOptions,
    cursor: PageCursor<HistoryEvent>,
}

impl WorkflowHistoryIterator {
    /// Adapt to the standard async Stream API without eager fetching.
    pub fn into_stream(self) -> impl futures_util::Stream<Item = Result<HistoryEvent>> + Send {
        futures_util::stream::try_unfold(self, |mut iterator| async move {
            Ok(iterator.next().await?.map(|item| (item, iterator)))
        })
    }

    pub async fn next(&mut self) -> Result<Option<HistoryEvent>> {
        loop {
            if let Some(item) = self.cursor.items.pop_front() {
                return Ok(Some(item));
            }
            if self.cursor.done {
                return Ok(None);
            }
            self.cursor.items = VecDeque::new();
            let mut options = self.options.clone();
            options.next_page_token = self.cursor.token.clone();
            match self
                .client
                .get_workflow_history(&self.workflow_id, &self.run_id, options)
                .await
            {
                Ok(page) => self.cursor.accept(page.events, page.next_page_token)?,
                Err(error) => {
                    self.cursor.done = true;
                    return Err(error);
                }
            }
        }
    }
}

struct PageCursor<T> {
    items: VecDeque<T>,
    token: Option<String>,
    done: bool,
    checkpoint: Option<String>,
    distance: u64,
    power: u64,
}

impl<T> PageCursor<T> {
    fn new(token: Option<String>) -> Self {
        Self {
            items: VecDeque::new(),
            checkpoint: token.clone(),
            token,
            done: false,
            distance: 0,
            power: 1,
        }
    }

    fn accept(&mut self, items: Vec<T>, next: Option<String>) -> Result<()> {
        let next = next.filter(|value| !value.is_empty());
        if let Some(token) = &next {
            // Brent's cycle detection retains two tokens rather than an
            // ever-growing set of every page in a long audit/export.
            if Some(token) == self.token.as_ref() || Some(token) == self.checkpoint.as_ref() {
                self.done = true;
                return Err(invalid(
                    "repeated_page_token",
                    "Server repeated a continuation token or formed a token cycle",
                ));
            }
            self.distance = self.distance.saturating_add(1);
            if self.distance >= self.power {
                self.checkpoint = Some(token.clone());
                self.power = self.power.saturating_mul(2);
                self.distance = 0;
            }
        }
        self.done = next.is_none();
        self.token = next;
        self.items = items.into();
        Ok(())
    }
}
