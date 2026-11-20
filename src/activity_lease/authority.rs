//! One activity's original identity and acknowledged runtime authority.

use super::MAX_REQUEST_WINDOW;
use crate::{
    percent_encode_path_segment, wait_for_worker_stop, Client, Error, Result, WorkerSession,
};
use chrono::{DateTime, FixedOffset, SecondsFormat};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio::time::Instant as Clock;

mod session;

type Timestamp = DateTime<FixedOffset>;

fn lost(message: &str) -> Error {
    Error::ActivityExecutionAbandoned(format!("activity lease: {message}"))
}

fn timestamp(value: &Value) -> Result<Timestamp> {
    let text = value
        .as_str()
        .ok_or_else(|| lost("missing original UTC6 timing"))?;
    let date =
        DateTime::parse_from_rfc3339(text).map_err(|_| lost("invalid original UTC6 timing"))?;
    if date.to_rfc3339_opts(SecondsFormat::Micros, true) != text {
        return Err(lost(
            "original timing must be UTC with six fractional digits",
        ));
    }
    Ok(date)
}

fn duration(later: Timestamp, earlier: Timestamp) -> Result<Duration> {
    let value = (later - earlier)
        .to_std()
        .map_err(|_| lost("expired timing"))?;
    if value.is_zero() {
        return Err(lost("expired timing"));
    }
    Ok(value)
}

fn until(start: Clock, later: Timestamp, earlier: Timestamp) -> Result<Clock> {
    start
        .checked_add(duration(later, earlier)?)
        .ok_or_else(|| lost("timing exceeds the local clock"))
}

fn interval(seconds: i64) -> Result<chrono::TimeDelta> {
    chrono::TimeDelta::try_seconds(seconds)
        .ok_or_else(|| lost("heartbeat interval is out of range"))
}

#[derive(Debug)]
struct State {
    captured: Clock,
    lease_until: Clock,
    business_until: Option<Clock>,
    business_deadline: Option<Timestamp>,
    heartbeat_until: Option<Clock>,
    heartbeat_interval: Option<i64>,
    heartbeat_time: Option<Timestamp>,
    heartbeat_deadline: Option<Timestamp>,
    server_time: Timestamp,
    lease_expiry: Timestamp,
    /// Length of the latest acknowledged lease grant; sizes the release reserve.
    grant: Duration,
    last_renewal: Option<(Timestamp, Timestamp, Clock)>,
    session: Option<session::Session>,
}

impl State {
    fn remaining(&self) -> Result<Duration> {
        let bound = self
            .business_until
            .into_iter()
            .chain(self.heartbeat_until)
            .chain(self.session.as_ref().map(session::Session::bound))
            .fold(self.lease_until, Clock::min);
        bound
            .checked_duration_since(Clock::now())
            .filter(|value| !value.is_zero())
            .ok_or_else(|| lost("original execution budget expired"))
    }

    /// Time kept back at the end of the lease to release an unrenewed claim:
    /// at most 10 s and at most a fifth of the latest grant.
    fn reserve(&self) -> Duration {
        RELEASE_RESERVE.min(self.grant / 5)
    }

    /// Lease time still usable for renewal attempts before the reserve.
    fn renewal_window(&self) -> Duration {
        self.lease_until
            .saturating_duration_since(Clock::now())
            .saturating_sub(self.reserve())
    }

    fn heartbeat_terms(
        task: &Value,
        deadlines: &Value,
        server: Timestamp,
    ) -> Result<(Option<Timestamp>, Option<i64>)> {
        let bound = deadlines
            .get("heartbeat")
            .filter(|value| !value.is_null())
            .map(timestamp)
            .transpose()?;
        let interval = task["retry_policy"]["heartbeat_timeout"]
            .as_i64()
            .filter(|value| *value > 0);
        if let Some(bound) = bound {
            let interval = interval.ok_or_else(|| lost("missing recorded heartbeat interval"))?;
            if bound - server > self::interval(interval)? {
                return Err(lost("heartbeat bound exceeds its recorded interval"));
            }
        }
        Ok((bound, interval))
    }

    /// `requested` is when the poll request was sent. The Server granted the
    /// lease at some point after that, so measuring from it never overstates
    /// the time left.
    fn delivery(
        task: &Value,
        server_time: &Value,
        worker: &str,
        namespace: &str,
        requested: Clock,
    ) -> Result<Self> {
        let captured = requested;
        let server_time = timestamp(server_time)?;
        let lease_expiry = timestamp(&task["lease_expires_at"])?;
        let grant = duration(lease_expiry, server_time)?;
        if grant > Duration::from_secs(86_400) {
            return Err(lost("invalid original lease duration"));
        }
        let empty = json!({});
        let deadlines = task.get("deadlines").unwrap_or(&empty);
        if !deadlines.is_object() {
            return Err(lost("invalid original deadline map"));
        }
        let mut business_deadline = None;
        for name in ["start_to_close", "schedule_to_close", "run", "execution"] {
            if let Some(value) = deadlines.get(name).filter(|value| !value.is_null()) {
                let bound = timestamp(value)?;
                business_deadline = Some(
                    business_deadline.map_or(bound, |original: Timestamp| original.min(bound)),
                );
            }
        }
        let (heartbeat_deadline, heartbeat_interval) =
            Self::heartbeat_terms(task, deadlines, server_time)?;
        let state = Self {
            captured,
            lease_until: until(captured, lease_expiry, server_time)?,
            business_until: business_deadline
                .map(|bound| until(captured, bound, server_time))
                .transpose()?,
            business_deadline,
            heartbeat_until: heartbeat_deadline
                .map(|bound| until(captured, bound, server_time))
                .transpose()?,
            heartbeat_time: heartbeat_deadline
                .zip(heartbeat_interval)
                .map(|(bound, seconds)| {
                    bound
                        .checked_sub_signed(interval(seconds)?)
                        .ok_or_else(|| lost("heartbeat timing underflow"))
                })
                .transpose()?,
            heartbeat_interval,
            heartbeat_deadline,
            server_time,
            lease_expiry,
            grant,
            last_renewal: None,
            session: session::Session::delivery(task, server_time, captured, worker, namespace)?,
        };
        state.remaining()?;
        Ok(state)
    }

    fn observe_progress(&mut self, reply: &Value, server: Timestamp, started: Clock) -> Result<()> {
        let Some(interval) = self.heartbeat_interval else {
            return Ok(());
        };
        let Ok(progress) = timestamp(&reply["last_heartbeat_at"]) else {
            return Ok(());
        };
        let Ok(deadline) = timestamp(&reply["heartbeat_deadline_at"]) else {
            return Ok(());
        };
        if progress > server
            || deadline <= server
            || deadline - progress != self::interval(interval)?
            || self
                .heartbeat_time
                .is_some_and(|previous| progress <= previous)
            || self
                .heartbeat_deadline
                .is_some_and(|previous| deadline <= previous)
        {
            return Ok(());
        }
        let bound = until(started, deadline, server)?;
        if bound > Clock::now() {
            self.heartbeat_until = Some(self.heartbeat_until.map_or(bound, |old| old.max(bound)));
            self.heartbeat_time = Some(progress);
            self.heartbeat_deadline = Some(deadline);
        }
        Ok(())
    }

    fn renewal(&mut self, reply: &Value, started: Clock) -> Result<()> {
        self.remaining()?;
        let server = timestamp(&reply["server_time"])?;
        let expiry = timestamp(&reply["lease_expires_at"])?;
        let grant = duration(expiry, server)?;
        let bound = until(started, expiry, server)?;
        if started < self.captured
            || started > Clock::now()
            || grant > Duration::from_secs(86_400)
            || bound <= Clock::now()
        {
            return Err(lost("invalid or consumed renewal grant"));
        }
        if self
            .heartbeat_time
            .is_some_and(|progress| server < progress)
            && expiry <= self.lease_expiry
        {
            return Ok(());
        }
        if server < self.server_time || expiry < self.lease_expiry {
            return Err(lost("renewal predates the acknowledged grant"));
        }
        if let Some(previous) = self.last_renewal {
            if server <= previous.0 {
                return if previous == (server, expiry, started) {
                    Ok(())
                } else {
                    Err(lost("replayed renewal"))
                };
            }
        }
        if let Some(deadline) = self.business_deadline {
            let observed = until(started, deadline, server)?;
            self.business_until = Some(
                self.business_until
                    .map_or(observed, |original| original.min(observed)),
            );
        }
        self.observe_progress(reply, server, started)?;
        if let Some(session) = &mut self.session {
            session.acknowledge(&reply["worker_session"], server, started)?;
        }
        self.lease_until = self.lease_until.max(bound);
        self.server_time = server;
        self.lease_expiry = expiry;
        self.grant = grant;
        self.last_renewal = Some((server, expiry, started));
        self.remaining().map(|_| ())
    }

    fn progress(&mut self, reply: &Value, started: Clock) -> Result<()> {
        self.remaining()?;
        if started < self.captured || started > Clock::now() {
            return Err(lost("invalid heartbeat clock"));
        }
        let progress = timestamp(&reply["last_heartbeat_at"])?;
        let expiry = timestamp(&reply["lease_expires_at"])?;
        let grant = duration(expiry, progress)?;
        if grant > Duration::from_secs(86_400) {
            return Err(lost("invalid heartbeat lease grant"));
        }
        if self.heartbeat_time.is_some_and(|old| progress <= old) {
            return Ok(());
        }
        let bound = until(started, expiry, progress)?;
        if bound <= Clock::now() {
            return Err(lost("heartbeat grant consumed in transport"));
        }
        if let Some(session) = &mut self.session {
            session.acknowledge(&reply["worker_session"], progress, started)?;
        }
        if let Some(interval) = self.heartbeat_interval {
            let deadline = progress
                .checked_add_signed(self::interval(interval)?)
                .ok_or_else(|| lost("heartbeat deadline overflow"))?;
            let heartbeat_until = until(started, deadline, progress)?;
            if heartbeat_until <= Clock::now() {
                return Err(lost("heartbeat progress consumed in transport"));
            }
            self.heartbeat_until = Some(
                self.heartbeat_until
                    .map_or(heartbeat_until, |old| old.max(heartbeat_until)),
            );
            self.heartbeat_deadline = Some(deadline);
        }
        self.heartbeat_time = Some(progress);
        if expiry > self.lease_expiry {
            self.grant = grant;
        }
        self.lease_expiry = self.lease_expiry.max(expiry);
        self.lease_until = self.lease_until.max(bound);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Authority {
    client: Client,
    task_id: String,
    attempt_id: String,
    owner: String,
    identity: Value,
    state: Mutex<State>,
    active: AtomicBool,
    changed: Notify,
    /// Latest transient renewal failure, named in a release report.
    last_failure: Mutex<Option<String>>,
    /// Set when the Worker gave the claim up because renewal did not recover.
    release: Mutex<Option<String>>,
    released: AtomicBool,
}

impl Authority {
    fn delivery(
        client: &Client,
        wire: &Value,
        worker: &str,
        queue: &str,
        requested: Clock,
    ) -> Result<Self> {
        let task = &wire["task"];
        for name in [
            "task_id",
            "workflow_id",
            "run_id",
            "activity_execution_id",
            "activity_attempt_id",
            "lease_owner",
            "task_queue",
        ] {
            if !task[name].as_str().is_some_and(|value| {
                !value.trim().is_empty() && value.trim() == value && value.chars().count() <= 255
            }) {
                return Err(lost("missing or malformed original activity identity"));
            }
        }
        if task["lease_owner"].as_str() != Some(worker)
            || task["task_queue"].as_str() != Some(queue)
            || task
                .get("namespace")
                .is_some_and(|value| value.as_str() != Some(client.namespace.as_str()))
            || task["attempt_number"]
                .as_u64()
                .is_none_or(|value| value == 0)
            || task
                .get("attempt_id")
                .is_some_and(|alias| !alias.is_null() && *alias != task["activity_attempt_id"])
        {
            return Err(lost(
                "activity claim changed owner, queue, namespace or attempt",
            ));
        }
        let mut base = client.clone();
        base.activity_lease_authority = None;
        Ok(Self {
            client: base,
            task_id: task["task_id"].as_str().unwrap().to_owned(),
            attempt_id: task["activity_attempt_id"].as_str().unwrap().to_owned(),
            owner: worker.to_owned(),
            identity: json!({"workflow_task_id":task["task_id"],"workflow_instance_id":task["workflow_id"],
                "workflow_run_id":task["run_id"],"activity_execution_id":task["activity_execution_id"],
                "activity_attempt_id":task["activity_attempt_id"],"attempt_number":task["attempt_number"],"lease_owner":worker}),
            state: Mutex::new(State::delivery(
                task,
                &wire["server_time"],
                worker,
                &client.namespace,
                requested,
            )?),
            active: AtomicBool::new(true),
            changed: Notify::new(),
            last_failure: Mutex::new(None),
            release: Mutex::new(None),
            released: AtomicBool::new(false),
        })
    }

    fn invalidate(&self, message: &str) -> Error {
        self.active.store(false, Ordering::SeqCst);
        self.changed.notify_waiters();
        lost(message)
    }

    fn remaining(&self) -> Result<Duration> {
        if !self.active.load(Ordering::SeqCst)
            || self
                .client
                .worker_storage_admission
                .as_ref()
                .is_some_and(|value| value.stop.load(Ordering::SeqCst))
        {
            return Err(self.invalidate("callback settled, claim lost or worker stopping"));
        }
        self.state
            .lock()
            .map_err(|_| lost("claim state poisoned"))?
            .remaining()
            .map_err(|error| self.invalidate(&error.to_string()))
    }

    pub(crate) fn attach_session(&self, handle: Option<&WorkerSession>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| lost("claim state poisoned"))?;
        match (&mut state.session, handle) {
            (Some(session), Some(handle)) => session.attach(handle),
            (None, None) => Ok(()),
            _ => Err(self.invalidate("required session is unavailable")),
        }
    }

    pub(crate) fn refresh_delivery_session(&self, task: &mut Value) -> Result<()> {
        self.remaining()?;
        let state = self
            .state
            .lock()
            .map_err(|_| lost("claim state poisoned"))?;
        if let Some(session) = &state.session {
            task["worker_session"] = session.snapshot().clone();
        }
        Ok(())
    }

    pub(crate) fn governs(&self, path: &str) -> bool {
        (path.starts_with("/worker/activity-tasks/")
            && ["/complete", "/fail", "/heartbeat", "/renew-lease"]
                .iter()
                .any(|action| path.ends_with(action)))
            || path == "/external-payloads/v1"
    }

    pub(crate) fn check(&self, path: &str, body: Option<&Value>) -> Result<()> {
        if !self.governs(path) {
            return Ok(());
        }
        self.remaining()?;
        if path.starts_with("/worker/activity-tasks/") {
            let prefix = format!(
                "/worker/activity-tasks/{}/",
                percent_encode_path_segment(&self.task_id)
            );
            if !path.starts_with(&prefix)
                || body.is_none_or(|body| {
                    body["activity_attempt_id"].as_str() != Some(self.attempt_id.as_str())
                        || body["lease_owner"].as_str() != Some(self.owner.as_str())
                })
            {
                return Err(self.invalidate("publication changed its original claim identity"));
            }
        }
        Ok(())
    }

    pub(crate) async fn protect<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::pin!(operation);
        let stopped = async {
            match &self.client.worker_storage_admission {
                Some(admission) => wait_for_worker_stop(&admission.stop).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(stopped);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let remaining = self.remaining()?;
            tokio::select! {
                biased;
                _ = &mut stopped => return Err(self.invalidate("worker stopping")),
                _ = tokio::time::sleep(remaining) => { self.remaining()?; },
                _ = &mut changed => { self.remaining()?; },
                value = &mut operation => { self.remaining()?; return value; },
            }
        }
    }

    pub(crate) fn progress(&self, reply: &Value, started: Clock) -> Result<()> {
        self.remaining()?;
        if reply["heartbeat_recorded"] != true
            || reply["can_continue"] != true
            || reply["cancel_requested"] != false
            || reply.get("reason").is_some_and(|value| !value.is_null())
            || reply["task_id"].as_str() != Some(self.task_id.as_str())
            || reply["activity_attempt_id"].as_str() != Some(self.attempt_id.as_str())
            || reply["lease_owner"].as_str() != Some(self.owner.as_str())
        {
            return Err(self.invalidate("heartbeat did not acknowledge this original activity"));
        }
        let result = self
            .state
            .lock()
            .map_err(|_| lost("claim state poisoned"))?
            .progress(reply, started);
        result.map_err(|error| self.invalidate(&error.to_string()))?;
        self.changed.notify_waiters();
        Ok(())
    }

    /// Ask the Server to extend this claim once. `Ok(false)` reports a
    /// transient failure: the claim stays valid under its last acknowledged
    /// grant and the caller retries within that window.
    async fn renew(&self) -> Result<bool> {
        let started = Clock::now();
        let remaining = self.remaining()?;
        let window = self.window()?;
        if window.is_zero() {
            self.remaining()?;
            return Ok(false);
        }
        // Spend at most half of the remaining grant on one attempt, so a
        // stalled request still leaves time to retry, and never the release
        // reserve at the end of the lease.
        let budget = if remaining > RETRY_FLOOR {
            remaining / 2
        } else {
            remaining
        }
        .min(window)
        .min(MAX_REQUEST_WINDOW);
        let reply = self
            .client
            .renew_activity_task_lease(&self.task_id, &self.attempt_id, &self.owner, budget)
            .await;
        let transient = match &reply {
            Err(Error::Timeout) => Some("renew-lease timed out".to_owned()),
            Err(Error::Transport(error)) if error.is_timeout() => {
                Some("renew-lease timed out".to_owned())
            }
            Err(Error::Transport(error)) => Some(format!("renew-lease transport error: {error}")),
            Err(Error::ActivityTaskRejected(rejection))
                if rejection.status >= 500 || rejection.status == 429 =>
            {
                Some(format!("renew-lease returned HTTP {}", rejection.status))
            }
            Err(Error::Http { status, .. })
                if status.is_server_error() || status.as_u16() == 429 =>
            {
                Some(format!("renew-lease returned HTTP {}", status.as_u16()))
            }
            _ => None,
        };
        if let Some(cause) = transient {
            self.remaining()?;
            *self
                .last_failure
                .lock()
                .map_err(|_| lost("claim state poisoned"))? = Some(cause);
            return Ok(false);
        }
        let reply = match reply {
            Ok(value) => value,
            Err(error) => return Err(self.invalidate(&error.to_string())),
        };
        self.remaining()?;
        if reply["lease_renewed"] != true
            || reply["can_continue"] != true
            || reply["cancel_requested"] != false
            || reply["heartbeat_recorded"] != false
            || reply.get("reason").is_some_and(|value| !value.is_null())
            || self
                .identity
                .as_object()
                .unwrap()
                .iter()
                .any(|(key, value)| reply.get(key) != Some(value))
        {
            return Err(self.invalidate("renewal changed the original identity or authority"));
        }
        let result = self
            .state
            .lock()
            .map_err(|_| lost("claim state poisoned"))?
            .renewal(&reply, started);
        result.map_err(|error| self.invalidate(&error.to_string()))?;
        self.changed.notify_waiters();
        Ok(true)
    }

    fn window(&self) -> Result<Duration> {
        Ok(self
            .state
            .lock()
            .map_err(|_| lost("claim state poisoned"))?
            .renewal_window())
    }

    /// Renewal has failed and only the release reserve of the lease is left.
    fn release_due(&self, failures: u32) -> Result<bool> {
        Ok(failures > 0 && self.window()?.is_zero())
    }

    /// Stop holding a claim whose renewal did not recover. Publication and the
    /// callback end at once; [`Lease::settle_release`] reports it to the Server.
    fn give_up(&self) -> Error {
        let cause = self
            .last_failure
            .lock()
            .ok()
            .and_then(|cause| cause.clone())
            .unwrap_or_else(|| "renew-lease did not succeed".to_owned());
        if let Ok(mut release) = self.release.lock() {
            release.get_or_insert(cause);
        }
        self.invalidate("renewal did not recover before the release reserve; releasing the claim")
    }

    async fn maintain(&self, confirmed: bool) -> Result<()> {
        let mut failures: u32 = u32::from(!confirmed);
        loop {
            if self.release_due(failures)? {
                return Err(self.give_up());
            }
            let mut delay = if failures == 0 {
                (self.remaining()? / 3).min(retry_delay(failures))
            } else {
                // Retry at the backoff cadence, and wake when the reserve
                // starts so the release still fits inside the lease.
                retry_delay(failures).min(self.window()?)
            };
            if let Some(interval) = self
                .state
                .lock()
                .map_err(|_| lost("claim state poisoned"))?
                .heartbeat_interval
            {
                delay = delay.min(Duration::from_secs(interval as u64) / 3);
            }
            tokio::time::sleep(delay).await;
            failures = if self.renew().await? {
                0
            } else {
                failures.saturating_add(1)
            };
        }
    }
}

/// Smallest remaining grant that is still split between attempts.
const RETRY_FLOOR: Duration = Duration::from_millis(2);

/// Upper bound of the lease time kept back to release an unrenewed claim.
const RELEASE_RESERVE: Duration = Duration::from_secs(10);
/// Upper bound of the lease time that must remain when a release is sent.
const RELEASE_MARGIN: Duration = Duration::from_secs(1);

/// `failure.type` of a released claim. The Server records it as the attempt's
/// `exception_type`, so retry policies can match it.
pub(crate) const WORKER_LOST_TYPE: &str = "worker_lost";
/// `failure.class` of a released claim, recorded as `exception_class`.
pub(crate) const WORKER_LOST_CLASS: &str = "ActivityWorkerLost";

/// Renewal cadence: steady state after an acknowledged grant. After the
/// n-th consecutive failure (n from 0) the wait is drawn uniformly from
/// `[0, min(4 s, 250 ms * 2^n)]` (full jitter), the same in every SDK. The
/// caller also caps every wait so retries and the release fit in the lease.
fn retry_delay(failures: u32) -> Duration {
    match failures {
        0 => Duration::from_secs(30),
        failures => backoff_ceiling(failures).mul_f64(unit_random()),
    }
}

fn backoff_ceiling(failures: u32) -> Duration {
    Duration::from_millis(250 << failures.saturating_sub(1).min(4))
}

/// A uniform sample from `[0, 1)` from the OS random source behind UUID v4.
fn unit_random() -> f64 {
    let (bits, _) = uuid::Uuid::new_v4().as_u64_pair();
    (bits >> 11) as f64 / (1_u64 << 53) as f64
}

#[derive(Debug)]
pub(crate) struct Lease {
    pub(crate) authority: Arc<Authority>,
    maintenance: tokio::task::JoinHandle<()>,
}

struct MaintenanceExit(Arc<Authority>);
impl Drop for MaintenanceExit {
    fn drop(&mut self) {
        self.0.invalidate("lease-maintenance task stopped");
    }
}

impl Lease {
    pub(crate) async fn confirm(
        client: &Client,
        wire: &Value,
        worker: &str,
        queue: &str,
        requested: Clock,
    ) -> Result<Option<Arc<Self>>> {
        client.observe_worker_activity_lease_capabilities(&wire["server_capabilities"]);
        if wire["task"].is_null()
            || !client
                .activity_lease_renewal_supported
                .load(Ordering::SeqCst)
        {
            return Ok(None);
        }
        let authority = Arc::new(Authority::delivery(client, wire, worker, queue, requested)?);
        // A refusal or a changed identity still ends the claim before the
        // callback starts. A timeout, transport error, 429 or 5xx does not:
        // the poll's own grant stays valid, so the callback starts under it and
        // maintenance retries the renewal with backoff inside that grant.
        // Dropping the claim here would leave the Server holding a lease that
        // nobody settles until it expires (sdk-php#445).
        let confirmed = authority.renew().await?;
        if authority.release_due(u32::from(!confirmed))? {
            let error = authority.give_up();
            authority.send_release().await;
            return Err(error);
        }
        let maintained = Arc::clone(&authority);
        let (started_tx, started_rx) = oneshot::channel();
        let maintenance = tokio::spawn(async move {
            let _exit = MaintenanceExit(Arc::clone(&maintained));
            let _ = started_tx.send(());
            if let Err(error) = maintained.maintain(confirmed).await {
                maintained.invalidate(&error.to_string());
            }
        });
        let lease = Arc::new(Self {
            authority,
            maintenance,
        });
        // Poll the maintainer before callback entry. A newly spawned task can
        // remain in Tokio's non-stealable LIFO slot while its spawning task
        // blocks, even when another executor thread is available.
        started_rx.await.map_err(|_| {
            lease
                .authority
                .invalidate("lease-maintenance task stopped before callback entry")
        })?;
        lease.authority.remaining()?;
        Ok(Some(lease))
    }

    /// After the claim ended, release it through the Server if the Worker gave
    /// it up because renewal did not recover. Server refusals, expiry and
    /// worker shutdown send nothing.
    pub(crate) async fn settle_release(&self) {
        self.authority.send_release().await;
    }
}

impl Authority {
    /// Fail the attempt as retryable `worker_lost`, once.
    ///
    /// Callers invoke this only after the callback has confirmed stopped: it
    /// never started, or its future was dropped (activity callbacks run
    /// inline in the poll task, never spawned). A blocking callback can delay
    /// that drop, so the report is sent only while more than a margin of the
    /// lease is left. Otherwise nothing is sent and the Server records
    /// `lease_expired`; two executions of one attempt are never possible.
    async fn send_release(&self) {
        let Some(cause) = self.release.lock().ok().and_then(|cause| cause.clone()) else {
            return;
        };
        let Ok((lease_left, margin)) = self.state.lock().map(|state| {
            (
                state.lease_until.saturating_duration_since(Clock::now()),
                RELEASE_MARGIN.min(state.reserve() / 2),
            )
        }) else {
            return;
        };
        if lease_left <= margin {
            tracing::warn!(
                target: "durable_workflow::activity_lease",
                task_id = %self.task_id,
                activity_attempt_id = %self.attempt_id,
                "activity callback stopped too late to release its claim; the Server settles it at lease expiry"
            );
            return;
        }
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        let budget = lease_left;
        let failure = json!({
            "type": WORKER_LOST_TYPE,
            "class": WORKER_LOST_CLASS,
            "non_retryable": false,
            "message": format!(
                "worker released activity attempt {} because its lease could not be renewed: {cause}",
                self.attempt_id
            ),
        });
        // `self.client` carries no lease authority, so this report is not
        // fenced by the claim it releases.
        let reported = tokio::time::timeout(
            budget,
            self.client.fail_activity_task_with_failure(
                &self.task_id,
                &self.attempt_id,
                &self.owner,
                failure,
            ),
        )
        .await;
        if !matches!(reported, Ok(Ok(_))) {
            tracing::warn!(
                target: "durable_workflow::activity_lease",
                task_id = %self.task_id,
                activity_attempt_id = %self.attempt_id,
                "could not release an unrenewable activity claim; the Server settles it at lease expiry"
            );
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.authority
            .invalidate("activity claim settled or abandoned");
        self.maintenance.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_backoff_uses_full_jitter_up_to_a_four_second_cap() {
        assert_eq!(retry_delay(0), Duration::from_secs(30));
        let ceilings = [250, 500, 1_000, 2_000, 4_000, 4_000, 4_000];
        for (failures, ceiling) in (1..).zip(ceilings) {
            assert_eq!(backoff_ceiling(failures), Duration::from_millis(ceiling));
            let samples: Vec<_> = (0..200).map(|_| retry_delay(failures)).collect();
            assert!(samples
                .iter()
                .all(|delay| *delay <= Duration::from_millis(ceiling)));
            // Full jitter spreads over the whole range, not just near the cap.
            assert!(samples
                .iter()
                .any(|delay| *delay < Duration::from_millis(ceiling / 2)));
        }
        assert_eq!(backoff_ceiling(u32::MAX), Duration::from_secs(4));
    }
}
