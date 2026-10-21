//! One activity's original identity and acknowledged runtime authority.

use super::*;
use chrono::{FixedOffset, SecondsFormat};
use tokio::sync::Notify;
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

    fn delivery(task: &Value, server_time: &Value, worker: &str, namespace: &str) -> Result<Self> {
        let captured = Clock::now();
        let server_time = timestamp(server_time)?;
        let lease_expiry = timestamp(&task["lease_expires_at"])?;
        if duration(lease_expiry, server_time)? > Duration::from_secs(86_400) {
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
}

impl Authority {
    fn delivery(client: &Client, wire: &Value, worker: &str, queue: &str) -> Result<Self> {
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
            || !task["attempt_number"]
                .as_u64()
                .is_some_and(|value| value > 0)
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
            )?),
            active: AtomicBool::new(true),
            changed: Notify::new(),
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

    async fn renew(&self) -> Result<bool> {
        let started = Clock::now();
        let budget = self.remaining()?.min(MAX_REQUEST_WINDOW);
        let reply = self
            .client
            .renew_activity_task_lease(&self.task_id, &self.attempt_id, &self.owner, budget)
            .await;
        let reply = match reply {
            Ok(value) => value,
            Err(Error::Timeout | Error::Transport(_)) => {
                self.remaining()?;
                return Ok(false);
            }
            Err(Error::ActivityTaskRejected(ref rejection))
                if rejection.status >= 500 || rejection.status == 429 =>
            {
                self.remaining()?;
                return Ok(false);
            }
            Err(Error::Http { ref status, .. })
                if status.is_server_error() || status.as_u16() == 429 =>
            {
                self.remaining()?;
                return Ok(false);
            }
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

    async fn maintain(&self) -> Result<()> {
        let mut confirmed = true;
        loop {
            let mut delay = (self.remaining()? / 3).min(if confirmed {
                Duration::from_secs(30)
            } else {
                Duration::from_secs(1)
            });
            if let Some(interval) = self
                .state
                .lock()
                .map_err(|_| lost("claim state poisoned"))?
                .heartbeat_interval
            {
                delay = delay.min(Duration::from_secs(interval as u64) / 3);
            }
            tokio::time::sleep(delay).await;
            confirmed = self.renew().await?;
        }
    }
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
    ) -> Result<Option<Arc<Self>>> {
        client.observe_activity_lease_capabilities(&wire["server_capabilities"]);
        if wire["task"].is_null()
            || !client
                .activity_lease_renewal_supported
                .load(Ordering::SeqCst)
        {
            return Ok(None);
        }
        let authority = Arc::new(Authority::delivery(client, wire, worker, queue)?);
        if !authority.renew().await? {
            return Err(authority.invalidate("initial lease confirmation unavailable"));
        }
        let maintained = Arc::clone(&authority);
        let maintenance = tokio::spawn(async move {
            let _exit = MaintenanceExit(Arc::clone(&maintained));
            if let Err(error) = maintained.maintain().await {
                maintained.invalidate(&error.to_string());
            }
        });
        Ok(Some(Arc::new(Self {
            authority,
            maintenance,
        })))
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.authority
            .invalidate("activity claim settled or abandoned");
        self.maintenance.abort();
    }
}
