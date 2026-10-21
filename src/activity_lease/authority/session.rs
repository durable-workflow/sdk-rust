//! Only acknowledged same-holder session grants extend session authority.

use super::{lost, timestamp, until, Clock, Timestamp};
use crate::{Result, WorkerSession, WorkerSessionOptions};
use serde_json::Value;

#[derive(Debug)]
pub(super) struct Session {
    options: Value,
    owner: String,
    namespace: String,
    ttl: Timestamp,
    ttl_until: Clock,
    lease_until: Clock,
    lease_expiry: Timestamp,
    snapshot: Value,
    handle: Option<WorkerSession>,
}

impl Session {
    pub(super) fn delivery(
        task: &Value,
        server: Timestamp,
        captured: Clock,
        worker: &str,
        namespace: &str,
    ) -> Result<Option<Self>> {
        let Some(value) = task.get("worker_session").filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let options: WorkerSessionOptions = serde_json::from_value(value.clone())?;
        let ttl = timestamp(&value["ttl_expires_at"])?;
        let expiry = timestamp(&value["lease_expires_at"])?;
        let mut session = Self {
            options: options.to_wire()?,
            owner: worker.to_owned(),
            namespace: namespace.to_owned(),
            ttl,
            ttl_until: until(captured, ttl, server)?,
            lease_until: until(captured, expiry, server)?,
            lease_expiry: expiry,
            snapshot: value.clone(),
            handle: None,
        };
        session.acknowledge(value, server, captured)?;
        Ok(Some(session))
    }

    pub(super) fn bound(&self) -> Clock {
        self.ttl_until.min(self.lease_until)
    }

    pub(super) fn snapshot(&self) -> &Value {
        &self.snapshot
    }

    pub(super) fn acknowledge(
        &mut self,
        value: &Value,
        server: Timestamp,
        started: Clock,
    ) -> Result<()> {
        let options: WorkerSessionOptions = serde_json::from_value(value.clone())?;
        let expiry = timestamp(&value["lease_expires_at"])?;
        if options.to_wire()? != self.options
            || value["status"] != "active"
            || value["lease_owner"].as_str() != Some(self.owner.as_str())
            || value
                .get("namespace")
                .is_some_and(|value| value.as_str() != Some(self.namespace.as_str()))
            || timestamp(&value["ttl_expires_at"])? != self.ttl
            || expiry < self.lease_expiry
        {
            return Err(lost(
                "renewal changed required session identity, holder or original TTL",
            ));
        }
        self.ttl_until = self.ttl_until.min(until(started, self.ttl, server)?);
        self.lease_until = self.lease_until.max(until(started, expiry, server)?);
        self.lease_expiry = expiry;
        self.snapshot = value.clone();
        if let Some(handle) = &self.handle {
            handle.track(value)?;
        }
        Ok(())
    }

    pub(super) fn attach(&mut self, handle: &WorkerSession) -> Result<()> {
        handle.track(&self.snapshot)?;
        self.handle = Some(handle.clone());
        Ok(())
    }
}
