use crate::{wait_for_worker_stop, ManagedPollOutcome, Result, Worker};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(super) struct TaskCapacity {
    permits: Arc<Semaphore>,
    active: Arc<AtomicUsize>,
    limit: usize,
}

impl TaskCapacity {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(limit)),
            active: Arc::new(AtomicUsize::new(0)),
            limit,
        })
    }

    pub(super) fn available(&self) -> usize {
        self.limit
            .saturating_sub(self.active.load(Ordering::SeqCst))
    }

    async fn reserve(&self, stop: Option<&Arc<AtomicBool>>) -> Option<Reservation> {
        let stopped = async {
            match stop {
                Some(stop) => wait_for_worker_stop(stop).await,
                None => std::future::pending().await,
            }
        };
        let permit = tokio::select! {
            biased;
            _ = stopped => return None,
            permit = self.permits.clone().acquire_owned() => permit.ok()?,
        };
        Some(Reservation {
            _permit: permit,
            marker: Arc::new(TaskSlotMarker {
                active: self.active.clone(),
                state: Mutex::new(SlotState::Polling),
            }),
        })
    }
}

#[derive(Debug, PartialEq)]
enum SlotState {
    Polling,
    Active,
    Closed,
}

/// Only the reservation owns the permit. A retained callback Client cannot
/// keep a settled task's capacity occupied or reopen its slot.
#[derive(Debug)]
pub(super) struct TaskSlotMarker {
    active: Arc<AtomicUsize>,
    state: Mutex<SlotState>,
}

impl TaskSlotMarker {
    pub(super) fn claimed(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if *state == SlotState::Polling {
            self.active.fetch_add(1, Ordering::SeqCst);
            *state = SlotState::Active;
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if *state == SlotState::Active {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        *state = SlotState::Closed;
    }
}

struct Reservation {
    _permit: OwnedSemaphorePermit,
    marker: Arc<TaskSlotMarker>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.marker.close();
    }
}

#[derive(Clone, Copy)]
enum TaskFamily {
    Workflow,
    Activity,
}

impl Worker {
    pub(super) async fn poll_workflow_once(&self) -> Result<ManagedPollOutcome> {
        let Some(slot) = self.workflow_capacity.reserve(self.run_stop.as_ref()).await else {
            return Ok(ManagedPollOutcome::Stop);
        };
        let mut worker = self.clone();
        worker.client.worker_task_slot = Some(slot.marker.clone());
        worker.poll_workflow_once_reserved().await
    }

    pub(super) async fn poll_activity_once(&self) -> Result<ManagedPollOutcome> {
        let Some(slot) = self.activity_capacity.reserve(self.run_stop.as_ref()).await else {
            return Ok(ManagedPollOutcome::Stop);
        };
        let mut worker = self.clone();
        worker.client.worker_task_slot = Some(slot.marker.clone());
        worker.poll_activity_once_reserved().await
    }

    pub(super) async fn poll_workflows_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        self.poll_task_lanes(stop, TaskFamily::Workflow).await
    }

    pub(super) async fn poll_activities_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        self.poll_task_lanes(stop, TaskFamily::Activity).await
    }

    async fn poll_task_lanes(self, stop: Arc<AtomicBool>, family: TaskFamily) -> Result<()> {
        let count = match family {
            TaskFamily::Workflow => self.max_concurrent_workflow_tasks,
            TaskFamily::Activity => self.max_concurrent_activity_tasks,
        };
        let mut lanes = FuturesUnordered::new();
        for _ in 0..count {
            let worker = self.clone();
            let stop = stop.clone();
            lanes.push(async move {
                while !stop.load(Ordering::SeqCst) {
                    let outcome = match family {
                        TaskFamily::Workflow => worker.poll_workflow_once().await,
                        TaskFamily::Activity => worker.poll_activity_once().await,
                    };
                    match outcome {
                        Ok(ManagedPollOutcome::Stop) => {
                            stop.store(true, Ordering::SeqCst);
                            break;
                        }
                        Err(error) => {
                            stop.store(true, Ordering::SeqCst);
                            return Err(error);
                        }
                        _ => {}
                    }
                }
                Ok(())
            });
        }
        // Retain every claimed task through settlement before deregistration.
        let mut first_error = None;
        while let Some(result) = lanes.next().await {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn empty_polls_do_not_consume_advertised_capacity_and_retained_clients_cannot_reopen_slots(
    ) {
        let capacity = TaskCapacity::new(1);
        let reservation = capacity.reserve(None).await.unwrap();
        assert_eq!(capacity.available(), 1, "a pending poll can receive work");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), capacity.reserve(None))
                .await
                .is_err()
        );
        let retained = reservation.marker.clone();
        retained.claimed();
        retained.claimed();
        assert_eq!(capacity.available(), 0);
        drop(reservation);
        assert_eq!(capacity.available(), 1);
        retained.claimed();
        assert_eq!(
            capacity.available(),
            1,
            "a retained callback client is no longer an active claim"
        );
        assert!(capacity.reserve(None).await.is_some());
    }
}
