//! Synthetic receipt provider and bounded service activity. See CHECKPOINTED_BATCH.md.
use durable_workflow::{
    json, ActivityError, ActivityOptions, ActivityRetryPolicy, Client, Error, Result, Uuid, Value,
    Worker, WorkflowResultOptions,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::{Read, Write},
    path::PathBuf,
    time::Duration,
};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum FailurePoint {
    BeforeCommit,
    AfterCommit,
    AfterCheckpoint,
}

#[derive(Clone, Deserialize, Serialize)]
struct Batch {
    business_id: String,
    items: Vec<String>,
    failure: FailurePoint,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Checkpoint {
    batch_sha256: String,
    next_item: usize,
}

fn invalid(message: &str) -> Error {
    ActivityError::new("BatchRefused", message)
        .non_retryable(true)
        .into()
}

fn planned_failure() -> Error {
    ActivityError::new(
        "PlannedFailure",
        "the first attempt stops at the chosen boundary",
    )
    .into()
}

fn provider_io(error: std::io::Error) -> Error {
    ActivityError::new("ProviderUnavailable", error.to_string()).into()
}

impl Batch {
    fn fingerprint(&self) -> Result<String> {
        if self.business_id.is_empty()
            || self.business_id.len() > 128
            || self.items.is_empty()
            || self.items.len() > 16
            || self
                .items
                .iter()
                .any(|item| item.is_empty() || item.len() > 128)
        {
            return Err(invalid(
                "use a business ID and between one and sixteen bounded items",
            ));
        }
        let bytes = serde_json::to_vec(&(&self.business_id, &self.items))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// This directory is a synthetic provider's durable state, separate from checkpoints.
#[derive(Clone)]
struct ReceiptStore(PathBuf);

impl ReceiptStore {
    fn commit_or_reconcile(&self, business_id: &str, index: usize, item: &str) -> Result<bool> {
        // A business key survives activity retries and workflow restarts. An attempt
        // ID would create a different operation instead of reconciling the old one.
        let key = serde_json::to_vec(&(business_id, index))?;
        let path = self.0.join(format!("{:x}.json", Sha256::digest(&key)));
        let receipt = serde_json::to_vec(&json!({"key":[business_id,index],"item":item}))?;
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                file.write_all(&receipt).map_err(provider_io)?;
                file.sync_all().map_err(provider_io)?;
                File::open(&self.0)
                    .and_then(|directory| directory.sync_all())
                    .map_err(provider_io)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut existing = Vec::new();
                let mut file = File::open(path).map_err(provider_io)?;
                (&mut file)
                    .take(2049)
                    .read_to_end(&mut existing)
                    .map_err(provider_io)?;
                if existing != receipt {
                    return Err(invalid(
                        "provider receipt is incomplete or conflicts; reconcile before retrying",
                    ));
                }
                // A previous writer may have lost its sync response. Re-establish
                // durability before acknowledging a reconciled receipt as well.
                file.sync_all().map_err(provider_io)?;
                File::open(&self.0)
                    .and_then(|directory| directory.sync_all())
                    .map_err(provider_io)?;
                Ok(false)
            }
            Err(error) => Err(provider_io(error)),
        }
    }
}

async fn process_batch<A, F>(
    store: &ReceiptStore,
    batch: &Batch,
    received: Option<Checkpoint>,
    attempt: u64,
    mut acknowledge: A,
) -> Result<Value>
where
    A: FnMut(Checkpoint) -> F,
    F: Future<Output = Result<()>>,
{
    let fingerprint = batch.fingerprint()?;
    let next = match received {
        Some(checkpoint)
            if checkpoint.batch_sha256 == fingerprint
                && checkpoint.next_item <= batch.items.len() =>
        {
            checkpoint.next_item
        }
        Some(_) => {
            return Err(invalid(
                "checkpoint belongs to different input or is out of range",
            ))
        }
        None => 0,
    };
    for index in next..batch.items.len() {
        let fail = |point| attempt == 1 && index == 0 && batch.failure == point;
        if fail(FailurePoint::BeforeCommit) {
            return Err(planned_failure());
        }
        store.commit_or_reconcile(&batch.business_id, index, &batch.items[index])?;
        if fail(FailurePoint::AfterCommit) {
            return Err(planned_failure());
        }
        acknowledge(Checkpoint {
            batch_sha256: fingerprint.clone(),
            next_item: index + 1,
        })
        .await?;
        if fail(FailurePoint::AfterCheckpoint) {
            return Err(planned_failure());
        }
    }
    Ok(json!({"business_id":batch.business_id,"completed":batch.items.len()}))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let directory = arguments
        .next()
        .ok_or_else(|| invalid("supply a new receipt directory"))?;
    let failure = match arguments.next().as_deref() {
        Some("before-commit") => FailurePoint::BeforeCommit,
        Some("after-commit") => FailurePoint::AfterCommit,
        Some("after-checkpoint") => FailurePoint::AfterCheckpoint,
        _ => {
            return Err(invalid(
                "choose before-commit, after-commit or after-checkpoint",
            ))
        }
    };
    if arguments.next().is_some() {
        return Err(invalid(
            "expected exactly a receipt directory and failure point",
        ));
    }
    // Refuse an existing directory so this demo cannot mix with application data.
    std::fs::create_dir(&directory).map_err(provider_io)?;
    let store = ReceiptStore(PathBuf::from(directory));
    let client = Client::builder(
        std::env::var("DURABLE_WORKFLOW_SERVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
    )
    .token(std::env::var("DURABLE_WORKFLOW_TOKEN").ok())
    .namespace("default")
    .build()?;
    let queue = format!("checkpointed-batch-{}", Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue).poll_timeout(Duration::from_millis(250));
    worker.register_typed_activity("checkpointed-batch", move |ctx, batch: Batch| {
        let store = store.clone();
        async move {
            let received = ctx
                .last_heartbeat_checkpoint()
                .map_err(|_| invalid("this activity requires activity_heartbeat_resume v1"))?
                .map(|checkpoint| {
                    checkpoint
                        .details_as::<Checkpoint>()
                        .map_err(|_| invalid("checkpoint details do not match this batch schema"))
                })
                .transpose()?;
            process_batch(&store, &batch, received, ctx.attempt_number, |checkpoint| {
                let ctx = ctx.clone();
                async move {
                    let response = ctx.heartbeat(checkpoint).await?;
                    if !response.heartbeat_recorded || response.should_stop() {
                        return Err(invalid(
                            "checkpoint was not acknowledged or the attempt must stop",
                        ));
                    }
                    Ok(())
                }
            })
            .await
        }
    });
    worker.register_typed_workflow("checkpointed-batch", |ctx, batch: Batch| async move {
        ctx.activity_typed_with_options::<_, Value>(
            "checkpointed-batch",
            ActivityOptions::new()
                .retry_policy(
                    ActivityRetryPolicy::new(2).backoff_intervals([Duration::from_secs(1)]),
                )
                .heartbeat_timeout(Duration::from_secs(10))
                .start_to_close_timeout(Duration::from_secs(30))
                .schedule_to_close_timeout(Duration::from_secs(60)),
            batch,
        )
        .await
    });
    worker.register().await?;
    let handle = client
        .start_workflow(
            "checkpointed-batch",
            &queue,
            &queue,
            Batch {
                business_id: format!("demo-{}", Uuid::new_v4()),
                items: vec!["first".into(), "second".into(), "third".into()],
                failure,
            },
        )
        .await?;
    let watcher = handle.clone();
    worker
        .run_until(async move {
            let _ = tokio::time::timeout(Duration::from_secs(70), async move {
                loop {
                    if watcher.describe().await.is_ok_and(|run| run.is_terminal()) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            })
            .await;
        })
        .await?;
    let result = handle
        .result(WorkflowResultOptions {
            poll_interval: Duration::from_millis(250),
            timeout: Duration::from_secs(3),
        })
        .await?;
    println!("{result}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        store: ReceiptStore,
    }
    impl Fixture {
        fn new() -> Self {
            let directory =
                std::env::temp_dir().join(format!("checkpoint-batch-{}", Uuid::new_v4()));
            std::fs::create_dir(&directory).unwrap();
            Self {
                store: ReceiptStore(directory),
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.store.0).unwrap();
        }
    }
    fn batch(failure: FailurePoint) -> Batch {
        Batch {
            business_id: "business-batch".into(),
            items: vec!["first".into(), "second".into(), "third".into()],
            failure,
        }
    }

    #[tokio::test]
    async fn retries_reconcile_commit_and_resume_only_acknowledged_items() {
        for point in [
            FailurePoint::BeforeCommit,
            FailurePoint::AfterCommit,
            FailurePoint::AfterCheckpoint,
        ] {
            let fixture = Fixture::new();
            let batch = batch(point);
            let mut persisted = None;
            let first = process_batch(&fixture.store, &batch, None, 1, |checkpoint| {
                persisted = Some(checkpoint);
                std::future::ready(Ok(()))
            })
            .await;
            assert!(first.is_err());
            assert_eq!(
                persisted.as_ref().map(|value| value.next_item),
                if point == FailurePoint::AfterCheckpoint {
                    Some(1)
                } else {
                    None
                }
            );
            assert_eq!(
                std::fs::read_dir(&fixture.store.0).unwrap().count(),
                if point == FailurePoint::BeforeCommit {
                    0
                } else {
                    1
                }
            );
            let mut resumed = Vec::new();
            let result = process_batch(&fixture.store, &batch, persisted, 2, |checkpoint| {
                resumed.push(checkpoint.next_item);
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
            assert_eq!(
                resumed,
                if point == FailurePoint::AfterCheckpoint {
                    vec![2, 3]
                } else {
                    vec![1, 2, 3]
                }
            );
            assert_eq!(result["completed"], 3);
            assert_eq!(std::fs::read_dir(&fixture.store.0).unwrap().count(), 3);
            for (index, item) in batch.items.iter().enumerate() {
                assert!(!fixture
                    .store
                    .commit_or_reconcile(&batch.business_id, index, item)
                    .unwrap());
            }
        }
    }

    #[tokio::test]
    async fn lost_ack_response_reconciles_with_or_without_a_committed_checkpoint() {
        for committed in [false, true] {
            let fixture = Fixture::new();
            let batch = batch(FailurePoint::AfterCheckpoint);
            let mut persisted = None;
            assert!(
                process_batch(&fixture.store, &batch, None, 1, |checkpoint| {
                    if committed {
                        persisted = Some(checkpoint);
                    }
                    std::future::ready(Err(Error::Timeout))
                })
                .await
                .is_err()
            );
            let result = process_batch(&fixture.store, &batch, persisted, 2, |_| {
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
            assert_eq!(result["completed"], 3);
            assert_eq!(std::fs::read_dir(&fixture.store.0).unwrap().count(), 3);
        }
    }

    #[tokio::test]
    async fn mismatched_checkpoint_or_unbounded_batch_is_refused_before_provider_access() {
        let fixture = Fixture::new();
        let batch = batch(FailurePoint::AfterCommit);
        for checkpoint in [
            Checkpoint {
                batch_sha256: "foreign".into(),
                next_item: 1,
            },
            Checkpoint {
                batch_sha256: batch.fingerprint().unwrap(),
                next_item: 4,
            },
        ] {
            let acknowledged = std::cell::Cell::new(false);
            assert!(
                process_batch(&fixture.store, &batch, Some(checkpoint), 2, |_| {
                    acknowledged.set(true);
                    std::future::ready(Ok(()))
                })
                .await
                .is_err()
            );
            assert!(!acknowledged.get());
        }
        let mut changed = batch;
        changed.items = vec!["item".into(); 17];
        assert!(
            process_batch(&fixture.store, &changed, None, 2, |_| std::future::ready(
                Ok(())
            ))
            .await
            .is_err()
        );
        assert_eq!(std::fs::read_dir(&fixture.store.0).unwrap().count(), 0);
    }

    #[test]
    fn provider_refuses_conflicting_or_partial_receipts() {
        let fixture = Fixture::new();
        assert!(fixture
            .store
            .commit_or_reconcile("batch", 0, "first")
            .unwrap());
        assert!(fixture
            .store
            .commit_or_reconcile("batch", 0, "changed")
            .is_err());
        let path = std::fs::read_dir(&fixture.store.0)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(path, b"").unwrap();
        assert!(fixture
            .store
            .commit_or_reconcile("batch", 0, "first")
            .is_err());
    }
}
