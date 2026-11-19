//! Run unchanged in a registry-only consumer. Transport loss and legacy
//! readback run through `scripts/acceptance/command-keys/run_open.py`.
use durable_workflow::{json, Client, Error, RawRequest, UpdateWaitStage, Value, Worker};
use std::time::Duration;

fn client(namespace: &str) -> Client {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    assert!(origin
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0));
    Client::builder(origin)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .namespace(namespace)
        .build()
        .unwrap()
}

fn fixture(client: Client, queue: &str) -> Worker {
    let mut worker = Worker::new(client, queue).poll_timeout(Duration::from_millis(10));
    for name in ["keys.wait", "keys.continue"] {
        let continuing = name == "keys.continue";
        worker.register_workflow(name, move |ctx, input| async move {
            ctx.wait_signal("finish").await?;
            if continuing && input[0] == json!(0) {
                ctx.continue_as_new(json!([1]))
            } else {
                Ok(input)
            }
        });
        worker
            .declare_workflow_signals(name, &["finish", "touch", "other"])
            .unwrap();
        for update in ["mutate", "other"] {
            worker.register_update(name, update, |_, arguments| async move { Ok(arguments) });
        }
    }
    worker
}

fn count(history: &Value, kind: &str) -> usize {
    history["history_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == kind)
        .count()
}

fn commands(history: &Value) -> Vec<Value> {
    history["history_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some("SignalReceived" | "UpdateAccepted")
            )
        })
        .cloned()
        .collect()
}

#[tokio::test]
#[ignore = "requires an isolated published Server advertising command_request_keys"]
async fn retained_command_keys_preserve_identity_and_execution_fences() {
    tokio::time::timeout(Duration::from_secs(120), direct_cases())
        .await
        .expect("bounded command-key fixture");
}

async fn direct_cases() {
    let client = client("default");
    let info = client.cluster_info().await.unwrap();
    assert_eq!(info["capabilities"]["command_request_keys"], true);
    let queue = format!("rust-keys-{}", durable_workflow::Uuid::new_v4());
    let worker = fixture(client.clone(), &queue);
    worker.register().await.unwrap();
    let id = format!("{queue}-hold");
    let workflow = client
        .start_workflow("keys.wait", &queue, &id, json!([0]))
        .await
        .unwrap();
    let run = workflow.run_id.as_deref().unwrap();
    let signal = client
        .signal_workflow_with_request_id(&id, "touch", json!([1]), "signal-key")
        .await
        .unwrap();
    assert!(signal["command_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert_eq!(signal["run_id"], run);
    let mut retries = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let client = client.clone();
        let id = id.clone();
        retries.spawn(async move {
            client
                .signal_workflow_with_request_id(&id, "touch", json!([1]), "signal-key")
                .await
                .unwrap()
        });
    }
    while let Some(receipt) = retries.join_next().await {
        let receipt = receipt.unwrap();
        assert_eq!(receipt["retained"], true);
        assert_eq!(receipt["command_id"], signal["command_id"]);
        assert_eq!(receipt["run_id"], signal["run_id"]);
    }
    let history = client.export_workflow_history(&id, run).await.unwrap();
    assert_eq!(count(&history, "SignalReceived"), 1);
    let error = client
        .signal_workflow_with_request_id(&id, "touch", json!([true]), "signal-key")
        .await
        .unwrap_err();
    assert!(error.is_request_id_conflict());
    assert_eq!(error.request_id().as_deref(), Some("signal-key"));
    // A changed name or an explicit run target is a different intent too.
    let error = client
        .signal_workflow_with_request_id(&id, "other", json!([1]), "signal-key")
        .await
        .unwrap_err();
    assert!(error.is_request_id_conflict(), "{error:?}");
    let conflict = error.request_id_conflict().unwrap();
    assert_eq!(conflict.request_id, "signal-key");
    assert_eq!(
        conflict.retained_command_id.as_deref(),
        signal["command_id"].as_str()
    );
    let error = client
        .signal_workflow_run_with_request_id(&id, run, "touch", json!([1]), "signal-key")
        .await
        .unwrap_err();
    assert!(error.is_request_id_conflict(), "{error:?}");
    // Export metadata contains a fresh exported_at timestamp. Only durable
    // history events are a no-write invariant across two exports.
    assert_eq!(
        client.export_workflow_history(&id, run).await.unwrap()["history_events"],
        history["history_events"]
    );

    // A generated key is returned on the receipt and retries like an explicit one.
    let generated = client
        .signal_workflow(&id, "touch", json!([2]))
        .await
        .unwrap();
    let generated_key = generated["request_id"].as_str().unwrap().to_string();
    assert!(generated_key.starts_with("sdk-rust-signal-"));
    let retried = client
        .signal_workflow_with_request_id(&id, "touch", json!([2]), &generated_key)
        .await
        .unwrap();
    assert_eq!(retried["retained"], true);
    assert_eq!(retried["command_id"], generated["command_id"]);

    // Four first submissions racing on one fresh key admit one signal.
    let mut racing = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let client = client.clone();
        let id = id.clone();
        racing.spawn(async move {
            client
                .signal_workflow_with_request_id(&id, "touch", json!([3]), "race-key")
                .await
                .unwrap()
        });
    }
    let mut raced = Vec::new();
    while let Some(receipt) = racing.join_next().await {
        raced.push(receipt.unwrap());
    }
    assert!(raced[0]["command_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert!(
        raced
            .iter()
            .all(|receipt| receipt["command_id"] == raced[0]["command_id"]
                && receipt["run_id"] == run)
    );
    let history = client.export_workflow_history(&id, run).await.unwrap();
    assert_eq!(count(&history, "SignalReceived"), 3);

    let pending = match client
        .update_workflow(&id, "mutate", json!([1]), Some("update-key"))
        .await
        .unwrap_err()
    {
        Error::UpdateStillPending(pending) => pending,
        error => panic!("expected a typed pending receipt: {error:?}"),
    };
    let update_id = pending.update_id.as_deref().unwrap();
    assert_eq!(pending.run_id.as_deref(), Some(run));
    let mut retries = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let client = client.clone();
        let id = id.clone();
        retries.spawn(async move {
            client
                .update_workflow(&id, "mutate", json!([1]), Some("update-key"))
                .await
                .unwrap_err()
        });
    }
    while let Some(error) = retries.join_next().await {
        let Error::UpdateStillPending(retry) = error.unwrap() else {
            panic!("retry must retain a pending receipt")
        };
        assert_eq!(retry.update_id.as_deref(), Some(update_id));
        assert_eq!(retry.run_id, pending.run_id);
        assert_eq!(retry.receipt["retained"], true);
        assert_eq!(retry.receipt["intent_verified"], true);
    }
    let history = client.export_workflow_history(&id, run).await.unwrap();
    assert_eq!(count(&history, "UpdateAccepted"), 1);
    let error = client
        .update_workflow(&id, "mutate", json!([true]), Some("update-key"))
        .await
        .unwrap_err();
    assert!(error.is_request_id_conflict());
    assert_eq!(error.request_id().as_deref(), Some("update-key"));
    let error = client
        .update_workflow(&id, "other", json!([1]), Some("update-key"))
        .await
        .unwrap_err();
    assert!(error.is_request_id_conflict(), "{error:?}");
    let retained = error.request_id_conflict().unwrap().retained_command_id;
    assert!(retained.is_some());
    if let Some(command_id) = pending.receipt["command_id"].as_str() {
        assert_eq!(retained.as_deref(), Some(command_id));
    }
    assert_eq!(
        client.export_workflow_history(&id, run).await.unwrap()["history_events"],
        history["history_events"]
    );

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(async move {
        worker
            .run_until(async {
                let _ = stopped.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let receipt = client
                .update_outcome_receipt(&id, update_id, UpdateWaitStage::Accepted, None)
                .await
                .unwrap();
            if receipt["update_status"] == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    client
        .signal_workflow_with_request_id(&id, "finish", json!([]), "close-hold")
        .await
        .unwrap();
    assert_eq!(
        workflow.result(Default::default()).await.unwrap(),
        json!([0])
    );
    let history = client.export_workflow_history(&id, run).await.unwrap();
    let retained = client
        .signal_workflow_with_request_id(&id, "touch", json!([1]), "signal-key")
        .await
        .unwrap();
    assert_eq!(retained["retained"], true);
    assert_eq!(retained["command_id"], signal["command_id"]);
    assert_eq!(
        client
            .update_workflow(&id, "mutate", json!([1]), Some("update-key"))
            .await
            .unwrap(),
        json!([1])
    );
    assert_eq!(
        client.export_workflow_history(&id, run).await.unwrap()["history_events"],
        history["history_events"]
    );

    let chain = format!("{queue}-continue");
    let first = client
        .start_workflow("keys.continue", &queue, &chain, json!([0]))
        .await
        .unwrap();
    let first_run = first.run_id.as_deref().unwrap();
    let signal = client
        .signal_workflow_with_request_id(&chain, "touch", json!([1]), "continue-key")
        .await
        .unwrap();
    assert!(signal["command_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert_eq!(signal["run_id"], first_run);
    client
        .signal_workflow_with_request_id(&chain, "finish", json!([]), "continue-now")
        .await
        .unwrap();
    let second_run = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let current = client.describe_workflow(&chain).await.unwrap();
            if current.run_id.as_deref() != Some(first_run) {
                break current.run_id.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    // Worker bookkeeping can advance while a run waits. Compare the durable
    // command events to prove a retry admits no signal/update to either run.
    let old_commands = commands(
        &client
            .export_workflow_history(&chain, first_run)
            .await
            .unwrap(),
    );
    let new_commands = commands(
        &client
            .export_workflow_history(&chain, &second_run)
            .await
            .unwrap(),
    );
    assert_eq!(old_commands.len(), 2);
    assert!(new_commands.is_empty());
    let retained = client
        .signal_workflow_with_request_id(&chain, "touch", json!([1]), "continue-key")
        .await
        .unwrap();
    assert_eq!(retained["retained"], true);
    assert_eq!(retained["command_id"], signal["command_id"]);
    assert_eq!(retained["run_id"], first_run);
    let error = client
        .signal_workflow_run_with_request_id(
            &chain,
            first_run,
            "touch",
            json!([1]),
            "new-historical-key",
        )
        .await
        .unwrap_err();
    let Error::Http { body, .. } = error else {
        panic!("expected historical run refusal")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["reason"],
        "historical_run_command_rejected"
    );
    assert_eq!(
        commands(
            &client
                .export_workflow_history(&chain, first_run)
                .await
                .unwrap()
        ),
        old_commands
    );
    assert_eq!(
        commands(
            &client
                .export_workflow_history(&chain, &second_run)
                .await
                .unwrap()
        ),
        new_commands
    );

    let namespace = format!("keys-{}", durable_workflow::Uuid::new_v4());
    let response = client.control_plane_request(RawRequest::post(["namespaces"])
        .json(json!({"name":namespace,"description":"synthetic command-key fixture","retention_days":30}))).await.unwrap();
    assert_eq!(response.status(), 201);
    let foreign = self::client(&namespace);
    let error = foreign
        .signal_workflow_with_request_id(&chain, "touch", json!([1]), "continue-key")
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Http { status, .. } if status.as_u16() == 404));
    fixture(foreign.clone(), &queue).register().await.unwrap();
    // Workflow IDs are globally reserved, so the local namespace uses its own
    // workflow and reuses only the caller key.
    let other = format!("{queue}-other");
    let other_workflow = foreign
        .start_workflow("keys.wait", &queue, &other, json!([0]))
        .await
        .unwrap();
    let local = foreign
        .signal_workflow_with_request_id(&other, "touch", json!([1]), "continue-key")
        .await
        .unwrap();
    assert!(local["command_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert_eq!(local["run_id"], other_workflow.run_id.as_deref().unwrap());
    assert_ne!(local["command_id"], signal["command_id"]);
    let history = foreign
        .export_workflow_history(&other, other_workflow.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(count(&history, "SignalReceived"), 1);
    assert_eq!(
        commands(
            &client
                .export_workflow_history(&chain, &second_run)
                .await
                .unwrap()
        ),
        new_commands
    );

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(15), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires an isolated published Server advertising command_request_keys"]
async fn unicode_255_character_signal_key_is_accepted() {
    let client = client("default");
    assert_eq!(
        client.cluster_info().await.unwrap()["capabilities"]["command_request_keys"],
        true
    );
    let queue = format!("rust-unicode-{}", durable_workflow::Uuid::new_v4());
    fixture(client.clone(), &queue).register().await.unwrap();
    let workflow = client
        .start_workflow("keys.wait", &queue, &queue, json!([0]))
        .await
        .unwrap();
    let key = "λ".repeat(255);
    let original = client
        .signal_workflow_with_request_id(&queue, "touch", json!([1]), &key)
        .await
        .expect("255 Unicode characters are valid command keys");
    let mut retries = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let client = client.clone();
        let (queue, key) = (queue.clone(), key.clone());
        retries.spawn(async move {
            client
                .signal_workflow_with_request_id(&queue, "touch", json!([1]), &key)
                .await
                .unwrap()
        });
    }
    while let Some(receipt) = retries.join_next().await {
        let receipt = receipt.unwrap();
        assert_eq!(receipt["retained"], true);
        assert_eq!(receipt["command_id"], original["command_id"]);
        assert_eq!(receipt["request_id"].as_str(), Some(key.as_str()));
    }
    let error = client
        .signal_workflow_with_request_id(&queue, "touch", json!([2]), &key)
        .await
        .unwrap_err();
    assert!(error.is_request_id_conflict(), "{error:?}");
    let error = client
        .signal_workflow_with_request_id(&queue, "touch", json!([1]), &"λ".repeat(256))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Codec(_)),
        "256 characters are refused locally: {error:?}"
    );
    let history = client
        .export_workflow_history(&queue, workflow.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(count(&history, "SignalReceived"), 1);
}
