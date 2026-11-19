//! durable-workflow/.github#223 / Server #1134: workflow code signals another
//! workflow through the `signal_external_workflow` command against an
//! isolated native Server that advertises it.
//!
//! Covers a delivered signal (the target resumes with the arguments), a
//! missing target, a closed target, a signal the target does not declare, and
//! a cold replay of each sender's exported history that submits nothing.
//!
//! Run with `DURABLE_WORKFLOW_LOCAL_ISOLATED=1`,
//! `DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:<port>` and
//! `DURABLE_WORKFLOW_AUTH_TOKEN`, then
//! `cargo test --test signal_external_workflow_server -- --ignored`.
use durable_workflow::{
    json, testing::WorkflowEnvironment, testing::WorkflowHistory, Client, Value, Worker,
    WorkflowResultOptions,
};
use std::time::Duration;

fn events<'a>(history: &'a Value, kind: &'a str) -> Vec<&'a Value> {
    history["history_events"]
        .as_array()
        .expect("exported history")
        .iter()
        .filter(|event| event["type"] == kind || event["event_type"] == kind)
        .collect()
}

fn register(worker: &mut Worker) {
    worker.register_workflow("g223.target", |ctx, _input| async move {
        let args = ctx.wait_signal("approve").await?;
        Ok(json!({"approved_by": args}))
    });
    worker
        .declare_workflow_signals("g223.target", &["approve"])
        .unwrap();
    // Two signals in separate turns, then later durable commands: each takes
    // the next authored sequence (Server #1134 counts the request).
    worker.register_workflow("g223.sequence", |ctx, input| async move {
        let first = ctx
            .signal_external_workflow(input[0].as_str().unwrap(), "approve", json!(["first"]))
            .await?;
        let second = ctx
            .signal_external_workflow(input[1].as_str().unwrap(), "approve", json!(["second"]))
            .await?;
        let marker = ctx.side_effect(|| "after-signals".to_string())?;
        ctx.start_timer(Duration::from_secs(1)).await?;
        Ok(json!([
            first.outcome.as_str(),
            second.outcome.as_str(),
            marker
        ]))
    });
    worker.register_workflow("g223.done", |_ctx, _input| async move { Ok(json!("done")) });
    worker.register_workflow("g223.sender", |ctx, input| async move {
        let target = input[0].as_str().unwrap().to_string();
        let signal = input[1].as_str().unwrap().to_string();
        let mut call = ctx.signal_external_workflow(target, signal, json!(["manager", 7]));
        if let Some(run_id) = input[2].as_str() {
            call = call.run_id(run_id);
        }
        let sent = call.await?;
        Ok(json!({
            "outcome": sent.outcome.as_str(),
            "reason": sent.reason,
            "target_workflow_id": sent.target_workflow_id,
            "target_run_id": sent.target_run_id,
            "signal_name": sent.signal_name,
            "request_id": sent.request_id,
            "target_command_id": sent.target_command_id,
            "target_signal_id": sent.target_signal_id,
            "retained": sent.retained,
        }))
    });
}

#[tokio::test]
#[ignore = "requires an isolated native Server that advertises signal_external_workflow"]
async fn workflow_code_signals_other_workflows_durably() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    let client = Client::builder(origin)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let info = client.cluster_info().await.unwrap();
    assert_eq!(info["implementation"], "rust", "{info}");
    assert_eq!(
        info["capabilities"]["signal_external_workflow"], true,
        "{info}"
    );

    let queue = format!("g223-{}", durable_workflow::Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue).poll_timeout(Duration::from_millis(10));
    register(&mut worker);
    worker.register().await.unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut running = tokio::spawn(async move {
        worker
            .run_until(async {
                let _ = stopped.await;
            })
            .await
    });
    let options = WorkflowResultOptions {
        timeout: Duration::from_secs(60),
        ..Default::default()
    };
    macro_rules! result {
        ($handle:expr) => {
            tokio::select! {
                result = $handle.result(options.clone()) => result.unwrap(),
                exit = &mut running => panic!("worker stopped: {exit:?}"),
            }
        };
    }
    let sender = |id: String, args: Value| {
        let client = client.clone();
        let queue = queue.clone();
        async move {
            client
                .start_workflow("g223.sender", &queue, &id, args)
                .await
                .unwrap()
        }
    };

    // Delivered: the target resumes with the sender's arguments.
    let target_id = format!("{queue}-target");
    let target = client
        .start_workflow("g223.target", &queue, &target_id, json!([]))
        .await
        .unwrap();
    let delivered = sender(
        format!("{queue}-sender-delivered"),
        json!([target_id, "approve", null]),
    )
    .await;
    let sent = result!(delivered);
    assert_eq!(sent["outcome"], "delivered", "{sent}");
    assert_eq!(sent["reason"], Value::Null);
    assert_eq!(sent["target_workflow_id"], target_id.as_str());
    assert_eq!(sent["target_run_id"], target.run_id.as_deref().unwrap());
    assert_eq!(sent["retained"], false);
    assert!(sent["target_command_id"].is_string(), "{sent}");
    assert!(sent["target_signal_id"].is_string(), "{sent}");
    let sender_run = delivered.run_id.as_deref().unwrap();
    assert_eq!(
        sent["request_id"],
        format!("workflow-signal:{sender_run}:1").as_str()
    );
    assert_eq!(
        result!(target),
        json!({"approved_by": ["manager", 7]}),
        "the target received the signal arguments"
    );

    // Missing target.
    let missing = sender(
        format!("{queue}-sender-missing"),
        json!([format!("{queue}-nobody"), "approve", null]),
    )
    .await;
    let sent = result!(missing);
    assert_eq!(sent["outcome"], "target_not_found", "{sent}");
    assert_eq!(sent["reason"], "instance_not_found", "{sent}");
    assert_eq!(sent["target_run_id"], Value::Null);

    // Closed target.
    let closed_id = format!("{queue}-closed");
    let closed = client
        .start_workflow("g223.done", &queue, &closed_id, json!([]))
        .await
        .unwrap();
    assert_eq!(result!(closed), json!("done"));
    let to_closed = sender(
        format!("{queue}-sender-closed"),
        json!([closed_id, "approve", null]),
    )
    .await;
    let sent = result!(to_closed);
    assert_eq!(sent["outcome"], "target_closed", "{sent}");
    assert_eq!(sent["reason"], "run_not_active", "{sent}");
    assert_eq!(sent["target_run_id"], closed.run_id.as_deref().unwrap());

    // A signal the target does not declare.
    let open_id = format!("{queue}-open");
    let open = client
        .start_workflow("g223.target", &queue, &open_id, json!([]))
        .await
        .unwrap();
    let rejected = sender(
        format!("{queue}-sender-rejected"),
        json!([open_id, "unknown", null]),
    )
    .await;
    let sent = result!(rejected);
    assert_eq!(sent["outcome"], "rejected", "{sent}");
    assert_eq!(sent["reason"], "unknown_signal", "{sent}");
    let release = sender(
        format!("{queue}-sender-release"),
        json!([open_id, "approve", open.run_id.as_deref().unwrap()]),
    )
    .await;
    assert_eq!(result!(release)["outcome"], "delivered");
    assert_eq!(result!(open), json!({"approved_by": ["manager", 7]}));

    // Two signals in a row, then a side effect and a timer.
    let first_id = format!("{queue}-seq-a");
    let second_id = format!("{queue}-seq-b");
    let first = client
        .start_workflow("g223.target", &queue, &first_id, json!([]))
        .await
        .unwrap();
    let second = client
        .start_workflow("g223.target", &queue, &second_id, json!([]))
        .await
        .unwrap();
    let sequence = client
        .start_workflow(
            "g223.sequence",
            &queue,
            &format!("{queue}-sequence"),
            json!([first_id, second_id]),
        )
        .await
        .unwrap();
    assert_eq!(
        result!(sequence),
        json!(["delivered", "delivered", "after-signals"])
    );
    assert_eq!(result!(first), json!({"approved_by": ["first"]}));
    assert_eq!(result!(second), json!({"approved_by": ["second"]}));

    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Each sender recorded one request and one resolution, and a cold replay
    // of its exported history resolves the call without submitting anything.
    let mut offline = Worker::new(client.clone(), &queue);
    register(&mut offline);
    let env = WorkflowEnvironment::from_worker(offline);
    for (handle, resolution) in [
        (&delivered, "ExternalSignalDelivered"),
        (&missing, "ExternalSignalFailed"),
        (&to_closed, "ExternalSignalFailed"),
        (&rejected, "ExternalSignalFailed"),
        (&release, "ExternalSignalDelivered"),
        (&sequence, "ExternalSignalDelivered"),
    ] {
        let export = client
            .export_workflow_history(&handle.workflow_id, handle.run_id.as_deref().unwrap())
            .await
            .unwrap();
        let signals = if handle.workflow_id == sequence.workflow_id {
            2
        } else {
            1
        };
        assert_eq!(events(&export, "ExternalSignalRequested").len(), signals);
        assert_eq!(events(&export, resolution).len(), signals, "{export}");
        assert_eq!(events(&export, "WorkflowTaskFailed").len(), 0, "{export}");
        let report = env
            .replay(&WorkflowHistory::from_export(export).unwrap())
            .unwrap();
        assert!(report.completed);
        assert_eq!(report.commands.len(), 1, "{:?}", report.commands);
        assert_eq!(report.commands[0]["type"], "complete_workflow");
    }

    // Every authored command took its own sequence.
    let export = client
        .export_workflow_history(&sequence.workflow_id, sequence.run_id.as_deref().unwrap())
        .await
        .unwrap();
    let sequence_of = |kind: &str| -> Vec<u64> {
        events(&export, kind)
            .iter()
            .map(|event| event["payload"]["sequence"].as_u64().unwrap())
            .collect()
    };
    assert_eq!(sequence_of("ExternalSignalRequested"), [1, 2], "{export}");
    assert_eq!(sequence_of("ExternalSignalDelivered"), [1, 2], "{export}");
    assert_eq!(sequence_of("SideEffectRecorded"), [3], "{export}");
    assert_eq!(sequence_of("TimerScheduled"), [4], "{export}");
    assert_eq!(sequence_of("TimerFired"), [4], "{export}");

    // The delivered target saw exactly one signal from the sender.
    let target_history = client
        .export_workflow_history(&target_id, target.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(events(&target_history, "SignalReceived").len(), 1);
    println!("PASS: delivered, target_not_found, target_closed and rejected external signals; two signals then a side effect and timer at sequences 1-4; cold replay submitted nothing");
}
