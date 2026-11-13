//! Copy this test into a fresh registry-only consumer for published acceptance.
use durable_workflow::{
    json, Client, Error, ParallelOperation, ParallelResult, Value, Worker, WorkflowResultOptions,
};
use std::time::Duration;

fn events<'a>(history: &'a Value, kind: &'a str) -> impl Iterator<Item = &'a Value> {
    history["history_events"]
        .as_array()
        .expect("exported history")
        .iter()
        .filter(move |event| event["type"] == kind)
}

#[tokio::test]
#[ignore = "requires an isolated published Server with negotiated completion limits"]
async fn published_worker_completes_inline_fanout_and_fails_an_oversized_batch() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    assert!(origin
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0));
    let client = Client::builder(origin)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let info = client.cluster_info().await.unwrap();
    let vectors: Value = serde_json::from_str(include_str!(
        "fixtures/workflow-task-completion-limits-v1-vectors.json"
    ))
    .unwrap();
    assert_eq!(
        info["limits"]["max_payload_bytes"],
        vectors["default_limits"]["max_payload_bytes"]
    );
    assert_eq!(
        info["limits"]["max_workflow_task_completion_bytes"],
        vectors["default_limits"]["max_workflow_task_completion_bytes"],
        "qualification requires the explicit published producer, never a version/multiplier guess"
    );
    assert!(
        matches!(
            info["namespace"]["external_payload_storage"]["status"].as_str(),
            Some("unconfigured" | "disabled" | "unsupported")
        ),
        "fixture must have no payload store"
    );

    let queue = format!("completion-limits-{}", durable_workflow::Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue).max_concurrent_activity_tasks(16);
    worker.register_activity("limits.bytes", |_, args| async move {
        Ok(json!(args[0].as_str().unwrap().len()))
    });
    worker.register_workflow("limits.fanout", |ctx, args| async move {
        let count = args[0].as_u64().unwrap() as usize;
        let size = args[1].as_u64().unwrap() as usize;
        let payload = "x".repeat(size);
        let operations = (0..count)
            .map(|_| ParallelOperation::activity("limits.bytes", json!([payload])))
            .collect();
        let results = ctx.join(operations).await?;
        assert_eq!(results.len(), count);
        assert!(results.iter().all(
            |value| matches!(value, ParallelResult::Activity(value) if *value == json!(size))
        ));
        Ok(json!({"count":count,"bytes_each":size}))
    });
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
        timeout: Duration::from_secs(90),
        ..Default::default()
    };
    let normal = client
        .start_workflow(
            "limits.fanout",
            &queue,
            &format!("{queue}-normal"),
            json!([300, 10_000]),
        )
        .await
        .unwrap();
    let result = tokio::select! {
        result = normal.result(options.clone()) => result.unwrap(),
        exit = &mut running => panic!("worker stopped during normal fanout: {exit:?}"),
    };
    assert_eq!(result, json!({"count":300,"bytes_each":10_000}));
    let history = client
        .export_workflow_history(&normal.workflow_id, normal.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(events(&history, "ActivityCompleted").count(), 300);
    assert_eq!(events(&history, "WorkflowCompleted").count(), 1);

    // 60 individually valid arguments exceed the default aggregate allowance
    // after Avro/base64 and JSON encoding, without exceeding pending-work caps.
    let oversized = client
        .start_workflow(
            "limits.fanout",
            &queue,
            &format!("{queue}-oversized"),
            json!([60, 450_000]),
        )
        .await
        .unwrap();
    let error = tokio::select! {
        result = oversized.result(options.clone()) => result.unwrap_err(),
        exit = &mut running => panic!("worker stopped instead of recording structural failure: {exit:?}"),
    };
    let Error::WorkflowFailed(outcome) = error else {
        panic!("{error:?}");
    };
    assert_eq!(
        outcome.failure_category.as_deref(),
        Some("structural_limit")
    );
    assert_eq!(outcome.non_retryable, Some(true));
    assert!(outcome
        .message
        .as_deref()
        .unwrap()
        .starts_with("Structural limit exceeded: workflow_task_completion_bytes:"));
    let history = client
        .export_workflow_history(&oversized.workflow_id, oversized.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(events(&history, "WorkflowFailed").count(), 1);
    assert_eq!(
        events(&history, "ActivityScheduled").count(),
        0,
        "the rejected batch must not partially commit"
    );
    assert_eq!(events(&history, "ActivityCompleted").count(), 0);

    let followup = client
        .start_workflow(
            "limits.fanout",
            &queue,
            &format!("{queue}-followup"),
            json!([1, 10]),
        )
        .await
        .unwrap();
    let result = tokio::select! {
        result = followup.result(options) => result.unwrap(),
        exit = &mut running => panic!("worker did not remain usable: {exit:?}"),
    };
    assert_eq!(result, json!({"count":1,"bytes_each":10}));
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    println!("PASS: 300x10KB inline fanout; typed aggregate structural failure with no partial activities; same-worker followup");
}
