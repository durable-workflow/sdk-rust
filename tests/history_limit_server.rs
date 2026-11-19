//! durable-workflow/.github#228: the managed Worker against a published native
//! Server whose history-transaction budget is lowered
//! (`DW_V2_LIMIT_HISTORY_TRANSACTION_BYTES`). A fan-out that fits the
//! completion size limit is refused with 409 `history_transaction_limit_exceeded`
//! and must fail its run once with a typed structural-limit failure, with no
//! partially scheduled activities; the same Worker then completes other work.
//!
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
        .filter(move |event| event["type"] == kind || event["event_type"] == kind)
}

#[tokio::test]
#[ignore = "requires an isolated published native Server with a lowered history-transaction budget"]
async fn published_worker_fails_an_over_budget_fanout_once_and_keeps_working() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    assert!(origin
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0));
    let budget: u64 = std::env::var("DURABLE_WORKFLOW_HISTORY_TRANSACTION_BYTES")
        .expect("the lowered history budget the Server was started with")
        .parse()
        .unwrap();
    let client = Client::builder(origin)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let info = client.cluster_info().await.unwrap();
    assert_eq!(
        info["version"].as_str(),
        Some(
            std::env::var("DURABLE_WORKFLOW_EXPECT_SERVER_VERSION")
                .unwrap()
                .as_str()
        )
    );
    assert_eq!(info["implementation"], "rust");
    let completion_cap = info["limits"]["max_workflow_task_completion_bytes"]
        .as_u64()
        .expect("native Server advertises its completion cap");
    assert!(
        completion_cap > 4 * budget,
        "the fan-out must fit the completion size limit"
    );

    let queue = format!("history-limit-{}", durable_workflow::Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue)
        .poll_timeout(Duration::from_millis(10))
        .max_concurrent_activity_tasks(4);
    worker.register_activity("history.length", |_, args| async move {
        Ok(json!(args[0].as_str().unwrap().len()))
    });
    worker.register_workflow("history.fanout", |ctx, args| async move {
        let count = args[0].as_u64().unwrap() as usize;
        let size = args[1].as_u64().unwrap() as usize;
        let payload = "x".repeat(size);
        let operations = (0..count)
            .map(|_| ParallelOperation::activity("history.length", json!([payload])))
            .collect();
        let results = ctx.join(operations).await?;
        assert!(results.iter().all(
            |value| matches!(value, ParallelResult::Activity(value) if *value == json!(size))
        ));
        Ok(json!([results.len(), results.len() * size]))
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
        timeout: Duration::from_secs(60),
        ..Default::default()
    };

    // 40 activities with 4 KB arguments: about 160 KB of ActivityScheduled
    // history in one completion, far under the completion cap but over the
    // lowered history budget.
    let over = client
        .start_workflow(
            "history.fanout",
            &queue,
            &format!("{queue}-over"),
            json!([40, 4096]),
        )
        .await
        .unwrap();
    let error = tokio::select! {
        result = over.result(options) => result.unwrap_err(),
        exit = &mut running => panic!("worker stopped instead of recording a structural failure: {exit:?}"),
    };
    let Error::WorkflowFailed(outcome) = error else {
        panic!("{error:?}");
    };
    assert_eq!(
        outcome.failure_category.as_deref(),
        Some("structural_limit")
    );
    assert_eq!(outcome.non_retryable, Some(true));
    let message = outcome.message.as_deref().unwrap();
    assert!(
        message.starts_with("Structural limit exceeded: history_transaction_bytes: "),
        "{message}"
    );
    assert!(
        message.contains(&format!("over the server limit of {budget} history bytes")),
        "{message}"
    );
    let history = client
        .export_workflow_history(&over.workflow_id, over.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(events(&history, "WorkflowFailed").count(), 1);
    assert_eq!(
        events(&history, "ActivityScheduled").count(),
        0,
        "the refused completion must not partially commit"
    );
    assert_eq!(events(&history, "WorkflowTaskFailed").count(), 0);
    let failed = events(&history, "WorkflowFailed").next().unwrap();
    assert_eq!(failed["payload"]["failure_category"], "structural_limit");
    assert_eq!(
        failed["payload"]["exception_type"],
        "StructuralLimitExceeded"
    );

    let followup = client
        .start_workflow(
            "history.fanout",
            &queue,
            &format!("{queue}-followup"),
            json!([3, 10]),
        )
        .await
        .unwrap();
    let result = tokio::select! {
        result = followup.result(options) => result.unwrap(),
        exit = &mut running => panic!("worker did not remain usable: {exit:?}"),
    };
    assert_eq!(result, json!([3, 30]));
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    println!("PASS: over-budget fan-out failed once with {message:?}; no partial activities; same-worker follow-up [3, 30]");
}
