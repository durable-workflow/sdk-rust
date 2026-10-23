//! Real paged list/history acceptance against an isolated published Server.
use durable_workflow::{
    json,
    testing::{WorkflowEnvironment, WorkflowHistory},
    Client, Error, SearchAttributeUpdate, Worker, WorkflowHistoryOptions, WorkflowListOptions,
    WorkflowResultOptions,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_routes_filter_lists_and_return_complete_multi_page_history() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    let token = std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap();
    let response = reqwest::Client::new()
        .post(format!("{url}/api/search-attributes"))
        .bearer_auth(&token)
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .json(&json!({"name":"OrderRef","type":"keyword"}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "synthetic attribute registration: {status} {body}"
    );
    let client = Client::builder(url)
        .token(Some(token))
        .namespace("default")
        .build()
        .unwrap();
    let queue = format!("rust-pagination-{}", durable_workflow::Uuid::new_v4());
    let customer = format!("customer-{}", durable_workflow::Uuid::new_v4());
    let entered = Arc::new(AtomicUsize::new(0));
    let (release, hold) = tokio::sync::watch::channel(false);
    let mut worker = Worker::new(client.clone(), &queue).poll_timeout(Duration::from_millis(10));
    worker.register_activity("pagination.echo", {
        let entered = entered.clone();
        move |_, args| {
            let entered = entered.clone();
            let mut hold = hold.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                hold.wait_for(|ready| *ready).await.unwrap();
                Ok(args[0].clone())
            }
        }
    });
    for (name, attribute) in [
        ("tests.pagination", Some(customer.clone())),
        ("tests.pagination-other", None),
    ] {
        worker.register_workflow(name, move |ctx, args| {
            let attribute = attribute.clone();
            async move {
                if let Some(attribute) = attribute {
                    ctx.upsert_search_attributes(
                        SearchAttributeUpdate::new().keyword("OrderRef", attribute)?,
                    )?;
                }
                let result = ctx.activity("pagination.echo", args).await?;
                ctx.sleep(Duration::from_secs(1)).await?;
                ctx.activity("pagination.echo", json!([result])).await
            }
        });
    }
    worker.register().await.unwrap();
    let input = "x".repeat(128 * 1024);
    let mut handles = Vec::new();
    for index in 0..4 {
        handles.push(
            client
                .start_workflow(
                    if index < 3 {
                        "tests.pagination"
                    } else {
                        "tests.pagination-other"
                    },
                    &queue,
                    &format!("{queue}-{index}"),
                    json!([input]),
                )
                .await
                .unwrap(),
        );
    }
    let watchers = handles.clone();
    let driver = worker.clone();
    let running = tokio::spawn(async move {
        driver
            .run_until(async move {
                loop {
                    let mut finished = true;
                    for handle in &watchers {
                        finished &= handle.describe().await.unwrap().is_terminal();
                    }
                    if finished {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), async {
        while entered.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all four first activity attempts must be held");
    let filters = WorkflowListOptions::new()
        .workflow_type("tests.pagination")
        .status("running")
        .query(format!("OrderRef = '{customer}'"))
        .page_size(2);
    let first = client.list_workflows(filters.clone()).await.unwrap();
    assert_eq!(first.workflows.len(), 2);
    assert_eq!(first.workflow_count, Some(2));
    assert!(first.next_page_token.is_some());
    let mut list = client.iter_workflows(filters.page_size(1));
    let mut ids = Vec::new();
    while let Some(summary) = list.next().await.unwrap() {
        assert_eq!(summary.search_attributes["OrderRef"], customer);
        assert_eq!(summary.workflow_type, "tests.pagination");
        assert_eq!(summary.status_bucket.as_deref(), Some("running"));
        assert!(matches!(
            summary.status.as_str(),
            "pending" | "running" | "waiting"
        ));
        assert_eq!(summary.is_terminal, Some(false));
        ids.push(summary.workflow_id);
    }
    ids.sort();
    let mut expected = handles[..3]
        .iter()
        .map(|h| h.workflow_id.clone())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        ids, expected,
        "actual query must include every filtered run once across three pages"
    );
    let other = client
        .list_workflows(
            WorkflowListOptions::new()
                .workflow_type("tests.pagination-other")
                .query(format!("WorkflowId = '{}'", handles[3].workflow_id)),
        )
        .await
        .unwrap();
    assert_eq!(other.workflows.len(), 1);
    assert!(other.workflows[0].search_attributes.is_empty());
    release.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(45), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        handles[0]
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        input
    );
    let workflow = &handles[0].workflow_id;
    let run = handles[0].run_id.as_deref().unwrap();
    let first = client
        .get_workflow_history(
            workflow,
            run,
            WorkflowHistoryOptions::new()
                .page_size(2)
                .wait_new_event(true),
        )
        .await
        .unwrap();
    assert_eq!(first.events.len(), 2);
    assert!(first.next_page_token.is_some());
    let baseline = client
        .get_workflow_history(workflow, run, WorkflowHistoryOptions::new().page_size(1000))
        .await
        .unwrap();
    assert!(baseline.next_page_token.is_none());
    assert!(baseline.events.len() > 2);
    let mut history =
        client.iter_workflow_history(workflow, run, WorkflowHistoryOptions::new().page_size(2));
    let mut events = Vec::new();
    while let Some(event) = history.next().await.unwrap() {
        events.push(event);
    }
    assert_eq!(
        serde_json::to_value(&events).unwrap(),
        serde_json::to_value(&baseline.events).unwrap()
    );
    assert!(events
        .iter()
        .any(|event| event.event_type == "WorkflowCompleted"));
    assert!(
        matches!(client.get_workflow_history(workflow,run,WorkflowHistoryOptions::new().page_size(0)).await,Err(Error::Http {status,..}) if status.as_u16()==422)
    );
    let replay =
        WorkflowHistory::from_export(client.export_workflow_history(workflow, run).await.unwrap())
            .unwrap();
    assert!(
        WorkflowEnvironment::from_worker(worker)
            .replay(&replay)
            .unwrap()
            .completed
    );
    println!("published pagination passed:3filtered runs/3list pages,{}history events/{}history pages,128KiB payloads,selected-run export replay",events.len(),events.len().div_ceil(2));
}
