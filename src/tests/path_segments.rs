use super::*;

// Exercise public entry points so validation cannot accidentally happen after
// capability discovery or be skipped by a typed/selected-run variant.
const ROUTES: &[(&str, &str, bool, Option<&str>)] = &[
    ("describe", "", false, None),
    (
        "outcome",
        "/updates/{n}?wait_for=completed",
        false,
        Some("update_id"),
    ),
    (
        "outcome-avro",
        "/updates/{n}?wait_for=completed",
        false,
        Some("update_id"),
    ),
    ("outcome-receipt", "/updates/{n}", false, Some("update_id")),
    ("describe-run", "/runs/{r}", true, None),
    ("signal", "/signal/{n}", false, Some("signal_name")),
    (
        "signal-run",
        "/runs/{r}/signal/{n}",
        true,
        Some("signal_name"),
    ),
    ("query", "/query/{n}", false, Some("query_name")),
    ("query-run", "/runs/{r}/query/{n}", true, Some("query_name")),
    ("query-avro", "/query/{n}", false, Some("query_name")),
    (
        "query-run-avro",
        "/runs/{r}/query/{n}",
        true,
        Some("query_name"),
    ),
    ("update", "/update/{n}", false, Some("update_name")),
    ("update-avro", "/update/{n}", false, Some("update_name")),
    ("cancel", "/cancel", false, None),
    ("cancel-run", "/runs/{r}/cancel", true, None),
    ("terminate", "/terminate", false, None),
    ("terminate-run", "/runs/{r}/terminate", true, None),
    ("redrive", "/runs/{r}/redrive", true, None),
    (
        "message",
        "/message-streams/{n}/messages",
        false,
        Some("stream_name"),
    ),
    ("streams", "/runs/{r}/streams", true, None),
    ("stream", "/runs/{r}/streams/{n}", true, Some("stream_name")),
    (
        "subscribe",
        "/runs/{r}/streams/{n}/items?from=0&max_items=1&wait_seconds=0",
        true,
        Some("stream_name"),
    ),
    (
        "append",
        "/runs/{r}/streams/{n}/items",
        true,
        Some("stream_name"),
    ),
    (
        "close",
        "/runs/{r}/streams/{n}/close",
        true,
        Some("stream_name"),
    ),
    ("activities", "/runs/{r}/activities", true, None),
    ("history", "/runs/{r}/history", true, None),
    ("export", "/runs/{r}/history/export", true, None),
    ("cooperative", "/request-cancellation", false, None),
    (
        "cooperative-run",
        "/runs/{r}/request-cancellation",
        true,
        None,
    ),
];

async fn exercise(client: &Client, route: &str, w: &str, r: &str, n: &str) -> Result<()> {
    match route {
        "outcome" => client.update_outcome(w, n, None).await.map(|_| ()),
        "outcome-avro" => client
            .update_outcome_avro_value(w, n, None)
            .await
            .map(|_| ()),
        "outcome-receipt" => client
            .update_outcome_receipt(w, n, UpdateWaitStage::Accepted, None)
            .await
            .map(|_| ()),
        "describe" => client.describe_workflow(w).await.map(|_| ()),
        "describe-run" => client.describe_workflow_run(w, r).await.map(|_| ()),
        "signal" => client.signal_workflow(w, n, json!([])).await.map(|_| ()),
        "signal-run" => client
            .signal_workflow_run(w, r, n, json!([]))
            .await
            .map(|_| ()),
        "query" => client.query_workflow(w, n, json!([])).await.map(|_| ()),
        "query-run" => client
            .query_workflow_run(w, r, n, json!([]))
            .await
            .map(|_| ()),
        "query-avro" => client
            .query_workflow_avro_value(w, n, json!([]))
            .await
            .map(|_| ()),
        "query-run-avro" => client
            .query_workflow_run_avro_value(w, r, n, json!([]))
            .await
            .map(|_| ()),
        "update" => client
            .update_workflow(w, n, json!([]), None)
            .await
            .map(|_| ()),
        "update-avro" => client
            .update_workflow_avro_value(w, n, json!([]), None)
            .await
            .map(|_| ()),
        "cancel" => client
            .cancel_workflow(w, Default::default())
            .await
            .map(|_| ()),
        "cancel-run" => client
            .cancel_workflow_run(w, r, Default::default())
            .await
            .map(|_| ()),
        "terminate" => client
            .terminate_workflow(w, Default::default())
            .await
            .map(|_| ()),
        "terminate-run" => client
            .terminate_workflow_run(w, r, Default::default())
            .await
            .map(|_| ()),
        "redrive" => client.redrive_workflow_run(w, r, None).await.map(|_| ()),
        "message" => client
            .append_message_stream(w, n, "message-1", json!([]))
            .await
            .map(|_| ()),
        "streams" => client.list_workflow_streams(w, r).await.map(|_| ()),
        "stream" => client.describe_workflow_stream(w, r, n).await.map(|_| ()),
        "subscribe" => client
            .subscribe_workflow_stream(w, r, n, 0, 1, Duration::ZERO)
            .await
            .map(|_| ()),
        "append" => client
            .append_workflow_stream(w, r, n, &[WorkflowStreamAppendItem::new(json!(1))?], None)
            .await
            .map(|_| ()),
        "close" => client
            .close_workflow_stream(w, r, n, None, None)
            .await
            .map(|_| ()),
        "activities" => client.get_run_activities(w, r).await.map(|_| ()),
        "history" => client
            .get_workflow_history(w, r, Default::default())
            .await
            .map(|_| ()),
        "export" => client.export_workflow_history(w, r).await.map(|_| ()),
        "cooperative" => client
            .request_workflow_cancellation(w, Default::default())
            .await
            .map(|_| ()),
        "cooperative-run" => client
            .request_workflow_run_cancellation(w, r, Default::default())
            .await
            .map(|_| ()),
        _ => unreachable!("unknown route"),
    }
}

fn refuse_request(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
    if path == "/api/cluster/info" {
        return Some((
            "200 OK",
            json!({"capabilities": {"update_outcome_reads": true}}).to_string(),
        ));
    }
    path.starts_with("/api/workflows/")
        .then(|| ("400 Bad Request", "{}".into()))
}

#[tokio::test]
async fn all_routes_keep_reserved_and_unicode_identifiers_in_one_segment() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(refuse_request),
        ..Default::default()
    });
    let client = keyed_client(&server);
    let raw = "a/b?c#d%2F\\é";
    let encoded = "a%2Fb%3Fc%23d%252F%5C%C3%A9";
    for &(route, suffix, _, _) in ROUTES {
        // Cooperative encoding has its own successful discovery/ack test.
        if route.starts_with("cooperative") {
            continue;
        }
        let before = server.captured_paths().len();
        exercise(&client, route, raw, raw, raw)
            .await
            .expect_err("server refuses the encoded request");
        let expected = format!(
            "/api/workflows/{encoded}{}",
            suffix.replace("{r}", encoded).replace("{n}", encoded)
        );
        let paths = server.captured_paths();
        let requests: Vec<_> = paths[before..]
            .iter()
            .filter(|path| path.as_str() != "/api/cluster/info")
            .cloned()
            .collect();
        assert_eq!(requests, vec![expected], "{route}");
    }
}

#[tokio::test]
async fn all_routes_refuse_empty_and_dot_segments_before_any_http_request() {
    let server = MockWorkerServer::start();
    let client = keyed_client(&server);
    for &(route, _, has_run, name_field) in ROUTES {
        for invalid in ["", ".", ".."] {
            let mut cases = vec![(invalid, "run-1", "name", "workflow_id")];
            if has_run {
                cases.push(("workflow-1", invalid, "name", "run_id"));
            }
            if let Some(field) = name_field {
                cases.push(("workflow-1", "run-1", invalid, field));
            }
            for (w, r, n, field) in cases {
                let error = exercise(&client, route, w, r, n)
                    .await
                    .expect_err("invalid segment");
                assert!(
                    matches!(error, Error::InvalidPathSegment { field: actual } if actual == field),
                    "{route}, {field}={invalid:?}: {error:?}"
                );
            }
        }
    }
    assert!(
        server.captured_paths().is_empty(),
        "no discovery or mutation: {:?}",
        server.captured_paths()
    );
}
