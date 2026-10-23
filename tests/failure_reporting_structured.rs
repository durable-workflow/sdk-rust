mod failure_reporting_support;
use durable_workflow::{encode_avro_value, json, AvroValue, Client, Error, FailureReportError};
use failure_reporting_support::{Server, MIB};

#[tokio::test]
async fn structured_reports_bound_stack_combined_details_and_final_escaping() {
    let server = Server::start(2 * MIB, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    let details = serde_json::to_value(
        encode_avro_value(&AvroValue::String(
            "external-detail-original".repeat(200_000),
        ))
        .unwrap(),
    )
    .unwrap();
    let failure = json!({"type":"Original","message":"\u{0000}".repeat(3*MIB),"stack_trace":"\u{0001}".repeat(3*MIB),
        "retryable":false,"non_retryable":true,"details":details,"diagnostics":{"causes":[{"nested":"界".repeat(MIB)}]},
        "runtime_diagnostics":{"custom":"x".repeat(MIB)}});
    client
        .fail_activity_task_with_failure("task", "attempt", "owner", failure.clone())
        .await
        .unwrap();
    client
        .fail_workflow_task_with_failure("task", "owner", 8, failure)
        .await
        .unwrap();
    let reports = server.reports();
    assert_eq!(reports.len(), 2);
    for (path, bytes, body) in reports {
        assert!(
            bytes.len() <= MIB,
            "actual body including markers/escaping {} bytes",
            bytes.len()
        );
        let failure = &body["failure"];
        assert_eq!(failure["type"], "Original");
        assert_eq!(failure["non_retryable"], true);
        assert_eq!(failure["retryable"], false);
        assert!(
            failure.get("details").is_none(),
            "large Avro details omitted atomically"
        );
        if path.contains("/workflow-tasks/") {
            assert!(failure["message"]
                .as_str()
                .unwrap()
                .contains("original_stack_trace_bytes"));
            assert!(failure["message"].as_str().unwrap().contains("details"));
            assert!(
                failure.get("diagnostics").is_none(),
                "no unsupported diagnostic wire field added"
            );
        } else {
            let marker = &failure["diagnostics"]["sdk_failure_truncation"];
            assert_eq!(marker["original_stack_trace_bytes"], 3 * MIB);
            assert!(marker["omitted_fields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "details"));
        }
    }
}

#[tokio::test]
async fn newly_discovered_lower_limit_applies_before_payload_externalization() {
    let server = Server::start(4096, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    let details = serde_json::to_value(
        encode_avro_value(&AvroValue::String("valid original details".into())).unwrap(),
    )
    .unwrap();
    client
        .fail_activity_task_with_failure(
            "task",
            "exact-attempt",
            "exact-owner",
            json!({"type":"Original",
        "message":"\u{0000}".repeat(3*MIB),"details":details}),
        )
        .await
        .unwrap();
    client
        .fail_activity_task_with_error(
            "task",
            "exact-attempt",
            "exact-owner",
            &durable_workflow::ActivityError::new("Original", "\\\n".repeat(3 * MIB)),
        )
        .await
        .unwrap();
    let reports = server.reports();
    assert_eq!(reports.len(), 2);
    for (_, bytes, body) in reports {
        assert!(
            bytes.len() <= 4096,
            "actual post-marker body {} bytes",
            bytes.len()
        );
        assert_eq!(body["activity_attempt_id"], "exact-attempt");
        assert_eq!(body["lease_owner"], "exact-owner");
        assert!(body["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("[truncated"));
    }
    let result = client
        .fail_activity_task(
            "task",
            "exact-attempt",
            &"owner".repeat(4096),
            "small",
            false,
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::FailureReporting(
            FailureReportError::RequiredFieldsTooLarge { limit: 4096, .. }
        ))
    ));
    assert_eq!(
        server.reports().len(),
        2,
        "unfit identity is not sent or rewritten"
    );
}

#[tokio::test]
async fn small_structured_report_and_valid_details_are_unchanged() {
    let server = Server::start(2 * MIB, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    let details =
        serde_json::to_value(encode_avro_value(&AvroValue::String("original".into())).unwrap())
            .unwrap();
    let failure = json!({"type":"Original","message":"small","non_retryable":false,"details":details,"diagnostics":{"cause":"retained"}});
    client
        .fail_activity_task_with_failure("task", "attempt", "owner", failure.clone())
        .await
        .unwrap();
    assert_eq!(server.reports()[0].2["failure"], failure);
}

#[tokio::test]
async fn query_and_update_failures_cannot_bypass_complete_report_cap() {
    let server = Server::start(2 * MIB, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .fail_query_task(
            "exact-query",
            "owner",
            7,
            "\u{0000}".repeat(3 * MIB),
            "original_reason",
            "OriginalQueryType",
        )
        .await
        .unwrap();
    client.complete_workflow_task("task","owner",3,vec![json!({"type":"fail_update","update_id":"exact-update","message":"界\\\n".repeat(MIB),"exception_type":"OriginalUpdateType","non_retryable":true})]).await.unwrap();
    let reports = server.reports();
    assert_eq!(reports.len(), 2);
    assert!(reports.iter().all(|(_, bytes, _)| bytes.len() <= MIB));
    assert_eq!(reports[0].2["query_task_attempt"], 7);
    assert_eq!(reports[0].2["failure"]["reason"], "original_reason");
    assert_eq!(reports[0].2["failure"]["type"], "OriginalQueryType");
    assert_eq!(reports[1].2["commands"][0]["update_id"], "exact-update");
    assert_eq!(reports[1].2["commands"][0]["non_retryable"], true);
}
