mod failure_reporting_support;
use durable_workflow::{json, Client};
use failure_reporting_support::{Server, MIB};
#[tokio::test]
async fn small_terminal_failure_preserves_outer_fallback_and_all_supplied_fields() {
    let server = Server::start(2 * MIB, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    let commands = vec![
        json!({"type":"fail_workflow","message":"outer fallback","exception_type":"Original","non_retryable":true,"exception":{"type":"Original","message":""}}),
        json!({"type":"fail_workflow","message":"outer fallback","exception_type":"Original","exception":{"type":"Original","message":" \t\u{0000}"}}),
        json!({"type":"fail_workflow","message":"outer supplied","exception":{"type":"Original","message":"nested authoritative"}}),
        json!({"type":"fail_workflow","message":"outer fallback","exception":{"type":"Original"}}),
    ];
    for command in &commands {
        client
            .complete_workflow_task("task", "owner", 4, vec![command.clone()])
            .await
            .unwrap();
    }
    let reports = server.reports();
    assert_eq!(reports.len(), commands.len());
    for ((_, _, body), command) in reports.into_iter().zip(commands) {
        assert_eq!(body["commands"][0],command,"small valid reports keep their original wire values; published Server chooses nonblank nested message or the outer fallback");
    }
}

#[tokio::test]
async fn redundant_outer_message_reduces_only_when_the_lower_encoded_budget_requires_it() {
    let server = Server::start(4096, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    client.cluster_info().await.unwrap();
    client.complete_workflow_task("exact-task","exact-owner",4,vec![json!({"type":"fail_workflow","message":"\u{0000}".repeat(2000),
        "exception_type":"Original","non_retryable":true,"exception":{"type":"Original","message":"nested authoritative"}})]).await.unwrap();
    let reports = server.reports();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].1.len() <= 4096);
    let body = &reports[0].2;
    assert_eq!(body["lease_owner"], "exact-owner");
    assert_eq!(body["workflow_task_attempt"], 4);
    assert_eq!(body["commands"][0]["message"], "nested authoritative");
    assert_eq!(body["commands"][0]["non_retryable"], true);
}
