//! Compile the Rust 4 literal migration as a downstream crate, without private helpers.

use durable_workflow::{ActivityTask, QueryTask, WorkflowTask};

#[test]
fn tasks_remain_constructible_with_explicit_absent_optional_metadata() {
    let workflow = WorkflowTask {
        task_id: "workflow-task".into(),
        observability_context: None,
        dispatch_observability_context: None,
        observability_links: None,
        workflow_command_id: None,
        workflow_id: None,
        run_id: None,
        workflow_type: "example".into(),
        cancel_requested: false,
        payload_codec: "avro".into(),
        arguments: None,
        history_events: vec![],
        total_history_events: None,
        history_size_bytes: None,
        continue_as_new_recommended: None,
        history_budget_pressure: None,
        next_history_page_token: None,
        workflow_task_attempt: 1,
        workflow_signal_id: None,
        signal_name: None,
        signal_arguments: None,
        workflow_update_id: None,
        update_name: None,
        lease_owner: None,
    };
    let activity = ActivityTask {
        task_id: "activity-task".into(),
        workflow_id: None,
        run_id: None,
        activity_execution_id: None,
        observability_context: None,
        dispatch_observability_context: None,
        observability_links: None,
        activity_attempt_id: None,
        attempt_id: None,
        activity_type: "example".into(),
        payload_codec: "avro".into(),
        arguments: None,
        attempt_number: 1,
        lease_owner: None,
    };
    let query = QueryTask {
        query_task_id: "query-task".into(),
        observability_context: None,
        dispatch_observability_context: None,
        observability_links: None,
        query_task_attempt: 1,
        lease_owner: None,
        workflow_id: None,
        run_id: None,
        workflow_type: "example".into(),
        query_name: "example".into(),
        payload_codec: "avro".into(),
        workflow_arguments: None,
        query_arguments: None,
        history_events: vec![],
        history_export: None,
        run_status: None,
    };

    assert_eq!(workflow.payload_codec, activity.payload_codec);
    assert_eq!(activity.payload_codec, query.payload_codec);
}
