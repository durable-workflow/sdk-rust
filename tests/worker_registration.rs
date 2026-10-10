use std::{net::TcpListener, sync::Arc, time::Duration};

use durable_workflow::{
    json, AvroValue, Client, Error, RegistrationKind, Value, Worker, WorkerRetryPolicy,
    WorkflowInstance,
};

type Adapter = (&'static str, fn(&mut Worker, &str, &str));

fn workflow_adapters() -> Vec<Adapter> {
    vec![
        ("register_workflow", |worker, _, name| {
            worker.register_workflow(name, |_ctx, _input| async { Ok(json!("first")) });
        }),
        ("register_typed_workflow", |worker, _, name| {
            worker
                .register_typed_workflow(name, |_ctx, _input: Value| async { Ok(json!("typed")) });
        }),
        ("register_workflow_avro_value", |worker, _, name| {
            worker.register_workflow_avro_value(name, |_ctx, _input| async { Ok(AvroValue::Null) });
        }),
        ("register_replayed_workflow", |worker, _, name| {
            worker.register_replayed_workflow(
                name,
                || (),
                |_ctx, _input, _state: WorkflowInstance<()>| async { Ok(Value::Null) },
            );
        }),
        ("register_typed_replayed_workflow", |worker, _, name| {
            worker.register_typed_replayed_workflow(
                name,
                || (),
                |_ctx, _input: Value, _state: WorkflowInstance<()>| async { Ok(Value::Null) },
            );
        }),
        (
            "register_replayed_workflow_avro_value",
            |worker, _, name| {
                worker.register_replayed_workflow_avro_value(
                    name,
                    || (),
                    |_ctx, _input, _state: WorkflowInstance<()>| async { Ok(AvroValue::Null) },
                );
            },
        ),
    ]
}

fn activity_adapters() -> Vec<Adapter> {
    vec![
        ("register_activity", |worker, _, name| {
            worker.register_activity(name, |_ctx, _input| async { Ok(Value::Null) });
        }),
        ("register_typed_activity", |worker, _, name| {
            worker.register_typed_activity(name, |_ctx, _input: Value| async { Ok(Value::Null) });
        }),
        ("register_activity_avro_value", |worker, _, name| {
            worker.register_activity_avro_value(name, |_ctx, _input| async { Ok(AvroValue::Null) });
        }),
    ]
}

fn query_adapters() -> Vec<Adapter> {
    vec![
        ("register_query", |worker, scope, name| {
            worker.register_query(scope, name, |_ctx, _input| async { Ok(Value::Null) });
        }),
        ("register_query_avro_value", |worker, scope, name| {
            worker.register_query_avro_value(scope, name, |_ctx, _input| async {
                Ok(AvroValue::Null)
            });
        }),
        ("register_replayed_query", |worker, scope, name| {
            worker.register_replayed_query(scope, name, |_ctx, _state: Arc<()>, _input| async {
                Ok(Value::Null)
            });
        }),
        (
            "register_replayed_query_avro_value",
            |worker, scope, name| {
                worker.register_replayed_query_avro_value(
                    scope,
                    name,
                    |_ctx, _state: Arc<()>, _input| async { Ok(AvroValue::Null) },
                );
            },
        ),
    ]
}

fn update_adapters() -> Vec<Adapter> {
    vec![
        ("register_update", |worker, scope, name| {
            worker.register_update(scope, name, |_ctx, _input| async { Ok(Value::Null) });
        }),
        ("register_update_avro_value", |worker, scope, name| {
            worker.register_update_avro_value(scope, name, |_ctx, _input| async {
                Ok(AvroValue::Null)
            });
        }),
    ]
}

fn new_worker() -> Worker {
    Worker::new(
        Client::new("http://127.0.0.1:9").expect("client"),
        "registration-tests",
    )
}

#[test]
fn every_registration_adapter_rejects_same_kind_duplicates() {
    let groups = [
        (RegistrationKind::Workflow, workflow_adapters()),
        (RegistrationKind::Activity, activity_adapters()),
        (RegistrationKind::Query, query_adapters()),
        (RegistrationKind::Update, update_adapters()),
    ];
    let mut checked = 0;
    for (kind, adapters) in groups {
        for (first_name, first) in &adapters {
            for (second_name, second) in &adapters {
                let mut worker = new_worker();
                first(&mut worker, "orders", "same");
                worker.validate_registration().expect("first admission");
                second(&mut worker, "orders", "same");
                let Error::DuplicateRegistration(error) =
                    worker.validate_registration().expect_err("duplicate")
                else {
                    panic!("expected structured duplicate diagnostic");
                };
                assert_eq!(error.handler_kind, kind);
                assert_eq!(error.handler_name, "same");
                assert_eq!(
                    error.workflow_type.as_deref(),
                    match kind {
                        RegistrationKind::Query | RegistrationKind::Update => Some("orders"),
                        _ => None,
                    }
                );
                assert_eq!(error.first_definition.method, *first_name);
                assert_eq!(error.second_definition.method, *second_name);
                for definition in [&error.first_definition, &error.second_definition] {
                    assert!(definition.file.ends_with("tests/worker_registration.rs"));
                    assert!(definition.line > 0 && definition.column > 0);
                    assert!(definition.handler_type.contains("worker_registration"));
                }
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 65);
}

#[test]
fn distinct_names_worker_instances_and_handler_scopes_are_valid() {
    for adapters in [
        workflow_adapters(),
        activity_adapters(),
        query_adapters(),
        update_adapters(),
    ] {
        for (_, register) in adapters {
            let mut worker = new_worker();
            register(&mut worker, "orders", "first");
            register(&mut worker, "orders", "second");
            worker.validate_registration().expect("distinct names");
            let mut other_worker = new_worker();
            register(&mut other_worker, "orders", "first");
            other_worker
                .validate_registration()
                .expect("distinct Worker scope");
        }
    }
    let mut worker = new_worker();
    workflow_adapters()[0].1(&mut worker, "", "shared");
    activity_adapters()[0].1(&mut worker, "", "shared");
    query_adapters()[0].1(&mut worker, "shared", "shared");
    update_adapters()[0].1(&mut worker, "shared", "shared");
    query_adapters()[0].1(&mut worker, "other-workflow", "shared");
    update_adapters()[0].1(&mut worker, "other-workflow", "shared");
    worker
        .declare_workflow_signals("shared", &["finish", "finish"])
        .expect("signal set");
    worker
        .declare_workflow_signals("shared", &["changed"])
        .expect("explicit declaration replacement");
    worker
        .validate_registration()
        .expect("kind and workflow scopes");
}

#[test]
fn first_conflict_is_stable_and_clones_preserve_admission_state() {
    let mut worker = new_worker();
    let register = workflow_adapters()[0].1;
    register(&mut worker, "", "same");
    let valid_clone = worker.clone();
    register(&mut worker, "", "same");
    let first = worker
        .validate_registration()
        .expect_err("duplicate")
        .to_string();
    activity_adapters()[0].1(&mut worker, "", "other");
    activity_adapters()[1].1(&mut worker, "", "other");
    assert_eq!(
        worker
            .validate_registration()
            .expect_err("stable conflict")
            .to_string(),
        first
    );
    assert_eq!(
        worker
            .clone()
            .validate_registration()
            .expect_err("invalid clone")
            .to_string(),
        first
    );
    valid_clone
        .validate_registration()
        .expect("independent valid clone");
}

#[tokio::test]
async fn every_worker_network_entry_point_rejects_without_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let client = Client::builder(format!(
        "http://{}",
        listener.local_addr().expect("address")
    ))
    .timeout(Duration::from_millis(100))
    .build()
    .expect("client");
    let mut worker = Worker::new(client, "registration-tests").retry_policy(WorkerRetryPolicy {
        max_retries: 0,
        ..WorkerRetryPolicy::default()
    });
    workflow_adapters()[0].1(&mut worker, "", "same");
    workflow_adapters()[1].1(&mut worker, "", "same");
    for entry in 0..4 {
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            match entry {
                0 => worker.register().await.map(|_| ()),
                1 => worker.run_once().await.map(|_| ()),
                2 => worker.run().await,
                _ => worker.run_until(std::future::pending::<()>()).await,
            }
        })
        .await
        .expect("local rejection is immediate");
        assert!(
            matches!(result, Err(Error::DuplicateRegistration(_))),
            "{result:?}"
        );
        assert_eq!(
            listener.accept().expect_err("no Server connection").kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
