use durable_workflow::{
    ActivityOptions, Client, Result, Worker, WorkflowContext, WorkflowInstance,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct Request {
    account: String,
}

#[derive(Deserialize, Serialize)]
struct Response {
    account: String,
}

#[derive(Clone, Default)]
struct ReplayState {
    account: Option<String>,
}

#[test]
fn typed_handler_authoring_surface_compiles() {
    let client = Client::new("http://127.0.0.1:8080").expect("client configuration");
    let mut worker = Worker::new(client, "typed-contracts");

    worker.register_typed_activity("account.lookup", |_ctx, request: Request| async move {
        Ok(Response {
            account: request.account,
        })
    });
    worker.register_typed_workflow("account.workflow", typed_workflow);
    worker.register_typed_replayed_workflow(
        "account.replayed",
        ReplayState::default,
        typed_replayed_workflow,
    );
}

async fn typed_workflow(ctx: WorkflowContext, request: Request) -> Result<Response> {
    ctx.activity_typed_with_options("account.lookup", ActivityOptions::new(), request)
        .await
}

async fn typed_replayed_workflow(
    _ctx: WorkflowContext,
    request: Request,
    state: WorkflowInstance<ReplayState>,
) -> Result<Response> {
    state.update(|current| current.account = Some(request.account.clone()))?;
    Ok(Response {
        account: request.account,
    })
}
