//! Server-free workflow replay and in-process workflow/activity tests.

mod environment;
mod history;
mod runner;

pub use environment::{ReplayReport, WorkflowEnvironment};
pub use history::WorkflowHistory;

/// A testing input or execution limit that prevents a trustworthy result.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum TestingError {
    #[error("invalid or incomplete workflow history: {0}")]
    InvalidHistory(String),
    #[error("in-process workflow testing does not support command {0:?}")]
    UnsupportedCommand(String),
    #[error("in-process workflow testing exceeded its {0}-step limit")]
    StepLimit(usize),
    #[error("workflow is waiting without a resolvable test operation")]
    WorkflowBlocked,
}
