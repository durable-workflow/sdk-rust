use super::parallel::{all_path, event_indices, member_path, Member, Members};
use super::{child_lifecycle, HistoryEvent, RecordedCommand};
use crate::ParallelGroupMetadata;

fn member(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
    path: &[ParallelGroupMetadata],
) -> Option<Member> {
    if !member_path(history, indices, sequence, path) {
        return None;
    }
    let terminal = child_lifecycle(history, indices)?;
    // Two distinct calls may reuse one child run. The parent call identity,
    // rather than the target run, must be unique within the consumed group.
    let call = history[*indices.first()?]
        .payload
        .get("child_call_id")?
        .as_str()?;
    Some(Member {
        path: path.to_vec(),
        identity: call.into(),
        terminal,
        failed: terminal.is_some_and(|index| history[index].event_type != "ChildRunCompleted"),
    })
}

pub(super) fn members(history: &[HistoryEvent], commands: &[RecordedCommand]) -> Members {
    let events = event_indices(history, "Child");
    Members(
        commands
            .iter()
            .filter_map(|command| {
                let RecordedCommand::ChildWorkflow {
                    sequence,
                    parallel_group_path: Some(path),
                    ..
                } = command
                else {
                    return None;
                };
                if !all_path(path, "child") {
                    return None;
                }
                member(history, events.get(sequence)?, *sequence, path)
                    .map(|member| (*sequence, member))
            })
            .collect(),
    )
}
