"""Offline comparator for the newly captured two-member PHP history."""


def require(condition, label):
    if not condition:
        raise ValueError(label)


def check(report):
    require(report.get('outcome') == 'pass' and report.get('result') == 'both', 'typed-result')
    snapshots = [report.get(key) for key in ('initial', 'reopened', 'history')]
    require(all(isinstance(events, list) for events in snapshots), 'history-arrays')
    initial, reopened, final = snapshots
    require(reopened[:len(initial)] == initial and final[:len(reopened)] == reopened, 'append-only-history')
    opens = [e for e in final if e.get('event_type') == 'ConditionWaitOpened']
    require(len(opens) >= 4, 'four-opens')
    first = [e['payload'] for e in opens[:4]]
    require(all(type(p.get(field)) is int for p in first
                for field in ('sequence', 'parallel_group_index', 'parallel_group_base_sequence', 'parallel_group_size')),
            'typed-group-numbers')
    require([p.get('sequence') for p in first] == [1, 2, 3, 4], 'open-sequences')
    require([p.get('condition_key') for p in first] == ['votes', 'approvals', 'votes', 'approvals'], 'member-keys')
    require([p.get('parallel_group_index') for p in first] == [0, 1, 0, 1], 'member-indices')
    require(all(p.get('parallel_group_id') == 'parallel-calls:1:2'
                and p.get('parallel_group_kind') == 'condition'
                and p.get('parallel_group_base_sequence') == 1
                and p.get('parallel_group_size') == 2 for p in first), 'group-identity')
    for index in (0, 1):
        original, successor = first[index], first[index + 2]
        for field in ('condition_wait_occurrence_id', 'condition_definition_fingerprint'):
            require(isinstance(original.get(field), str) and original[field] == successor.get(field), field)
        require(original.get('timeout_seconds') == 300
                and isinstance(successor.get('timeout_seconds'), int)
                and not isinstance(successor['timeout_seconds'], bool)
                and 0 <= successor['timeout_seconds'] <= 300, 'original-timeout-bound')
    require(first[0]['condition_wait_occurrence_id'] != first[1]['condition_wait_occurrence_id'], 'distinct-members')
    types = [e.get('event_type') for e in final]
    require(not any(t in ('WorkflowFailed', 'ReplayFailure', 'WorkflowTimedOut') for t in types), 'no-replay-failure')
    require(types.count('WorkflowCompleted') == 1, 'one-completion')
    signal_rows = [(i, e.get('payload', {}).get('signal_name')) for i, e in enumerate(final)
                   if e.get('event_type') == 'SignalReceived']
    require([name for _, name in signal_rows] == ['vote', 'vote', 'approve'], 'signal-order')
    open_rows = [i for i, e in enumerate(final) if e.get('event_type') == 'ConditionWaitOpened']
    require(open_rows[1] < signal_rows[0][0] < open_rows[2] < open_rows[3] < signal_rows[1][0], 'false-wake-row-order')
    early_satisfied = [e.get('payload', {}).get('sequence') for e in reopened
                       if e.get('event_type') == 'ConditionWaitSatisfied']
    require(2 in early_satisfied and 1 not in early_satisfied, 'unresolved-first-member')
    return {'outcome': 'pass', 'first_open_sequences': [1, 2], 'reopened_sequences': [3, 4],
            'captured_events': len(final), 'result': report['result']}
