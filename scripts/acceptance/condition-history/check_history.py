"""Offline comparator for the newly captured two-member PHP history.

The declared identities follow unchanged H's two-member fixture. That minimal
fixture contains no timer rows, so a timer schedule is not mandatory. Every
timer row actually present must bind to an opened wait, and every timer terminal
must bind to its preceding schedule. Direct condition outcomes need no invented
timer row (H also supports that history shape).
"""

MEMBERS = [('votes', 'sha256:votes-v1'), ('approvals', 'sha256:approvals-v1')]
GROUP = {'parallel_group_id': 'parallel-calls:1:2', 'parallel_group_kind': 'condition',
         'parallel_group_base_sequence': 1, 'parallel_group_size': 2}


def require(condition, label):
    if not condition:
        raise ValueError(label)


def member(payload):
    require(isinstance(payload, dict), 'condition-payload')
    require(all(type(payload.get(field)) is int for field in
                ('sequence', 'parallel_group_index', 'parallel_group_base_sequence', 'parallel_group_size')),
            'typed-group-numbers')
    index = payload['parallel_group_index']
    require(index in (0, 1) and all(payload.get(k) == v for k, v in GROUP.items()), 'group-identity')
    key, fingerprint = MEMBERS[index]
    require(payload.get('condition_key') == key
            and payload.get('condition_definition_fingerprint') == fingerprint
            and payload.get('condition_wait_occurrence_id') == 'rust:condition-wait:' + str(index),
            'authored-member-identity')
    return index


def opened_wait(payload, waits):
    sequence = payload.get('sequence')
    require(type(sequence) is int and sequence in waits, 'outcome-before-open')
    wait = waits[sequence]
    require(payload.get('condition_wait_id') == wait['payload']['condition_wait_id'], 'wait-identity')
    return wait


def open_wait(payload, waits, tails):
    index = member(payload)
    sequence, timeout = payload['sequence'], payload.get('timeout_seconds')
    require(sequence == len(waits) + 1 and payload.get('condition_wait_id') == 'condition:' + str(sequence),
            'open-sequence-identity')
    require(type(timeout) is int and 0 <= timeout <= 300, 'typed-timeout-bound')
    if sequence <= 4:
        require(index == (sequence - 1) % 2, 'initial-reopen-members')
    previous = tails.get(index)
    if previous is None:
        require(sequence in (1, 2) and timeout == 300, 'original-timeout')
    else:
        require(timeout <= waits[previous]['payload']['timeout_seconds'], 'extended-timeout')
    waits[sequence] = {'payload': payload, 'member': index, 'terminal': None}
    tails[index] = sequence


def condition_outcome(kind, payload, waits, tails):
    index = member(payload)
    wait = opened_wait(payload, waits)
    require(wait['member'] == index and wait['terminal'] is None, 'condition-terminal-identity')
    if 'timeout_seconds' in payload:
        require(type(payload['timeout_seconds']) is int
                and payload['timeout_seconds'] == wait['payload']['timeout_seconds'], 'outcome-timeout')
    if kind == 'ConditionWaitTimedOut':
        require(tails[index] != payload['sequence'], 'current-tail-timeout')
    wait['terminal'] = kind


def timer_row(kind, payload, waits, tails, timers):
    require(isinstance(payload, dict), 'timer-payload')
    wait = opened_wait(payload, waits)
    timer_id = payload.get('timer_id')
    require(isinstance(timer_id, str) and timer_id and payload.get('timer_kind') == 'condition_timeout', 'timer-identity')
    require(type(payload.get('delay_seconds')) is int
            and payload['delay_seconds'] == wait['payload']['timeout_seconds'], 'timer-delay')
    identity = (payload['sequence'], payload['condition_wait_id'], payload['delay_seconds'])
    if kind == 'TimerScheduled':
        require(timer_id not in timers
                and not any(t['identity'][0] == identity[0] for t in timers.values()), 'duplicate-timer-schedule')
        timers[timer_id] = {'identity': identity, 'terminal': None}
    else:
        require(timer_id in timers and timers[timer_id]['identity'] == identity
                and timers[timer_id]['terminal'] is None, 'unresolved-timer-terminal')
        if kind == 'TimerFired':
            require(tails[wait['member']] != payload['sequence'], 'current-tail-timer-fired')
        timers[timer_id]['terminal'] = kind


def scan(events):
    waits, tails, timers = {}, {}, {}
    completed = False
    for event in events:
        require(isinstance(event, dict), 'history-row')
        kind, payload = event.get('event_type'), event.get('payload')
        if kind == 'WorkflowCompleted':
            require(not completed, 'duplicate-completion')
            completed = True
        elif kind == 'ConditionWaitOpened':
            require(not completed, 'open-after-completion')
            open_wait(payload, waits, tails)
        elif kind in ('ConditionWaitSatisfied', 'ConditionWaitTimedOut'):
            require(not completed, 'condition-outcome-after-completion')
            condition_outcome(kind, payload, waits, tails)
        elif kind in ('TimerScheduled', 'TimerFired', 'TimerCancelled'):
            require(not completed or kind == 'TimerCancelled', 'timer-after-completion')
            timer_row(kind, payload, waits, tails, timers)
    require(completed and len(waits) >= 4, 'completed-group-history')
    return waits


def check(report):
    require(report.get('outcome') == 'pass' and report.get('result') == 'both', 'typed-result')
    snapshots = [report.get(key) for key in ('initial', 'reopened', 'history')]
    require(all(isinstance(events, list) for events in snapshots), 'history-arrays')
    initial, reopened, final = snapshots
    require(reopened[:len(initial)] == initial and final[:len(reopened)] == reopened, 'append-only-history')
    waits = scan(final)
    require(sum(e.get('event_type') == 'ConditionWaitOpened' for e in initial) == 2, 'initial-two-members')
    types = [e.get('event_type') for e in final]
    require(not any(t in ('WorkflowFailed', 'ReplayFailure', 'WorkflowTimedOut') for t in types), 'no-replay-failure')
    signal_rows = [(i, e.get('payload', {}).get('signal_name')) for i, e in enumerate(final)
                   if e.get('event_type') == 'SignalReceived']
    require([name for _, name in signal_rows] == ['vote', 'vote', 'approve'], 'signal-order')
    open_rows = [i for i, e in enumerate(final) if e.get('event_type') == 'ConditionWaitOpened']
    require(open_rows[1] < signal_rows[0][0] < open_rows[2] < open_rows[3] < signal_rows[1][0], 'false-wake-row-order')
    early_satisfied = [e.get('payload', {}).get('sequence') for e in reopened
                       if e.get('event_type') == 'ConditionWaitSatisfied']
    require(2 in early_satisfied and 1 not in early_satisfied, 'unresolved-first-member')
    require(types.index('WorkflowCompleted') > signal_rows[-1][0], 'completion-after-approval')
    return {'outcome': 'pass', 'first_open_sequences': [1, 2], 'reopened_sequences': [3, 4],
            'all_open_sequences': list(waits), 'captured_events': len(final), 'result': report['result']}
