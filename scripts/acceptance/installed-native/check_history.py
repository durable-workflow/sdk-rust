"""Independent history check for one cell, using the runtime's public HTTP API.

Reads the driver's report from /state/driver.json, fetches the durable history
of every run it names, and writes /state/history.json. It does not use the SDK
under test.
"""
import json
import pathlib
import sys
import urllib.parse
import urllib.request

ORIGIN = 'http://qualification-origin:8080/api'
state = pathlib.Path('/state')
token = json.loads((state / 'credentials.json').read_text())['operator']
report = json.loads((state / 'driver.json').read_text())
problems = []


def get(path):
    request = urllib.request.Request(ORIGIN + path, headers={
        'Authorization': 'Bearer ' + token, 'Accept': 'application/json', 'X-Namespace': 'default',
        'X-Durable-Workflow-Control-Plane-Version': '2'})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def history(workflow_id, run_id):
    events, token_ = [], None
    for _ in range(50):
        query = '?' + urllib.parse.urlencode({'next_page_token': token_}) if token_ else ''
        page = get('/workflows/%s/runs/%s/history%s' % (
            urllib.parse.quote(workflow_id, safe=''), urllib.parse.quote(run_id, safe=''), query))
        events += page.get('events', [])
        token_ = page.get('next_page_token')
        if not token_:
            return events
    raise RuntimeError('history pagination did not end')


def timeouts(events):
    return [e for e in events if e.get('event_type') == 'WorkflowTimedOut']


def expect(condition, message):
    if not condition:
        problems.append(message)


observed = {}
if report.get('outcome') != 'pass':
    problems.append('driver did not pass: %s' % report.get('error'))
for name, case in report.get('cases', {}).items():
    runs = {case['original_run_id'], case['final_run_id']}
    case_history = {run: history(case['workflow_id'], run) for run in sorted(runs)}
    types = {run: [e.get('event_type') for e in events] for run, events in case_history.items()}
    final = case_history[case['final_run_id']]
    if report['cell'] == 'lifecycle':
        final_types = ' '.join(t or '' for t in types[case['final_run_id']])
        if name == 'regular':
            expect('Activity' in final_types and 'Signal' in final_types and not timeouts(final),
                   name + ': expected activity and signal events without a timeout')
        elif name == 'reject':
            expect('Activity' in final_types and 'Failed' in final_types and not timeouts(final),
                   name + ': expected activity and failure events without a timeout')
        elif name in ('continue', 'after-timeout'):
            expect(case['original_run_id'] != case['final_run_id'], name + ': run did not advance')
            expect(all(not timeouts(events) for events in case_history.values()), name + ': unexpected timeout')
        elif name == 'timeout':
            found = timeouts(final)
            expect(len(found) == 1 and (found[0].get('payload') or {}).get('timeout_kind') == 'run_timeout',
                   name + ': expected exactly one run_timeout event, got %r' % found)
        elif name == 'cancel':
            expect(any('Cancel' in (t or '') for t in types[case['final_run_id']]), name + ': no cancellation event')
            expect(not timeouts(final), name + ': unexpected timeout')
    else:
        found = timeouts(final)
        expect(len(found) == 1 and (found[0].get('payload') or {}).get('timeout_kind') == name + '_timeout',
               name + ': expected exactly one %s_timeout event, got %r' % (name, found))
        nxt = case['next']
        next_history = history(nxt['workflow_id'], nxt['final_run_id'])
        expect(not timeouts(next_history), name + ': follow-up workflow timed out')
        types['next:' + nxt['final_run_id']] = [e.get('event_type') for e in next_history]
    observed[name] = {'event_types': types, 'timeout_events': timeouts(final)}

result = {'outcome': 'fail' if problems else 'pass', 'problems': problems, 'cases': observed}
(state / 'history.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
print(json.dumps({'outcome': result['outcome'], 'problems': problems}))
sys.exit(1 if problems else 0)
