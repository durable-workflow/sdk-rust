"""Transparent HTTP proxy in front of the runtime (adapted from the sdk-rust#146 harness).

It records every workflow-task /complete and /fail acknowledgement to
/state/acks.jsonl. When /state/delay.json exists, the next workflow-task /fail
is held for that many seconds before forwarding, unchanged, so a run or
execution deadline passes while the worker waits for the receipt.
"""
import http.client, http.server, json, pathlib, re, threading, time

state = pathlib.Path('/state')
lock = threading.Lock()

HOP_BY_HOP = frozenset(('connection', 'keep-alive', 'proxy-connection', 'transfer-encoding',
                        'content-length', 'te', 'trailer', 'upgrade'))
HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
HEADER_VALUE = re.compile(r"[\t\x20-\x7e\x80-\xff]*")


def forwardable_headers(pairs):
    """Upstream response headers that are safe to relay.

    Hop-by-hop headers are dropped. A name that is not an RFC 9110 token, or a
    value containing CR, LF or another control character, is refused rather than
    relayed, so an upstream response cannot split or inject headers.
    """
    safe = []
    for name, value in pairs:
        if name.lower() in HOP_BY_HOP:
            continue
        if not HEADER_NAME.fullmatch(name) or not HEADER_VALUE.fullmatch(value):
            raise ValueError('refusing to relay an unsafe upstream header')
        safe.append((name, value))
    return safe


class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def handle_request(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        request = json.loads(raw) if raw and 'json' in self.headers.get('Content-Type', '') else {}
        delayed = 0
        if self.command == 'POST' and self.path.endswith('/fail') and '/workflow-tasks/' in self.path:
            with lock:
                arm = state / 'delay.json'
                if arm.exists():
                    delayed = json.loads(arm.read_text())['seconds']
                    arm.unlink()
            if delayed:
                time.sleep(delayed)
        connection = http.client.HTTPConnection('qualification-origin', 8080, timeout=25)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ('host', 'connection')}
        connection.request(self.command, self.path, body=raw, headers=headers)
        response = connection.getresponse()
        body = response.read()
        if self.path.endswith(('/fail', '/complete')) and '/workflow-tasks/' in self.path:
            event = {
                'time': time.time(), 'path': self.path, 'status': response.status,
                'delay_seconds': delayed,
                'request_attempt': request.get('workflow_task_attempt'),
                'failure_type': request.get('failure', {}).get('type'),
                'response': {k: v for k, v in json.loads(body).items() if k != 'server_capabilities'},
            }
            with lock:
                with (state / 'acks.jsonl').open('a') as log:
                    log.write(json.dumps(event) + '\n')
        self.send_response(response.status)
        for key, value in forwardable_headers(response.getheaders()):
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True
        connection.close()

    do_GET = handle_request
    do_POST = handle_request
    do_DELETE = handle_request
    do_PATCH = handle_request

if __name__ == '__main__':
    http.server.ThreadingHTTPServer(('0.0.0.0', 8080), Proxy).serve_forever()
