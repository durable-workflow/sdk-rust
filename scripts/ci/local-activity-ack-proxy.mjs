// Disposable qualification proxy. Forward a real completion to Server, consume
// its successful response, then close the downstream socket before its SDK sees
// the acknowledgement. This does not modify task, history or lease state.
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';

const upstream = process.env.DURABLE_WORKFLOW_ACK_UPSTREAM;
if (!upstream) throw new Error('DURABLE_WORKFLOW_ACK_UPSTREAM is required');
let receipt = null;

createServer(async (request, response) => {
  if (request.url === '/__qualification/lost-ack') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(receipt));
    return;
  }
  try {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    const headers = { ...request.headers };
    for (const name of ['host', 'connection', 'content-length', 'transfer-encoding']) delete headers[name];
    const result = await fetch(new URL(request.url, upstream), {
      method: request.method,
      headers,
      body: body.length ? body : undefined,
      redirect: 'manual',
      signal: AbortSignal.timeout(30_000),
    });
    const bytes = Buffer.from(await result.arrayBuffer());
    if (!receipt && result.ok && /\/worker\/workflow-tasks\/[^/]+\/complete$/.test(request.url)) {
      const commands = JSON.parse(body.toString()).commands;
      if (commands.some(command => command.type === 'record_local_activity')) {
        receipt = { upstream_status: result.status, path: request.url,
          command_sha256: createHash('sha256').update(body).digest('hex'), acknowledgement_dropped: true };
        response.destroy();
        return;
      }
    }
    const outputHeaders = Object.fromEntries(result.headers);
    for (const name of ['content-encoding', 'content-length', 'transfer-encoding', 'connection']) delete outputHeaders[name];
    response.writeHead(result.status, outputHeaders);
    response.end(bytes);
  } catch (error) {
    response.writeHead(502, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: error.message }));
  }
}).listen(Number(process.env.PORT || 3000), '0.0.0.0');
