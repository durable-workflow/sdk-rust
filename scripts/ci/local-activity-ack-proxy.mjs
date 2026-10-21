// Disposable qualification proxy. Forward a real completion to Server, consume
// its successful response, then close the downstream socket before its SDK sees
// the acknowledgement. This does not modify task, history or lease state.
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { parseProxyOrigin, proxyTargetUrl } from './proxy-target-url.mjs';

const upstream = parseProxyOrigin(process.env.DURABLE_WORKFLOW_ACK_UPSTREAM || '');
const commandType = process.env.DURABLE_WORKFLOW_ACK_COMMAND || 'record_local_activity';
if (!['record_local_activity', 'start_service_operation'].includes(commandType)) {
  throw new Error('Expected a local activity or service operation completion command');
}
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
    const result = await fetch(proxyTargetUrl(request.url, upstream), {
      method: request.method,
      headers,
      body: body.length ? body : undefined,
      redirect: 'manual',
      signal: AbortSignal.timeout(30_000),
    });
    const bytes = Buffer.from(await result.arrayBuffer());
    if (!receipt && result.ok && /\/worker\/workflow-tasks\/[^/]+\/complete$/.test(request.url)) {
      const commands = JSON.parse(body.toString()).commands;
      const selected = commands.find(command => command.type === commandType);
      const confirmed = selected && (commandType !== 'start_service_operation'
        || JSON.parse(bytes.toString()).recorded === true);
      if (confirmed) {
        receipt = { upstream_status: result.status, path: request.url,
          command_sha256: createHash('sha256').update(body).digest('hex'), acknowledgement_dropped: true,
          command_type: commandType };
        if (commandType === 'start_service_operation') {
          receipt.upstream_recorded = true;
          receipt.intent = { endpoint_name: selected.endpoint_name, service_name: selected.service_name,
            operation_name: selected.operation_name, namespace: selected.namespace,
            idempotency_key: selected.idempotency_key };
        }
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
