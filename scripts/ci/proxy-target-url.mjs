const requestTargetBase = new URL('https://request-target.invalid');

export function parseProxyOrigin(value) {
  let origin;
  try {
    origin = new URL(value);
  } catch {
    throw new Error('DURABLE_WORKFLOW_ACK_UPSTREAM must be an exact HTTP(S) origin');
  }

  if (!['http:', 'https:'].includes(origin.protocol)
      || origin.username !== ''
      || origin.password !== ''
      || origin.pathname !== '/'
      || origin.search !== ''
      || origin.hash !== '') {
    throw new Error('DURABLE_WORKFLOW_ACK_UPSTREAM must be an exact HTTP(S) origin');
  }

  return origin;
}

export function proxyTargetUrl(requestTarget, upstreamOrigin) {
  const incoming = new URL(requestTarget, requestTargetBase);
  const target = new URL(upstreamOrigin);
  target.pathname = incoming.pathname;
  target.search = incoming.search;
  target.hash = '';

  if (target.origin !== upstreamOrigin.origin) {
    throw new Error('Forwarded request escaped the configured upstream origin');
  }

  return target;
}
