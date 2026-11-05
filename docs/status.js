import {watchPublicStatus} from '/assets/status.mjs';

const pill = document.querySelector('[data-dw-status-link]');
if (pill) {
  const labels = {
    operational: 'All systems operational',
    degraded: 'Service degraded',
    incident: 'Service incident',
    maintenance: 'Maintenance',
    stale: 'Status stale',
    unverified: 'Status unverified',
    unavailable: 'Status unavailable',
    unknown: 'Status unknown',
  };
  const text = pill.querySelector('[data-dw-status-text]');
  const update = state => {
    const value = Object.hasOwn(labels, state) ? state : 'unknown';
    pill.dataset.status = value;
    if (text.textContent !== labels[value]) text.textContent = labels[value];
  };
  let cleanup = watchPublicStatus(update);
  window.addEventListener('pagehide', () => cleanup());
  window.addEventListener('pageshow', event => {
    if (event.persisted) cleanup = watchPublicStatus(update);
  });
}
