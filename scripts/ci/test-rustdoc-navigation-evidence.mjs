import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { bindResponsiveNavigationCapture } from './rustdoc-navigation-evidence.mjs';

const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'rustdoc-navigation-evidence-'));

try {
  const manifestPath = path.join(directory, 'manifest.json');
  const reportPath = path.join(directory, 'navigation-open-390x844.json');
  const capture = {
    surface: 'rust-sdk-reference',
    state: 'navigation-open',
    viewport: { width: 390, height: 844 },
    full_page: false,
    screenshot: 'navigation-open-390x844.png',
    report: 'navigation-open-390x844.json',
    interactions: [{ type: 'click', selector: '.sidebar-menu-toggle' }],
  };
  const writeManifest = (captures) => fs.writeFileSync(manifestPath, `${JSON.stringify({
    schema: 'durable-workflow.pipeline.visual-review/v1',
    captures,
  }, null, 2)}\n`, 'utf8');

  fs.writeFileSync(reportPath, '{}\n', 'utf8');
  writeManifest([capture]);
  bindResponsiveNavigationCapture({ manifestPath, reportPath, width: 390, height: 844 });
  const bound = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  assert.equal(bound.captures[0].state_scope, 'responsive');

  for (const malformed of [
    { ...capture, full_page: true },
    { ...capture, interactions: [] },
    { ...capture, state_scope: 'all' },
  ]) {
    writeManifest([malformed]);
    assert.throws(
      () => bindResponsiveNavigationCapture({ manifestPath, reportPath, width: 390, height: 844 }),
      /cannot bind malformed capture metadata/,
    );
  }

  writeManifest([capture, { ...capture }]);
  assert.throws(
    () => bindResponsiveNavigationCapture({ manifestPath, reportPath, width: 390, height: 844 }),
    /must match exactly one manifest capture/,
  );
} finally {
  fs.rmSync(directory, { recursive: true, force: true });
}

console.log('Validated responsive rustdoc navigation evidence binding.');
