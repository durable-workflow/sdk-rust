import fs from 'node:fs';
import path from 'node:path';

const EXPECTED_INTERACTIONS = [{ type: 'click', selector: '.sidebar-menu-toggle' }];

function normalizedArtifactPath(manifestPath, artifactPath) {
  return path.relative(path.dirname(manifestPath), artifactPath).replaceAll(path.sep, '/');
}

export function bindResponsiveNavigationCapture({ manifestPath, reportPath, width, height }) {
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  if (
    manifest.schema !== 'durable-workflow.pipeline.visual-review/v1'
    || !Array.isArray(manifest.captures)
  ) {
    throw new Error('the visual evidence manifest has an unsupported schema');
  }

  const report = normalizedArtifactPath(manifestPath, reportPath);
  const matches = manifest.captures.filter((capture) => (
    capture?.surface === 'rust-sdk-reference'
    && capture.state === 'navigation-open'
    && capture.viewport?.width === width
    && capture.viewport?.height === height
    && capture.report === report
  ));
  if (matches.length !== 1) {
    throw new Error('the navigation isolation probe must match exactly one manifest capture');
  }

  const capture = matches[0];
  if (
    capture.full_page !== false
    || JSON.stringify(capture.interactions) !== JSON.stringify(EXPECTED_INTERACTIONS)
    || ![undefined, 'responsive'].includes(capture.state_scope)
  ) {
    throw new Error('the navigation isolation probe cannot bind malformed capture metadata');
  }

  capture.state_scope = 'responsive';
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
}
