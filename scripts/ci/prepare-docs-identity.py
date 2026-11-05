#!/usr/bin/env python3
"""Apply shared static chrome without replacing generated API/search content."""
from html import escape
import json
from pathlib import Path
import re
import sys

REPOSITORY = Path(__file__).resolve().parents[2]
DOCS = REPOSITORY / 'docs'


def region(name: str, content: str) -> str:
    return f'<!-- dw:{name}:start -->\n{content.strip()}\n<!-- dw:{name}:end -->'


def replace_region(html: str, name: str, value: str, landing: bool) -> str:
    marker = f'<!-- dw:{name} -->'
    pattern = rf'<!-- dw:{name}:start -->.*?<!-- dw:{name}:end -->'
    if re.search(pattern, html, re.DOTALL):
        result, count = re.subn(pattern, lambda _: value, html, flags=re.DOTALL)
    elif marker in html:
        count = html.count(marker)
        result = html.replace(marker, value)
    elif landing:
        raise ValueError(f'missing {name} source marker')
    else:
        anchor = r'(<body\b[^>]*>)' if name == 'header' else r'(</body>)'
        def insert(match):
            return match[0] + '\n' + value if name == 'header' else value + '\n' + match[0]
        result, count = re.subn(anchor, insert, html)
    if count != 1:
        raise ValueError(f'ambiguous {name}')
    return result


def published_landing(html: str, identity: dict) -> str:
    version, revision = identity['version'], identity['source_revision']
    if not re.fullmatch(r'\d+\.\d+\.\d+', version) or not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('invalid published documentation identity')
    html = html.replace('Stable release</strong>', f'Stable release {escape(version)}</strong>')
    return re.sub(r'(https://github.com/durable-workflow/sdk-rust/blob/)[^/]+(/examples/)',
                  lambda match: match[1] + revision + match[2], html)


def status_runtime(html: str) -> str:
    count = html.count('src="/status.js"')
    if count == 1:
        return html
    if count or html.count('</head>') != 1:
        raise ValueError('ambiguous shared status runtime')
    return html.replace('</head>', '<script type="module" src="/status.js"></script></head>')


def page_content(path: Path, root: Path, header: str, footer: str, identity: dict | None) -> str | None:
    html = path.read_text(encoding='utf-8')
    landing = path == root / 'index.html'
    if '<meta http-equiv="refresh"' in html:
        return None
    if not landing and '<meta name="generator" content="rustdoc">' not in html:
        return None
    html = replace_region(html, 'header', header, landing)
    html = replace_region(html, 'footer', footer, landing)
    html = status_runtime(html)
    if not landing:
        return re.sub(r'(<body\b[^>]*\bclass=")([^"]*)(")',
                      lambda match: match[1] + ' '.join(dict.fromkeys([*match[2].split(), 'dw-reference'])) + match[3], html, count=1)
    return published_landing(html, identity) if identity else html


def prepare(root: Path) -> int:
    if root.is_symlink() or not root.is_dir():
        raise ValueError('documentation output must be a regular directory')
    root = root.resolve()
    header = region('header', (DOCS / 'header.html').read_text())
    footer = region('footer', (DOCS / 'footer.html').read_text())
    identity_path = root / '__build.json'
    identity = json.loads(identity_path.read_text()) if identity_path.exists() else None
    changes: dict[Path, str] = {}
    for path in root.rglob('*.html'):
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError('documentation page must be a regular file inside the output directory')
        html = page_content(path, root, header, footer, identity)
        if html is not None:
            changes[path] = html
    if root / 'index.html' not in changes:
        raise ValueError('missing landing page')
    for path in changes:
        with path.open('w', encoding='utf-8') as output:
            output.write(changes[path])
    return len(changes)


if __name__ == '__main__':
    if sys.argv[1:] not in ([], ['--stable']):
        raise SystemExit('Usage: prepare-docs-identity.py [--stable]')
    directory = REPOSITORY / 'target' / ('stable-docs' if sys.argv[1:] else 'doc')
    if not directory.resolve(strict=True).is_relative_to(REPOSITORY / 'target'):
        raise SystemExit('documentation output must stay inside the checkout target directory')
    print(f'Applied shared identity to {prepare(directory)} documentation pages.')
