import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("docs_identity", Path(__file__).with_name("prepare-docs-identity.py"))
identity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(identity)


class DocumentationIdentity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "index.html").write_text('<html><body><!-- dw:header --><main id="main-content"><strong>Stable release</strong><a href="https://github.com/durable-workflow/sdk-rust/blob/main/examples/hello_world.rs">Example</a></main><!-- dw:footer --></body></html>')
        self.api = self.root / "api.html"
        self.content = '<nav class="sidebar"><a href="#method.example">Method</a></nav><main><rustdoc-search></rustdoc-search><h1 id="method.example">Example API</h1><pre>fn example()</pre></main>'
        self.api.write_text('<html><head><meta name="generator" content="rustdoc"></head><body class="rustdoc mod">' + self.content + '</body></html>')

    def test_static_chrome_preserves_generated_api_search_and_anchors(self):
        self.assertEqual(identity.prepare(self.root), 2)
        rendered = self.api.read_text()
        self.assertIn(self.content, rendered)
        self.assertIn('class="rustdoc mod dw-reference"', rendered)
        self.assertEqual(rendered.count('class="dw-site-header"'), 1)
        self.assertEqual(rendered.count('class="dw-site-footer"'), 1)
        self.assertEqual(rendered.count('<h1'), 1)
        before = {p.name:p.read_bytes() for p in self.root.iterdir()}
        identity.prepare(self.root)
        self.assertEqual(before, {p.name:p.read_bytes() for p in self.root.iterdir()})

    def test_published_identity_drives_badge_and_example_links(self):
        revision = 'a' * 40
        (self.root / "__build.json").write_text(json.dumps({'version':'4.12.0', 'source_revision':revision}))
        identity.prepare(self.root)
        html = (self.root / "index.html").read_text()
        self.assertIn('Stable release 4.12.0</strong>', html)
        self.assertIn('/blob/' + revision + '/examples/', html)
        self.assertNotIn('/blob/main/examples/', html)

    def test_redirects_remain_untouched(self):
        redirect = self.root / 'redirect.html'
        original = '<meta http-equiv="refresh" content="0;URL=api.html">'
        redirect.write_text(original)
        identity.prepare(self.root)
        self.assertEqual(redirect.read_text(), original)

    def test_malformed_or_linked_page_refuses_before_any_write(self):
        before = (self.root / 'index.html').read_bytes()
        self.api.write_text(self.api.read_text().replace('</body>', '</body></body>'))
        with self.assertRaisesRegex(ValueError, 'ambiguous footer'):
            identity.prepare(self.root)
        self.assertEqual((self.root / 'index.html').read_bytes(), before)
        self.api.unlink()
        self.api.symlink_to(self.root / 'index.html')
        with self.assertRaisesRegex(ValueError, 'regular file'):
            identity.prepare(self.root)
        self.assertEqual((self.root / 'index.html').read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
