"""Header-relay safety for the acceptance proxy (CodeQL py/http-response-splitting)."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from proxy import FORWARDED_HEADERS, forwardable_headers  # noqa: E402


class ForwardableHeadersTest(unittest.TestCase):
    def test_relays_only_the_reviewed_set(self):
        relayed = forwardable_headers([
            ('Content-Type', 'application/json'),
            ('X-Durable-Workflow-Protocol-Version', '1.20'),
            ('Retry-After', '1'),
            ('Connection', 'keep-alive'),
            ('Transfer-Encoding', 'chunked'),
            ('Content-Length', '12'),
            ('Set-Cookie', 'session=1'),
            ('Location', 'https://example.test/'),
            ('X-Other', 'v'),
        ])
        self.assertEqual(relayed, [
            ('Content-Type', 'application/json'),
            ('X-Durable-Workflow-Protocol-Version', '1.20'),
            ('Retry-After', '1'),
        ])

    def test_reviewed_set_has_no_hop_by_hop_or_cookie_headers(self):
        for name in ('connection', 'keep-alive', 'transfer-encoding', 'content-length',
                     'te', 'trailer', 'upgrade', 'proxy-connection', 'set-cookie'):
            self.assertNotIn(name, FORWARDED_HEADERS)

    def test_refuses_control_and_non_ascii_values(self):
        for value in ('a\r\nSet-Cookie: x=1', 'a\nb', 'a\rb', 'a\x00b', 'a\x7fb',
                      'a\x85b', 'a\x9fb', 'a\xa0b', 'caf\xe9'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                forwardable_headers([('Content-Type', value)])

    def test_validates_dropped_headers_before_dropping_them(self):
        for name, value in (('Connection', 'close\r\nX: y'), ('Transfer-Encoding', 'a\x85'),
                            ('Set-Cookie', 'a\nb'), ('X-Other', '\x9f')):
            with self.subTest(name=name), self.assertRaises(ValueError):
                forwardable_headers([('Content-Type', 'application/json'), (name, value)])

    def test_refuses_invalid_header_names(self):
        for name in ('Bad Name', 'Bad:Name', 'Bad\r\nName', '', 'Bad\x00', 'Caf\xe9'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                forwardable_headers([(name, 'v')])

    def test_allows_htab_and_space_in_values(self):
        self.assertEqual(forwardable_headers([('Content-Type', 'application/json;\tcharset=utf-8 ')]),
                         [('Content-Type', 'application/json;\tcharset=utf-8 ')])


if __name__ == '__main__':
    unittest.main()
