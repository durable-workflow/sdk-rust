"""Header-relay safety for the acceptance proxy (CodeQL py/http-response-splitting)."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from proxy import forwardable_headers  # noqa: E402


class ForwardableHeadersTest(unittest.TestCase):
    def test_relays_ordinary_headers_and_drops_hop_by_hop(self):
        relayed = forwardable_headers([
            ('Content-Type', 'application/json'),
            ('X-Durable-Workflow-Protocol-Version', '1.20'),
            ('Connection', 'keep-alive'),
            ('Transfer-Encoding', 'chunked'),
            ('Content-Length', '12'),
            ('Retry-After', '1'),
        ])
        self.assertEqual(relayed, [
            ('Content-Type', 'application/json'),
            ('X-Durable-Workflow-Protocol-Version', '1.20'),
            ('Retry-After', '1'),
        ])

    def test_refuses_crlf_and_control_characters_in_values(self):
        for value in ('a\r\nSet-Cookie: x=1', 'a\nb', 'a\rb', 'a\x00b', 'a\x7fb'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                forwardable_headers([('X-Test', value)])

    def test_refuses_invalid_header_names(self):
        for name in ('Bad Name', 'Bad:Name', 'Bad\r\nName', '', 'Bad\x00'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                forwardable_headers([(name, 'v')])


if __name__ == '__main__':
    unittest.main()
