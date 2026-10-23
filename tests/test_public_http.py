"""公开资源网络守卫：全部网络响应和 DNS 均由本地夹具提供。"""
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from peach import public_http, routes_media


class PublicHttpTests(unittest.TestCase):
    def setUp(self):
        self.dns = patch.object(public_http, 'host_addresses', return_value=('1.1.1.1',)).start()
        self.session = patch.object(public_http.requests, 'Session').start()
        self.addCleanup(patch.stopall)
        self.response = MagicMock(status_code=200, headers={'content-type': 'image/png'})
        self.response.iter_content.return_value = iter((b'pixels',))
        self.client = self.session.return_value.__enter__.return_value
        self.client.stream.return_value.__enter__.return_value = self.response

    def test_only_public_https_targets_can_create_a_session(self):
        for url in ('http://public.example/icon', 'https://127.0.0.1/icon',
                    'https://[::1]/icon', 'https://peach.local/icon',
                    'https://user:pass@public.example/icon', 'https://public.example:8443/icon'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                public_http.get(url)
        self.session.assert_not_called()
        for addresses in ((), ('127.0.0.1',), ('1.1.1.1', '192.0.2.1'), ('::ffff:127.0.0.1',)):
            self.dns.return_value = addresses
            with self.subTest(addresses=addresses), self.assertRaises(ValueError):
                public_http.get('https://public.example/icon')
        self.session.assert_not_called()

    def test_connection_is_pinned_and_tls_keeps_the_original_hostname(self):
        result = public_http.get('https://public.example/icon')
        self.assertEqual(result.body, b'pixels')
        target = self.session.call_args.kwargs['curl_options'][public_http.CurlOpt.CONNECT_TO]
        self.assertEqual(public_http.ffi.string(target.data), b'public.example:443:1.1.1.1:443')
        self.dns.assert_called_once_with('public.example')
        self.assertEqual(self.client.stream.call_args.args[:2], ('GET', 'https://public.example/icon'))
        self.assertFalse(self.client.stream.call_args.kwargs['allow_redirects'])
        self.assertNotIn('verify', self.session.call_args.kwargs)

    def test_redirect_target_is_validated_before_another_connection(self):
        for location in ('http://127.0.0.1:9/probe', 'https://192.0.2.1/probe',
                         'https://public.example:444/probe'):
            self.response.status_code = 302
            self.response.headers = {'location': location}
            self.session.reset_mock()
            with self.subTest(location=location), self.assertRaises(ValueError):
                public_http.get('https://public.example/icon')
            self.assertEqual(self.session.call_count, 1)
        self.response.headers = {'location': 'https://other.example/icon'}
        self.dns.side_effect = [('1.1.1.1',), ('127.0.0.1',)]
        self.session.reset_mock()
        with self.assertRaises(ValueError):
            public_http.get('https://public.example/icon')
        self.assertEqual(self.session.call_count, 1)

    def test_same_hostname_is_resolved_and_checked_on_each_redirect(self):
        self.response.status_code = 302
        self.response.headers = {'location': '/next'}
        self.dns.side_effect = [('1.1.1.1',), ('127.0.0.1',)]
        with self.assertRaises(ValueError):
            public_http.get('https://public.example/icon')
        self.assertEqual(self.session.call_count, 1)

    def test_public_redirect_has_an_independent_pinned_session_without_cookies(self):
        first = MagicMock(status_code=302, headers={'location': 'https://cdn.example/icon'})
        second = MagicMock(status_code=200, headers={'Content-Type': 'image/png'})
        second.iter_content.return_value = iter((b'pixels',))
        self.client.stream.return_value.__enter__.side_effect = [first, second]
        self.dns.side_effect = [('1.1.1.1',), ('2606:4700:4700::1111',)]
        result = public_http.get('https://public.example/icon')
        self.assertEqual(result.url, 'https://cdn.example/icon')
        self.assertEqual(result.headers['content-type'], 'image/png')
        self.assertEqual(self.session.call_count, 2)
        target = self.session.call_args.kwargs['curl_options'][public_http.CurlOpt.CONNECT_TO]
        self.assertEqual(public_http.ffi.string(target.data),
                         b'cdn.example:443:[2606:4700:4700::1111]:443')
        for call in self.client.stream.call_args_list:
            self.assertNotIn('Cookie', call.kwargs['headers'])
            self.assertNotIn('Authorization', call.kwargs['headers'])

    def test_redirects_size_and_dns_time_are_bounded(self):
        self.response.iter_content.return_value = iter((b'123', b'456'))
        with self.assertRaisesRegex(ValueError, '体积'):
            public_http.get('https://public.example/icon', max_bytes=5)
        self.response.status_code = 302
        self.response.headers = {'location': '/again'}
        self.session.reset_mock()
        with self.assertRaisesRegex(ValueError, '重定向次数'):
            public_http.get('https://public.example/icon', max_redirects=2)
        self.assertEqual(self.session.call_count, 3)
        self.session.reset_mock()
        with patch.object(public_http.time, 'monotonic', side_effect=[0, 0, 9]):
            with self.assertRaisesRegex(ValueError, '超时'):
                public_http.get('https://public.example/icon', timeout=8)
        self.session.assert_not_called()

    def test_site_icon_declarations_cannot_reach_loopback(self):
        visited = []
        def stream(_method, url, **_kwargs):
            visited.append(url)
            response = MagicMock(status_code=200, headers={'content-type': 'text/html'})
            response.iter_content.return_value = iter((
                b'<link rel="icon" href="http://127.0.0.1:9/audit.svg">'
                b'<link rel="manifest" href="https://127.0.0.1/audit.json">',))
            context = MagicMock()
            context.__enter__.return_value = response
            return context
        self.client.stream.side_effect = stream
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(routes_media.link_marks, 'render_mark', return_value=None), \
                patch.object(routes_media.link_marks, 'plain_mark', return_value=None):
            routes_media._site_mark_response(SimpleNamespace(), 'https://public.example/profile',
                                             Path(tmp).resolve())
        self.assertTrue(visited)
        self.assertTrue(all(url.startswith('https://public.example') for url in visited))
