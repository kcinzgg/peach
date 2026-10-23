"""公开资源取回：固定已验证的公网连接地址，每次跳转重新校验。"""
from __future__ import annotations

from contextlib import contextmanager
import ipaddress
import re
import time
from urllib.parse import urljoin, urlsplit

from curl_cffi import CurlOpt, ffi, lib, requests

from .http import HttpResponse, host_addresses, public_https_url
from .user_agent import USER_AGENT


@contextmanager
def pinned_session(url: str, *, client_options=None, deadline: float | None = None):
    """TLS 校验原域名，连接固定公网 IP；会话不继承来源凭据。"""
    parsed = urlsplit(url)
    host = (parsed.hostname or '').encode('idna').decode('ascii')
    if (not public_https_url(url) or parsed.port not in (None, 443) or
            not re.fullmatch(r'[a-zA-Z0-9.-]+', host) or
            any(ord(char) < 33 for char in url)):
        raise ValueError('资源地址必须是公网 HTTPS 地址，端口为 443')
    if deadline is not None and time.monotonic() >= deadline:
        raise ValueError('资源查询已超时')
    addresses = host_addresses(host)
    if deadline is not None and time.monotonic() >= deadline:
        raise ValueError('资源查询已超时')
    if not addresses or any(not ipaddress.ip_address(ip).is_global for ip in addresses):
        raise ValueError('资源域名必须解析到公网地址')
    address = addresses[0]
    if ':' in address:
        address = f'[{address}]'
    # CONNECT_TO 固定连接地址，包括代理的 CONNECT；证书校验和 SNI 保留原域名。
    target = ffi.gc(lib.curl_slist_append(ffi.NULL, f'{host}:443:{address}:443'.encode()),
                    lib.curl_slist_free_all)
    options = dict(client_options or {'trust_env': False})
    curl_options = {CurlOpt.CONNECT_TO: target}
    if not options.get('proxy') and not options.get('trust_env'):
        curl_options[CurlOpt.NOPROXY] = '*'
    with requests.Session(curl_options=curl_options, **options) as client:
        yield client


def get(url: str, *, timeout: float = 8, max_bytes: int = 8 * 1024 * 1024,
        max_redirects: int = 5, client_options=None) -> HttpResponse:
    """取回有体积和总时间预算的资源，不把重定向交给底层自动跟随。"""
    expires = time.monotonic() + timeout
    for _ in range(max_redirects + 1):
        with pinned_session(url, client_options=client_options, deadline=expires) as client:
            remaining = expires - time.monotonic()
            if remaining <= 0:
                raise ValueError('资源查询已超时')
            with client.stream('GET', url, timeout=remaining, allow_redirects=False,
                               headers={'User-Agent': USER_AGENT}) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get('location')
                    if not location:
                        raise ValueError('资源重定向缺少地址')
                    url = urljoin(url, location)
                    continue
                chunks, total = [], 0
                for chunk in response.iter_content():
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError('资源响应超过体积限制')
                    if time.monotonic() >= expires:
                        raise ValueError('资源查询已超时')
                    chunks.append(chunk)
                return HttpResponse(response.status_code,
                                    {key.lower(): value for key, value in response.headers.items()},
                                    b''.join(chunks), url)
    raise ValueError('资源重定向次数超过限制')
