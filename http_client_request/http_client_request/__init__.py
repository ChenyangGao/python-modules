#!/usr/bin/env python3
# coding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 10)
__all__ = [
    "CONNECTION_POOL", "HTTPConnection", "HTTPSConnection", "HTTPResponse", 
    "ConnectionPool", "request", "set_keepalive", 
]

import socket

from array import array
from collections import defaultdict, deque, UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from http.client import (
    HTTPConnection as BaseHTTPConnection, HTTPSConnection as BaseHTTPSConnection, 
    HTTPResponse as BaseHTTPResponse, ImproperConnectionState, 
)
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from io import BufferedReader
from inspect import signature
from os import PathLike
from platform import system
from select import select
from socket import socket as Socket
from ssl import SSLWantReadError
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit, urlunsplit, ParseResult, SplitResult
from warnings import warn

from argtools import argcount
from cookietools import cookies_to_str, extract_cookies
from dicttools import get_all_items
from filewrap import SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import decompress_response, parse_response
from property import funcproperty
from urllib3 import HTTPResponse as Urllib3HTTPResponse, HTTPHeaderDict
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

HTTP_CONNECTION_KWARGS: Final = signature(BaseHTTPConnection).parameters.keys()
HTTPS_CONNECTION_KWARGS: Final = signature(BaseHTTPSConnection).parameters.keys()


def get_host_pair(url: None | str, /) -> None | tuple[str, None | int]:
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    urlp = urlsplit(url)
    return urlp.hostname or "localhost", urlp.port


def is_ipv6(host: str, /) -> bool:
    from ipaddress import _BaseV6, AddressValueError
    try:
        _BaseV6._ip_int_from_string(host) # type: ignore
        return True
    except AddressValueError:
        return False


def sock_buf_readable(sock: Socket, /) -> bool:
    rlist, *_ = select([sock], (), (), 0)
    return bool(rlist)


def set_keepalive_linux(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 5, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on Linux.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec) # type: ignore
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def set_keepalive_osx(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 5, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on MaxOSX.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, after_idle_sec) # type: ignore
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def set_keepalive_win(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 5, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on Windows.
    """
    sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, after_idle_sec * 1000, interval_sec * 1000)) # type: ignore


def set_keepalive(
    sock: Socket, 
    after_idle_sec: int = 1, 
    interval_sec: int = 3, 
    max_fails: int = 5, 
):
    """Set TCP keepalive on an open socket on multiple platforms.

    It activates after `after_idle_sec` second of idleness,
    then sends a keepalive ping once every `interval_sec` seconds,
    and closes the connection after `max_fails` failed ping (max_fails), or 15 seconds.
    """
    platform = system()
    match platform:
        case "Darwin":
            return set_keepalive_osx(sock, after_idle_sec, interval_sec, max_fails)
        case "Windows":
            return set_keepalive_win(sock, after_idle_sec, interval_sec, max_fails)
        case "Linux":
            return set_keepalive_linux(sock, after_idle_sec, interval_sec, max_fails)
    raise RuntimeError(f"unsupport platform {platform!r}")


try:
    from fcntl import ioctl
    from termios import FIONREAD

    def sock_bufsize(sock, /) -> int:
        sock_size = array("i", [0])
        ioctl(sock, FIONREAD, sock_size)
        return sock_size[0]
except ImportError:
    from ctypes import byref, c_ulong, WinDLL # type: ignore
    ws2_32 = WinDLL("ws2_32")
    def sock_bufsize(sock, /) -> int:
        FIONREAD = 0x4004667f
        b = c_ulong(0)
        ws2_32.ioctlsocket(sock.fileno(), FIONREAD, byref(b))
        return b.value


class HTTPResponse(BaseHTTPResponse):
    _pos: int = 0
    method: str
    pool: None | ConnectionPool = None
    connection: None | HTTPConnection | HTTPSConnection = None

    def __del__(self, /):
        self.close()

    @funcproperty
    def _fp(self, /) -> BufferedReader:
        return self.fp

    @property
    def buffer_size(self, /) -> int:
        fp = self._fp
        sock = fp.raw._sock
        sock.setblocking(False)            
        try:
            cache_size = len(fp.peek() or b"")
            while True:
                buffer_size = cache_size + sock_bufsize(sock)
                if cache_size == (cache_size := len(fp.peek() or b"")):
                    return buffer_size
        except (BlockingIOError, SSLWantReadError):
            return 0
        finally:
            sock.setblocking(True)

    @property
    def unbuffer_size(self, /) -> int:
        method = self.__dict__.get("method", "").upper()
        content_length = self.length
        if method == "HEAD" or content_length == 0:
            return 0
        if content_length:
            return content_length - self.tell() - self.buffer_size
        sock = self._fp.raw._sock
        if sock_bufsize(sock) == sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF):
            return -1
        else:
            return 0

    @property
    def unread_size(self, /) -> int:
        method = self.__dict__.get("method", "").upper()
        content_length = self.length
        if method == "HEAD" or content_length == 0:
            return 0
        sock = self._fp.raw._sock
        if content_length:
            return content_length - self.tell()
        elif sock_bufsize(sock) == sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF):
            return -1
        else:
            return self.buffer_size

    def _close_conn(self, /):
        fp = self._fp = self.fp
        setattr(self, "fp", None)
        try:
            pool = self.pool
            connection = self.connection
            if pool and connection:
                pool.return_connection(connection)
        except AttributeError:
            fp.close()

    def get_urllib3_response(self, /) -> Urllib3HTTPResponse:
        return Urllib3HTTPResponse(
            body=self, 
            headers=HTTPHeaderDict(self.headers.items()), 
            status=self.status, 
            version=self.version, 
            version_string="HTTP/%s" % (self.version or "?"), 
            reason=self.reason, 
            preload_content=False, 
            decode_content=True, 
            original_response=self, 
            msg=self.headers, 
            request_method=getattr(self, "method", None), 
            request_url=self.url, 
        )

    def read(self, /, amt=None) -> bytes:
        data = super().read(amt)
        self._pos += len(data)
        return data

    def read1(self, /, n=-1) -> bytes:
        data = super().read1(n)
        self._pos += len(data)
        return data

    def readinto(self, /, b) -> int:
        count = super().readinto(b)
        self._pos += count
        return count

    def readinto1(self, buffer, /) -> int:
        count = super().readinto1(buffer)
        self._pos += count
        return count

    def readline(self, limit=-1) -> bytes:
        data = super().readline(limit)
        self._pos += len(data)
        return data

    def readlines(self, hint=-1, /) -> list[bytes]:
        ls = super().readlines(hint)
        self._pos += sum(map(len, ls))
        return ls

    def tell(self, /) -> int:
        return self._pos


class HTTPConnectionMixin:

    def __del__(self: Any, /):
        self.close()

    def connect(self: Any, /):
        super().connect() # type: ignore
        set_keepalive(self.sock)

    @property
    def response(self: Any, /) -> None | HTTPResponse:
        return self._HTTPConnection__response

    @property
    def state(self: Any, /) -> str:
        return self._HTTPConnection__state

    def getresponse(self: Any, /) -> HTTPResponse:
        return cast(HTTPResponse, super().getresponse()) # type: ignore

    def putrequest(self: Any, /, *args, **kwds):
        excs: list[Exception] = []
        for _ in range(5):
            try:
                return super().putrequest(*args, **kwds) # type: ignore
            except (ConnectionResetError, BrokenPipeError, ImproperConnectionState) as e:
                excs.append(e)
                self.close()
        raise ExceptionGroup("too many retries", excs)

    def set_tunnel(self: Any, /, host=None, port=None, headers=None):
        has_sock = self.sock is not None
        if not host:
            if self._tunnel_host:
                if has_sock:
                    self.close()
                self._tunnel_host = self._tunnel_port = None
                self._tunnel_headers.clear()
        elif (self._tunnel_host, self._tunnel_port) != self._get_hostport(host, port):
            if has_sock:
                self.close()
            super().set_tunnel(host, port, headers) # type: ignore


class HTTPConnection(HTTPConnectionMixin, BaseHTTPConnection):
    response_class = HTTPResponse


class HTTPSConnection(HTTPConnectionMixin, BaseHTTPSConnection):
    response_class = HTTPResponse


class ConnectionPool:

    def __init__(
        self, 
        /, 
        pool: None | defaultdict[str, deque[HTTPConnection] | deque[HTTPSConnection]] = None, 
    ):
        if pool is None:
            pool = defaultdict(deque)
        self.pool = pool

    def __del__(self, /):
        for dq in self.pool.values():
            for con in dq:
                con.close()

    def __repr__(self, /) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}({self.pool!r})"

    def get_connection(
        self, 
        /, 
        url: str | ParseResult | SplitResult, 
        timeout: None | float = None, 
    ) -> HTTPConnection | HTTPSConnection:
        if isinstance(url, str):
            url = urlsplit(url)
        assert url.scheme, "not a complete URL"
        host = url.hostname or "localhost"
        if is_ipv6(host):
            host = f"[{host}]"
        port = url.port or (443 if url.scheme == 'https' else 80)
        origin = f"{url.scheme}://{host}:{port}"
        dq = self.pool[origin]
        while True:
            try:
                con = dq.popleft()
            except IndexError:
                break
            con.timeout = timeout
            sock = con.sock
            resp = con.response
            if con.state == "Idle" or not sock:
                pass
            elif con.state == "Request-sent" or getattr(sock, "_closed"):
                con.close()
            elif resp and 0 < resp.unbuffer_size <= 1024 * 1024:
                try:
                    resp.read()
                except (ConnectionResetError, BrokenPipeError):
                    con.close()
            else:
                try:
                    sock.setblocking(False)
                except OSError:
                    con.close()
                else:
                    try:
                        if Socket.recv(sock, 1):
                            con.close()
                    except BlockingIOError:
                        pass
                    except (ConnectionResetError, BrokenPipeError):
                        con.close()
                    try:
                        if not getattr(sock, "_closed"):
                            sock.setblocking(True)
                    except OSError:
                        pass
            return con
        if url.scheme == "https":
            return HTTPSConnection(url.hostname or "localhost", url.port, timeout=timeout)
        else:
            return HTTPConnection(url.hostname or "localhost", url.port, timeout=timeout)

    def return_connection(
        self, 
        con: HTTPConnection | HTTPSConnection, 
        /, 
    ) -> str:
        if isinstance(con, HTTPSConnection):
            scheme = "https"
        else:
            scheme = "http"
        host = con.host
        if is_ipv6(host):
            host = f"[{host}]"
        origin = f"{scheme}://{host}:{con.port}"
        self.pool[origin].append(con) # type: ignore
        return origin

    _put_conn = return_connection


CONNECTION_POOL = ConnectionPool()


@overload
def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    proxies: None | str | dict[str, str] = None, 
    pool: None | Undefined | ConnectionPool = undefined, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> HTTPResponse:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    proxies: None | str | dict[str, str] = None, 
    pool: None | Undefined | ConnectionPool = undefined, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    proxies: None | str | dict[str, str] = None, 
    pool: None | Undefined | ConnectionPool = undefined, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request[T](
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    proxies: None | str | dict[str, str] = None, 
    pool: None | Undefined | ConnectionPool = undefined,  
    *, 
    parse: Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T], 
    **request_kwargs, 
) -> T:
    ...
def request[T](
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    proxies: None | str | dict[str, str] = None, 
    pool: None | Undefined | ConnectionPool = undefined, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T] = None, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    if pool is undefined:
        if proxies:
            pool = None
        else:
            pool = CONNECTION_POOL
    pool = cast(None | ConnectionPool, pool)
    if isinstance(proxies, str):
        http_proxy = https_proxy = get_host_pair(proxies)
    elif isinstance(proxies, dict):
        http_proxy = get_host_pair(proxies.get("http"))
        https_proxy = get_host_pair(proxies.get("https"))
    else:
        http_proxy = https_proxy = None
    body: Any
    if isinstance(data, PathLike):
        data = open(data, "rb")
    if isinstance(data, SupportsRead):
        request_args = normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            headers=headers, 
            ensure_ascii=True, 
        )
        body = data
    else:
        request_args = normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            files=files, 
            json=json, 
            headers=headers, 
            ensure_ascii=True, 
        )
        body = request_args["data"]
    method   = request_args["method"]
    url      = request_args["url"]
    headers_ = request_args["headers"]
    headers_.setdefault("connection", "keep-alive")
    need_set_cookie = "cookie" not in headers_
    response_cookies = CookieJar()
    connection: HTTPConnection | HTTPSConnection
    while True:
        if need_set_cookie:
            if cookies:
                headers_["cookie"] = cookies_to_str(cookies, url)
            elif response_cookies:
                headers_["cookie"] = cookies_to_str(response_cookies, url)
        urlp = urlsplit(url)
        request_kwargs["host"] = urlp.hostname or "localhost"
        request_kwargs["port"] = urlp.port
        if pool:
            connection = pool.get_connection(urlp, timeout=request_kwargs.get("timeout"))
        elif urlp.scheme == "https":
            connection = HTTPSConnection(**dict(get_all_items(request_kwargs, *HTTPS_CONNECTION_KWARGS)))
        else:
            connection = HTTPConnection(**dict(get_all_items(request_kwargs, *HTTP_CONNECTION_KWARGS)))
        if urlp.scheme == "https":
            if https_proxy:
                connection.set_tunnel(*https_proxy)
            elif pool:
                connection.set_tunnel()
        elif http_proxy:
            connection.set_tunnel(*http_proxy)
        elif pool:
            connection.set_tunnel()
        connection.request(
            method, 
            urlunsplit(urlp._replace(scheme="", netloc="")), 
            body, 
            headers_, 
        )
        response = connection.getresponse()
        if pool and headers_.get("connection") == "keep-alive":
            setattr(response, "pool", pool)
        setattr(response, "connection", connection)
        setattr(response, "method", method)
        setattr(response, "url", url)
        setattr(response, "cookies", response_cookies)
        extract_cookies(response_cookies, url, response)
        if cookies is not None:
            extract_cookies(cookies, url, response) # type: ignore
        status_code = response.status
        if 300 <= status_code < 400 and follow_redirects:
            if location := response.headers.get("location"):
                url = request_args["url"] = urljoin(url, location)
                if body and status_code in (307, 308):
                    if isinstance(body, SupportsRead):
                        try:
                            body.seek(0) # type: ignore
                        except Exception:
                            warn(f"unseekable-stream: {body!r}")
                    elif not isinstance(body, Buffer):
                        warn(f"failed to resend request body: {body!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        method = "GET"
                    body = None
                continue
        elif status_code >= 400 and raise_for_status:
            raise HTTPError(
                url, 
                status_code, 
                response.reason, 
                response.headers, 
                response, 
            )
        if parse is None:
            return response
        elif parse is ...:
            response.close()
            return response
        if isinstance(parse, bool):
            content = decompress_response(response.read(), response)
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[HTTPResponse], T], parse)(response)
        else:
            content = decompress_response(response.read(), response)
            return cast(Callable[[HTTPResponse, bytes], T], parse)(
                response, content)

# TODO: 实现异步请求，非阻塞模式(sock.setblocking(False))，对于响应体的数据加载，使用 select 模块进行通知
