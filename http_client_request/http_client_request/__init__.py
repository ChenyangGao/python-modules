#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 5)
__all__ = ["ConnectionPool", "request"]

from collections import defaultdict, deque, UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from http.client import HTTPConnection, HTTPSConnection, HTTPResponse
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
from inspect import signature
from os import PathLike
from socket import socket
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
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

HTTP_CONNECTION_KWARGS: Final = signature(HTTPConnection).parameters.keys()
HTTPS_CONNECTION_KWARGS: Final = signature(HTTPSConnection).parameters.keys()

if "__del__" not in HTTPConnection.__dict__:
    setattr(HTTPConnection, "__del__", HTTPConnection.close)
if "__del__" not in HTTPSConnection.__dict__:
    setattr(HTTPSConnection, "__del__", HTTPSConnection.close)
if "__del__" not in HTTPResponse.__dict__:
    setattr(HTTPResponse, "__del__", HTTPResponse.close)

def _close_conn(self, /):
    fp = self.fp
    self.fp = None
    pool = getattr(self, "pool", None)
    conn = getattr(self, "connection", None)
    if pool and conn:
        try:
            pool.return_connection(conn)
        except NameError:
            pass
    else:
        fp.close()

setattr(HTTPResponse, "_close_conn", _close_conn)


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
            sock = con.sock
            if not sock or getattr(sock, "_closed"):
                con.connect()
            else:
                sock.setblocking(False)
                try:
                    if socket.recv(sock, 1):
                        con.connect()
                except BlockingIOError:
                    pass
                finally:
                    sock.setblocking(True)
            con.timeout = timeout
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
    cookies: None | CookieJar | SimpleCookie = None, 
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
    cookies: None | CookieJar | SimpleCookie = None, 
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
    cookies: None | CookieJar | SimpleCookie = None, 
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
    cookies: None | CookieJar | SimpleCookie = None, 
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
    cookies: None | CookieJar | SimpleCookie = None, 
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
            if http_proxy:
                connection.set_tunnel(*http_proxy)
        else:
            connection = HTTPConnection(**dict(get_all_items(request_kwargs, *HTTP_CONNECTION_KWARGS)))
            if https_proxy:
                connection.set_tunnel(*https_proxy)
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
