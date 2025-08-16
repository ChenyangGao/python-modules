#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 4)
__all__ = ["urlopen", "request", "download"]

from collections import defaultdict, deque, UserString
from collections.abc import Buffer, Callable, Generator, Iterable, Mapping
from copy import copy
from http.client import HTTPConnection, HTTPSConnection, HTTPResponse
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
from inspect import isgenerator
from os import fsdecode, fstat, makedirs, PathLike
from os.path import abspath, dirname, isdir, join as joinpath
from shutil import COPY_BUFSIZE # type: ignore
from socket import socket
from ssl import SSLContext, _create_unverified_context
from types import EllipsisType
from typing import cast, overload, Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, ParseResult, SplitResult
from urllib.request import (
    build_opener, AbstractHTTPHandler, BaseHandler, HTTPHandler, 
    HTTPSHandler, HTTPRedirectHandler, OpenerDirector, Request, 
)

from argtools import argcount
from cookietools import cookies_to_str, extract_cookies, update_cookies
from dicttools import iter_items
from filewrap import bio_skip_iter, bio_chunk_iter, SupportsRead, SupportsWrite
from http_request import normalize_request_args, SupportsGeturl
from http_response import (
    decompress_response, get_filename, get_length, is_chunked, is_range_request, 
    parse_response, 
)
from property import locked_cacheproperty
from yarl import URL
from undefined import undefined, Undefined


type string = Buffer | str | UserString

if "__del__" not in HTTPConnection.__dict__:
    setattr(HTTPConnection, "__del__", HTTPConnection.close)
if "__del__" not in HTTPSConnection.__dict__:
    setattr(HTTPSConnection, "__del__", HTTPSConnection.close)
if "__del__" not in HTTPResponse.__dict__:
    setattr(HTTPResponse, "__del__", HTTPResponse.close)
if "__del__" not in OpenerDirector.__dict__:
    setattr(OpenerDirector, "__del__", OpenerDirector.close)

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


def is_ipv6(host: str, /) -> bool:
    from ipaddress import _BaseV6, AddressValueError
    try:
        _BaseV6._ip_int_from_string(host) # type: ignore
        return True
    except AddressValueError:
        return False


class HTTPCookieProcessor(BaseHandler):

    def __init__(
        self, 
        /, 
        cookies: None | CookieJar | SimpleCookie = None, 
    ):
        if cookies is None:
            cookies = CookieJar()
        self.cookies = cookies

    def http_request(self, request):
        cookies = self.cookies
        if cookies:
            if isinstance(cookies, SimpleCookie):
                cookies = update_cookies(CookieJar(), cookies)
            cookies.add_cookie_header(request)
        return request

    def http_response(self, request, response):
        extract_cookies(self.cookies, request.full_url, response) # type: ignore
        return response

    https_request = http_request
    https_response = http_response


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


class KeepAliveBaseHTTPHandler(AbstractHTTPHandler):

    @locked_cacheproperty
    def pool(self, /) -> ConnectionPool:
        return ConnectionPool()

    def do_open(self, /, http_class, req, **http_conn_args) -> HTTPResponse:
        host = req.host
        if not host:
            raise URLError("no host given")
        pool = self.pool
        if issubclass(http_class, HTTPSHandler):
            origin = "https://" + host
        else:
            origin = "http://" + host
        h = pool.get_connection(origin, timeout=req.timeout)
        h.set_debuglevel(self._debuglevel) # type: ignore
        headers = dict(req.unredirected_hdrs)
        headers.update({k: v for k, v in req.headers.items()
                        if k not in headers})
        headers.setdefault("connection", "keep-alive")
        if req._tunnel_host:
            tunnel_headers = {}
            proxy_auth_hdr = "Proxy-Authorization"
            if proxy_auth_hdr in headers:
                tunnel_headers[proxy_auth_hdr] = headers[proxy_auth_hdr]
                del headers[proxy_auth_hdr]
            h.set_tunnel(req._tunnel_host, headers=tunnel_headers)
        try:
            try:
                h.request(req.get_method(), req.selector, req.data, headers,
                          encode_chunked=req.has_header('Transfer-encoding'))
            except OSError as err:
                raise URLError(err)
            r = h.getresponse()
        except:
            pool.return_connection(h)
            raise
        r.url = req.get_full_url()
        r.msg = r.reason
        if headers.get("connection") == "keep-alive":
            setattr(r, "pool", pool)
        setattr(r, "connection", h)
        return r


class KeepAliveHTTPHandler(HTTPHandler, KeepAliveBaseHTTPHandler):
    pass


class KeepAliveHTTPSHandler(HTTPSHandler, KeepAliveBaseHTTPHandler):
    pass


_pool = ConnectionPool()
_http_handler = KeepAliveHTTPHandler()
_http_handler.pool = _pool
_https_handler = KeepAliveHTTPSHandler(context=_create_unverified_context())
_https_handler.pool = _pool
_cookies = CookieJar()
_opener: OpenerDirector = build_opener(
    _http_handler, 
    _https_handler, 
    HTTPCookieProcessor(_cookies), 
)
setattr(_opener, "cookies", _cookies)


class NoRedirectHandler(HTTPRedirectHandler):

    def redirect_request(self, /, *args, **kwds):
        return None


def urlopen(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    proxies: None | Mapping[str, str] | Iterable[tuple[str, str]] = None, 
    context: None | SSLContext = None, 
    cookies: None | CookieJar | SimpleCookie = None, 
    timeout: None | Undefined | float = undefined, 
    opener: None | OpenerDirector = _opener, 
    **_, 
) -> HTTPResponse:
    if isinstance(url, Request):
        request = url
    else:
        if isinstance(data, PathLike):
            data = bio_chunk_iter(open(data, "rb"))
        elif isinstance(data, SupportsRead):
            data = bio_chunk_iter(data)
        request = Request(**normalize_request_args( # type: ignore
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            ensure_ascii=True, 
        ))
        if proxies:
            for host, type in iter_items(proxies):
                request.set_proxy(host, type)
    headers_ = request.headers
    if opener is None:
        handlers: list[BaseHandler] = []
        if cookies is None:
            cookies = CookieJar()
    else:
        handlers = list(map(copy, getattr(opener, "handlers")))
        if cookies is None:
            cookies = getattr(opener, "cookies", None)
    if cookies and "cookie" not in headers_:
        headers_["cookie"] = cookies_to_str(cookies)
    if context is None:
        if opener is None:
            handlers.append(copy(_https_handler))
        else:
            for i, handler in enumerate(handlers):
                if isinstance(handler, KeepAliveHTTPSHandler):
                    break
                elif isinstance(handler, HTTPSHandler):
                    handlers[i] = copy(_https_handler)
                    break
            else:
                handlers.append(copy(_https_handler))
    else:
        https_handler = KeepAliveHTTPSHandler(context=context)
        https_handler.pool = _pool
        if opener is None:
            handlers.append(https_handler)
        else:
            for i, handler in enumerate(handlers):
                if isinstance(handler, HTTPSHandler):
                    handlers[i] = https_handler
                    break
            else:
                handlers.append(https_handler)
    if opener is None:
        handlers.append(copy(_http_handler))
    else:
        for i, handler in enumerate(handlers):
            if isinstance(handler, KeepAliveHTTPHandler):
                break
            elif isinstance(handler, HTTPHandler):
                handlers[i] = copy(_http_handler)
                break
        else:
            handlers.append(copy(_http_handler))
    if cookies and (opener is None or all(
        h.cookies is not cookies 
        for h in getattr(opener, "handlers") if isinstance(h, HTTPCookieProcessor)
    )):
        handlers.append(HTTPCookieProcessor(cookies))
    response_cookies = CookieJar()
    if cookies is None:
        cookies = response_cookies
    handlers.append(HTTPCookieProcessor(response_cookies))
    if not follow_redirects:
        handlers.append(NoRedirectHandler())
    opener = build_opener(*handlers)
    setattr(opener, "cookies", cookies)
    try:
        if timeout is undefined:
            response = opener.open(request)
        else:
            response = opener.open(request, timeout=cast(None | float, timeout))
        setattr(response, "opener", opener)
        setattr(response, "cookies", response_cookies)
        return response
    except HTTPError as e:
        if response := getattr(e, "file", None):
            setattr(response, "opener", opener)
            setattr(response, "cookies", response_cookies)
        raise


@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> HTTPResponse:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    *, 
    parse: Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T], 
    **request_kwargs, 
) -> T:
    ...
def request[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T] = None, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    try:
        response = urlopen(
            url=url, 
            method=method, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            follow_redirects=follow_redirects, 
            cookies=cookies, 
            **request_kwargs, 
        )
    except HTTPError as e:
        if raise_for_status:
            raise
        response = getattr(e, "file")
    if parse is None:
        return response
    elif parse is ...:
        response.close()
        return response
    with response:
        if isinstance(parse, bool):
            data = decompress_response(response.read(), response)
            if parse:
                return parse_response(response, data)
            return data
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[HTTPResponse], T], parse)(response)
        else:
            data = decompress_response(response.read(), response)
            return cast(Callable[[HTTPResponse, bytes], T], parse)(response, data)


def download(
    url: string | SupportsGeturl | URL | Request, 
    file: bytes | str | PathLike | SupportsWrite[bytes] = "", 
    resume: bool = False, 
    chunksize: int = COPY_BUFSIZE, 
    headers: None | Mapping[str, str] | Iterable[tuple[str, str]] = None, 
    make_reporthook: None | Callable[[None | int], Callable[[int], Any] | Generator[int, Any, Any]] = None, 
    **request_kwargs, 
) -> str | SupportsWrite[bytes]:
    """Download a URL into a file.

    Example::

        1. use `make_reporthook` to show progress:

            You can use the following function to show progress for the download task

            .. code: python

                from time import perf_counter

                def progress(total=None):
                    read_num = 0
                    start_t = perf_counter()
                    while True:
                        read_num += yield
                        speed = read_num / 1024 / 1024 / (perf_counter() - start_t)
                        print(f"\r\x1b[K{read_num} / {total} | {speed:.2f} MB/s", end="", flush=True)

            Or use the following function for more real-time speed

            .. code: python

                from collections import deque
                from time import perf_counter
    
                def progress(total=None):
                    dq = deque(maxlen=64)
                    read_num = 0
                    dq.append((read_num, perf_counter()))
                    while True:
                        read_num += yield
                        cur_t = perf_counter()
                        speed = (read_num - dq[0][0]) / 1024 / 1024 / (cur_t - dq[0][1])
                        print(f"\r\x1b[K{read_num} / {total} | {speed:.2f} MB/s", end="", flush=True)
                        dq.append((read_num, cur_t))
    """
    if chunksize <= 0:
        chunksize = COPY_BUFSIZE
    headers = request_kwargs["headers"] = dict(headers or ())
    headers["accept-encoding"] = "identity"
    response: HTTPResponse = urlopen(url, **request_kwargs)
    content_length = get_length(response)
    if content_length == 0 and is_chunked(response):
        content_length = None
    fdst: SupportsWrite[bytes]
    if hasattr(file, "write"):
        file = fdst = cast(SupportsWrite[bytes], file)
    else:
        file = abspath(fsdecode(file))
        if isdir(file):
            file = joinpath(file, get_filename(response, "download"))
        try:
            fdst = open(file, "ab" if resume else "wb")
        except FileNotFoundError:
            makedirs(dirname(file), exist_ok=True)
            fdst = open(file, "ab" if resume else "wb")
    filesize = 0
    if resume:
        try:
            fileno = getattr(fdst, "fileno")()
            filesize = fstat(fileno).st_size
        except (AttributeError, OSError):
            pass
        else:
            if filesize == content_length:
                return file
            if filesize and is_range_request(response):
                if filesize == content_length:
                    return file
            elif content_length is not None and filesize > content_length:
                raise OSError(
                    5, # errno.EIO
                    f"file {file!r} is larger than url {url!r}: {filesize} > {content_length} (in bytes)", 
                )
    reporthook_close: None | Callable = None
    if callable(make_reporthook):
        reporthook = make_reporthook(content_length)
        if isgenerator(reporthook):
            reporthook_close = reporthook.close
            next(reporthook)
            reporthook = reporthook.send
        else:
            reporthook_close = getattr(reporthook, "close", None)
        reporthook = cast(Callable[[int], Any], reporthook)
    else:
        reporthook = None
    try:
        if filesize:
            if is_range_request(response):
                response.close()
                response = urlopen(url, headers={**headers, "Range": "bytes=%d-" % filesize}, **request_kwargs)
                if not is_range_request(response):
                    raise OSError(
                        5, # errno.EIO
                        f"range request failed: {url!r}", 
                    )
                if reporthook is not None:
                    reporthook(filesize)
            elif resume:
                for _ in bio_skip_iter(response, filesize, callback=reporthook):
                    pass

        fsrc_read = response.read 
        fdst_write = fdst.write
        while (chunk := fsrc_read(chunksize)):
            fdst_write(chunk)
            if reporthook is not None:
                reporthook(len(chunk))
    finally:
        response.close()
        if callable(reporthook_close):
            reporthook_close()
    return file

