#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__all__ = ["HTTPError", "request"]

from asyncio import to_thread
from collections import UserString
from collections.abc import (
    AsyncIterable, Awaitable, Buffer, Callable, Iterable, 
    Iterator, Mapping, 
)
from http.client import HTTPConnection, HTTPSConnection, HTTPResponse
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable, isgeneratorfunction
from os import PathLike
from sys import exc_info
from types import EllipsisType
from typing import cast, overload, Any, Literal
from urllib.parse import urljoin, urlsplit, urlunsplit
from warnings import warn

from argtools import argcount
from cookietools import update_cookies, cookies_to_str
from ensure import ensure_bytes, ensure_str
from dicttools import iter_items
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from http_response import (
    get_status_code, headers_get, decompress_response, parse_response, 
)
from yarl import URL

from .. import normalize_request_args, SupportsGeturl


type string = Buffer | str | UserString


def get_headers(response, /):
    if hasattr(response, "headers"):
        headers = response.headers
        if isinstance(headers, (Mapping, Iterable)):
            return headers
        if callable(headers):
            headers = headers()
        return headers
    elif hasattr(response, "getheaders"):
        return response.getheaders()
    elif hasattr(response, "info"):
        return response.info()
    raise TypeError("can't read response headers")


def call_read(response, /):
    if hasattr(response, "read"):
        return response.read()
    maybe_content_attrs = (
        "content", "body", "iter_content", "iter_chunks", 
        "iter_chunked", "iter_bytes", "iter_lines", 
    )
    if a := next((a for a in maybe_content_attrs if hasattr(response, a)), None):
        content = getattr(response, a)
        if callable(content):
            content = content()
        if isinstance(content, Iterator):
            content = b"".join(map(ensure_bytes, content))
        return content
    raise TypeError("can't read response body")


def call_close(response, /):
    try:
        if hasattr(response, "close"):
            return response.close()
        elif hasattr(response, "release"):
            return response.release()
        elif hasattr(response, "__exit__"):
            return response.__exit__(**exc_info())
        elif hasattr(response, "__del__"):
            return response.__del__()
    except Exception:
        pass


async def call_async_read(response, /):
    if hasattr(response, "aread"):
        return await response.aread()
    elif hasattr(response, "read"):
        ret = response.read()
        if isawaitable(ret):
            ret = await ret
        return ret
    maybe_content_attrs = (
        "async_content", "async_body", "async_iter_content", "async_iter_chunks", 
        "async_iter_chunked", "async_iter_bytes", "async_iter_lines", 
        "acontent", "abody", "aiter_content", "aiter_chunks", "aiter_chunked", 
        "aiter_bytes", "aiter_lines", 
        "content", "body", "iter_content", "iter_chunks", "iter_chunked", 
        "iter_bytes", "iter_lines", 
    )
    if a := next((a for a in maybe_content_attrs if hasattr(response, a)), None):
        content = getattr(response, a)
        if callable(content):
            content = content()
        if isawaitable(content):
            content = await content
        if isinstance(content, AsyncIterable):
            data = bytearray()
            async for chunk in content:
                data += chunk
            content = data
        elif isinstance(content, Iterator):
            content = b"".join(map(ensure_bytes, content))
        return content
    raise TypeError("can't read response body")


async def call_async_close(response, /):
    try:
        if hasattr(response, "aclose"):
            return await response.aclose()
        elif hasattr(response, "close"):
            ret = response.close()
            if isawaitable(ret):
                ret = await ret
            return ret
        elif hasattr(response, "async_release"):
            return await response.async_release()
        elif hasattr(response, "release"):
            ret = response.release()
            if isawaitable(ret):
                ret = await ret
            return ret
        elif hasattr(response, "__aexit__"):
            return await response.__aexit__(**exc_info())
        elif hasattr(response, "__exit__"):
            return response.__exit__(**exc_info())
        elif hasattr(response, "__del__"):
            return response.__del__()
    except Exception:
        pass


def urlopen(
    url: str, 
    method: str = "GET", 
    data=None, 
    headers=None, 
    **request_args, 
) -> HTTPResponse:
    urlp = urlsplit(url)
    if urlp.scheme == "https":
        con: HTTPConnection = HTTPSConnection(urlp.netloc)
    else:
        con = HTTPConnection(urlp.netloc)
    con.request(
        method, 
        urlunsplit(urlp._replace(scheme="", netloc="")), 
        data, 
        headers, 
    )
    return con.getresponse()


urlopen_async = lambda *a, **k: to_thread(urlopen, *a, **k)


class HTTPError(OSError):

    def __init__(
        self, 
        /, 
        *args, 
        url: str, 
        status: int, 
        method: str, 
        response, 
    ):
        super().__init__(*args)
        self.url = url
        self.status = status
        self.method = method
        self.response = response

    def __repr__(self, /):
        return f"{type(self).__module__}.{type(self).__qualname__}({self})"

    def __str__(self):
        args = ",".join(map(repr, self.args))
        url = self.url
        status = self.status
        method = self.method
        response = self.response
        kwargs = f"{url=!r}, {status=!r}, {method=!r}, {response=!r}"
        if args:
            args += kwargs
        else:
            args = kwargs
        return args


@overload
def request_sync[Response](
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
    urlopen: Callable[..., Response] = urlopen, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> Response:
    ...
@overload
def request_sync[Response](
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
    urlopen: Callable[..., Response] = urlopen, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request_sync[Response](
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
    urlopen: Callable[..., Response] = urlopen, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request_sync[Response, T](
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
    urlopen: Callable[..., Response] = urlopen, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response], T], 
    **request_kwargs, 
) -> T:
    ...
def request_sync[Response, T](
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
    urlopen: Callable[..., Response] = urlopen, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType| bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    if isinstance(data, PathLike):
        data = open(data, "rb")
    body = data
    request_kwargs.update(normalize_request_args(
        method=method, 
        url=url, 
        params=params, 
        data=data, 
        json=json, 
        files=files, 
        headers=headers, 
        ensure_ascii=True, 
        ensure_bytes=True, 
    ))
    request_url: str = request_kwargs["url"]
    headers = cast(dict, request_kwargs["headers"])
    no_default_cookie_header = "cookie" not in headers
    response_cookies = CookieJar()
    while True:
        if no_default_cookie_header:
            headers["cookie"] = cookies_to_str(response_cookies if cookies is None else cookies, request_url)
        response: Response = urlopen(**request_kwargs)
        if hasattr(response, "cookies"):
            response_cookies = response.cookies
            if callable(response_cookies):
                response_cookies = response_cookies()
            if cookies is not None and response_cookies:
                update_cookies(cookies, response_cookies) # type: ignore
        else:
            setattr(response, "cookies", response_cookies)
            set_cookies: list[str] = []
            if response_headers := get_headers(response):
                set_cookies.extend( 
                    v for k, v in iter_items(response_headers) 
                    if v and ensure_str(k).lower() in ("set-cookie", "set-cookie2")
                )
            if set_cookies:
                base_cookies: BaseCookie = BaseCookie()
                for set_cookie in set_cookies:
                    base_cookies.load(set_cookie)
                if cookies is not None:
                    update_cookies(cookies, base_cookies) # type: ignore
                update_cookies(response_cookies, base_cookies)
        status_code = get_status_code(response)
        if status_code >= 400 and raise_for_status:
            raise HTTPError(
                url=request_kwargs["url"], 
                status=status_code, 
                method=request_kwargs["method"], 
                response=response, 
            )
        elif 300 <= status_code < 400 and follow_redirects:
            location = headers_get(response, "location")
            if location and not isinstance(location, (Buffer, UserString, str)):
                location = location[0]
            if location:
                location = ensure_str(location)
                request_url = request_kwargs["url"] = urljoin(request_url, location)
                if body and status_code in (307, 308):
                    if isinstance(body, SupportsRead):
                        try:
                            body.seek(0) # type: ignore
                            request_kwargs["data"] = bio_chunk_iter(body)
                        except Exception:
                            warn(f"unseekable-stream: {body!r}")
                    elif not isinstance(body, Buffer):
                        warn(f"failed to resend request body: {body!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        request_kwargs["method"] = "GET"
                    body = None
                    request_kwargs["data"] = None
                if request_kwargs["method"] != "HEAD":
                    call_read(response)
                call_close(response)
                del response
                continue
        if parse is None:
            return response
        try:
            if parse is ...:
                return response
            if isinstance(parse, bool):
                ac = 2
                if parse:
                    parse = cast(Callable[[Response, bytes], T], parse_response)
                else:
                    parse = lambda _, content: content
            else:
                ac = argcount(parse)
            if ac == 1:
                return cast(Callable[[Response], T], parse)(response)
            else:
                content = call_read(response)
                if not dont_decompress:
                    content = decompress_response(content, response)
                return cast(Callable[[Response, bytes], T], parse)(response, content)
        finally:
            call_close(response)


@overload
async def request_async[Response](
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
    urlopen: Callable[..., Response] = urlopen_async, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> Response:
    ...
@overload
async def request_async[Response](
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
    urlopen: Callable[..., Response] = urlopen_async, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request_async[Response](
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
    urlopen: Callable[..., Response] = urlopen_async, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request_async[Response, T](
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
    urlopen: Callable[..., Response] = urlopen_async, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response], T], 
    **request_kwargs, 
) -> T:
    ...
async def request_async[Response, T](
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
    urlopen: Callable[..., Response] = urlopen_async, # type: ignore
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType| bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    if isinstance(data, PathLike):
        data = open(data, "rb")
    body = data
    request_kwargs.update(normalize_request_args(
        method=method, 
        url=url, 
        params=params, 
        data=data, 
        json=json, 
        files=files, 
        headers=headers, 
        ensure_ascii=True, 
        ensure_bytes=True, 
    ))
    request_url: str = request_kwargs["url"]
    headers = cast(dict, request_kwargs["headers"])
    no_default_cookie_header = "cookie" not in headers
    response_cookies = CookieJar()
    while True:
        if no_default_cookie_header:
            headers["cookie"] = cookies_to_str(response_cookies if cookies is None else cookies, request_url)
        resp = urlopen(**request_kwargs)
        if isawaitable(resp):
            resp = await resp
        response: Response = resp
        if hasattr(response, "cookies"):
            response_cookies = response.cookies
            if callable(response_cookies):
                response_cookies = response_cookies()
            if cookies is not None and response_cookies:
                update_cookies(cookies, response_cookies) # type: ignore
        else:
            setattr(response, "cookies", response_cookies)
            set_cookies: list[str] = []
            if response_headers := get_headers(response):
                set_cookies.extend( 
                    v for k, v in iter_items(response_headers) 
                    if v and ensure_str(k).lower() in ("set-cookie", "set-cookie2")
                )
            if set_cookies:
                base_cookies: BaseCookie = BaseCookie()
                for set_cookie in set_cookies:
                    base_cookies.load(set_cookie)
                if cookies is not None:
                    update_cookies(cookies, base_cookies) # type: ignore
                update_cookies(response_cookies, base_cookies)
        status_code = get_status_code(response)
        if status_code >= 400 and raise_for_status:
            raise HTTPError(
                url=request_kwargs["url"], 
                status=status_code, 
                method=request_kwargs["method"], 
                response=response, 
            )
        elif 300 <= status_code < 400 and follow_redirects:
            location = headers_get(response, "location")
            if location and not isinstance(location, (Buffer, UserString, str)):
                location = location[0]
            if location:
                location = ensure_str(location)
                request_url = request_kwargs["url"] = urljoin(request_url, location)
                if body and status_code in (307, 308):
                    if isinstance(body, SupportsRead):
                        try:
                            from asynctools import ensure_async
                            await ensure_async(body.seek)(0) # type: ignore
                            request_kwargs["data"] = bio_chunk_async_iter(body)
                        except Exception:
                            warn(f"unseekable-stream: {body!r}")
                    elif not isinstance(body, Buffer):
                        warn(f"failed to resend request body: {body!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        request_kwargs["method"] = "GET"
                    body = None
                    request_kwargs["data"] = None
                if request_kwargs["method"] != "HEAD":
                    await call_async_read(response)
                await call_async_close(response)
                del response
                continue
        if parse is None:
            return response
        try:
            if parse is ...:
                return response
            if isinstance(parse, bool):
                ac = 2
                if parse:
                    parse = cast(Callable[[Response, bytes], T], parse_response)
                else:
                    parse = lambda _, content: content
            else:
                ac = argcount(parse)
            if ac == 1:
                return cast(Callable[[Response], T], parse)(response)
            else:
                content = await call_async_read(response)
                if not dont_decompress:
                    content = decompress_response(content, response)
                return cast(Callable[[Response, bytes], T], parse)(response, content)
        finally:
            await call_async_close(response)


@overload
def request[Response, T](
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
    urlopen: None | Callable[..., Response] = None, 
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    ...
@overload
def request[Response, T](
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
    urlopen: None | Callable[..., Response] = None, 
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    async_: Literal[True], 
    **request_kwargs, 
) -> Awaitable[Response | bytes | str | dict | list | int | float | bool | None | T]:
    ...
def request[Response, T](
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
    urlopen: None | Callable[..., Response] = None, 
    dont_decompress: None | bool = None, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T | Awaitable[Response | bytes | str | dict | list | int | float | bool | None | T]:
    if callable(urlopen):
        if isgeneratorfunction(urlopen):
            async_ = True
        request_kwargs["urlopen"] = urlopen
    if async_:
        return request_async(
            url=url, 
            method=method, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            follow_redirects=follow_redirects, 
            raise_for_status=raise_for_status, 
            cookies=cookies, 
            dont_decompress=dont_decompress, 
            parse=parse, # type: ignore 
            **request_kwargs, 
        )
    else:
        return request_sync(
            url=url, 
            method=method, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            follow_redirects=follow_redirects, 
            raise_for_status=raise_for_status, 
            cookies=cookies, 
            dont_decompress=dont_decompress, 
            parse=parse, # type: ignore  
            **request_kwargs, 
        )

