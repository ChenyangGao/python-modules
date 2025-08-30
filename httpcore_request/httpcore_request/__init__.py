#!/usr/bin/env python3
# coding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 6)
__all__ = [
    "ResponseWrapper", "HTTPStatusError", "request", 
    "request_sync", "request_async", 
]

from collections import UserString
from collections.abc import (
    AsyncIterable, AsyncIterator, Awaitable, Buffer, Callable, 
    Iterable, Iterator, Mapping, 
)
from contextlib import aclosing, closing
from copy import copy
from dataclasses import dataclass
from http import HTTPStatus
from http.client import HTTPMessage
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable, signature
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal
from urllib.parse import urljoin
from urllib.request import Request as HTTPRequest
from warnings import warn

from argtools import argcount
from cookietools import cookies_to_str
from dicttools import get_all_items
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import decompress_response, parse_response
from httpcore import AsyncConnectionPool, ConnectionPool, Request, Response
from httpcore._models import (
    enforce_bytes, enforce_headers, enforce_stream, enforce_url, 
    include_request_headers, 
)
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

_INIT_CLIENT_KWARGS: Final  = signature(ConnectionPool).parameters.keys()
_INIT_ASYNC_CLIENT_KWARGS: Final  = signature(AsyncConnectionPool).parameters.keys()

if "__del__" not in ConnectionPool.__dict__:
    setattr(ConnectionPool, "__del__", ConnectionPool.close)
if "close" not in AsyncConnectionPool.__dict__:
    def close(self, /):
        from asynctools import run_async
        run_async(self.aclose())
    setattr(AsyncConnectionPool, "close", close)
if "__del__" not in AsyncConnectionPool.__dict__:
    setattr(AsyncConnectionPool, "__del__", getattr(AsyncConnectionPool, "close"))
# if "__del__" not in Response.__dict__:
    # from httpcore._async.connection_pool import PoolByteStream as AsyncPoolByteStream
    # from httpcore._sync.connection_pool import PoolByteStream
    # def __del__(self, /):
    #     if stream := self.stream:
    #         if isinstance(stream, PoolByteStream):
    #             self.close()
    #         elif isinstance(stream, AsyncPoolByteStream):
    #             from asynctools import run_async
    #             return run_async(self.aclose())
    # setattr(Response, "__del__", __del__)

_DEFAULT_CLIENT = ConnectionPool(http2=True, max_connections=128, retries=5)
_DEFAULT_ASYNC_CLIENT = AsyncConnectionPool(http2=True, max_connections=128, retries=5)
_DEFAULT_COOKIE_JAR = CookieJar()
setattr(_DEFAULT_CLIENT, "cookies", _DEFAULT_COOKIE_JAR)
setattr(_DEFAULT_ASYNC_CLIENT, "cookies", _DEFAULT_COOKIE_JAR)


class ResponseWrapper:

    def __init__(self, request: Request, response: Response):
        self.request = request
        self.response = response
        headers = self.headers = HTTPMessage()
        for key, val in response.headers:
            headers.set_raw(str(key, "latin-1"), str(val, "latin-1"))

    def __del__(self, /):
        if stream := self.response.stream:
            if hasattr(stream, "aclose"):
                from asynctools import run_async
                run_async(stream.aclose())
            elif hasattr(stream, "close"):
                stream.close()

    def __dir__(self, /) -> list[str]:
        s = set(super().__dir__())
        s.update(dir(self.response))
        return sorted(s)

    def __getattr__(self, attr, /):
        return getattr(self.response, attr)

    def __repr__(self, /):
        return f"{type(self).__qualname__}({self.response!r})"

    @property
    def code(self, /) -> int:
        return self.response.status

    def info(self, /) -> HTTPMessage:
        return self.headers

    def is_redirect(self, /):
        return 300 <= self.response.status < 400

    def raise_for_status(self, /):
        status_code = self.response.status
        if status_code >= 400:
            request = self.request
            raise HTTPStatusError(
                code=status_code, 
                method=str(request.method, "ascii"), 
                url=str(bytes(request.url), "ascii"), 
                reason=HTTPStatus(status_code).phrase, 
                message=HTTPStatus(status_code).description, 
                headers=list(self.headers.items()), 
                request=self.request, 
                response=self, 
            )


@dataclass(frozen=True)
class HTTPStatusError(OSError):
    code: int
    method: str
    url: str
    reason: str
    message: str
    headers: dict[str, str] | list[tuple[str, str]]
    request: Request
    response: ResponseWrapper | Response

    @property
    def args(self, /) -> tuple: # type: ignore
        return tuple(getattr(self, a) for a in self.__match_args__)

    @property
    def kwargs(self, /) -> dict[str, Any]:
        return {a: getattr(self, a) for a in self.__match_args__}

    def __str__(self, /) -> str:
        return "".join(f"\n    {a}={getattr(self, a)!r}" for a in self.__match_args__)


@overload
def request_sync(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | ConnectionPool = _DEFAULT_CLIENT, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> ResponseWrapper:
    ...
@overload
def request_sync(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | ConnectionPool = _DEFAULT_CLIENT, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request_sync(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | ConnectionPool = _DEFAULT_CLIENT, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request_sync[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | ConnectionPool = _DEFAULT_CLIENT, 
    *, 
    parse: Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper], T], 
    **request_kwargs, 
) -> T:
    ...
def request_sync[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | ConnectionPool = _DEFAULT_CLIENT, 
    *, 
    parse: None | EllipsisType | bool | Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper], T] = None, 
    **request_kwargs, 
) -> ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T:
    if session is None:
        session = ConnectionPool(**dict(get_all_items(
            request_kwargs, *_INIT_CLIENT_KWARGS)))
        setattr(session, "cookies", CookieJar())
    if cookies is None:
        cookies = getattr(session, "cookies", None)
    if isinstance(data, PathLike):
        data = open(data, "rb")
    if isinstance(data, Buffer):
        data = bytes(data)
    body: None | bytes | Iterable[Buffer] | SupportsRead[bytes] = data
    if isinstance(url, Request):
        request = copy(url)
        if isinstance(data, SupportsRead):
            data = bio_chunk_iter(data)
        request.stream  = enforce_stream(data, name="content")
        request.headers = include_request_headers(request.headers, url=request.url, content=data)
    else:
        if isinstance(data, (Buffer, SupportsRead)):
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                headers=headers, 
                ensure_bytes=True, 
            )
        else:
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                data=data, 
                files=files, 
                json=json, 
                headers=headers, 
                ensure_bytes=True, 
            )
            body = data = cast(None | bytes | Iterator[Buffer], request_args["data"])
        request = Request(
            method=enforce_bytes(request_args["method"], name="method"), 
            url=enforce_url(request_args["url"], name="url"), 
            headers=enforce_headers(request_args["headers"], name="headers"), # type: ignore
            content=data, 
            extensions=request_kwargs.get("extensions"), 
        )
        request.headers = include_request_headers(request.headers, url=request.url, content=data)
    raw_headers = [(k, v) for k, v in request.headers if k.lower() != b"host"]
    request_url = bytes(request.url).decode("utf-8")
    no_default_cookie_header = True
    for keyb, _ in request.headers:
        if keyb.lower() == b"cookie":
            no_default_cookie_header = False
            break
    else:
        if cookies:
            cookie_bytes = bytes(cookies_to_str(cookies, request_url), "latin-1")
        else:
            cookie_bytes = b""
        request.headers.append((b"cookie", cookie_bytes))
    response_cookies = CookieJar()
    while True:
        response = ResponseWrapper(request, session.handle_request(request))
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        if cookies is not None:
            if isinstance(cookies, BaseCookie):
                for key, val in response.headers.items():
                    if val and key.lower() in ("set-cookie", "set-cookie2"):
                        cookies.load(val)
            else:
                cookies.extract_cookies(response, HTTPRequest(request_url)) # type: ignore
        response_cookies.extract_cookies(response, HTTPRequest(request_url)) # type: ignore
        status_code = response.status
        if follow_redirects and 300 <= status_code < 400:
            if location := response.headers.get("location"):
                request = copy(request)
                request_url = urljoin(request_url, location)
                request.url = enforce_url(request_url, name="url")
                if body and status_code in (307, 308):
                    if isinstance(body, SupportsRead):
                        try:
                            body.seek(0) # type: ignore
                            data = bio_chunk_iter(body)
                            request.stream = enforce_stream(data, name="content")
                        except Exception:
                            warn(f"unseekable-stream: {body!r}")
                    elif not isinstance(body, Buffer):
                        warn(f"failed to resend request body: {body!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        request.method = b"GET"
                    body = None
                    request.stream = enforce_stream(None, name="content")
                if no_default_cookie_header:
                    cookie_bytes = bytes(cookies_to_str(response_cookies if cookies is None else cookies, request_url), "latin-1")
                    request.headers[-1] = (b"cookie", cookie_bytes)
                request.headers = include_request_headers(raw_headers, url=request.url, content=None)
                if request.method != "HEAD":
                    response.read()
                response.close()
                continue
        elif raise_for_status:
            response.raise_for_status()
        if parse is None:
            return response
        elif parse is ...:
            response.close()
            return response
        with closing(response):
            if isinstance(parse, bool):
                content = decompress_response(response.read(), response)
                if parse:
                    return parse_response(response, content)
                return content
            ac = argcount(parse)
            if ac == 1:
                return cast(Callable[[ResponseWrapper], T], parse)(response)
            else:
                content = decompress_response(response.read(), response)
                return cast(Callable[[ResponseWrapper, bytes], T], parse)(response, content)


@overload
async def request_async(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncConnectionPool = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> ResponseWrapper:
    ...
@overload
async def request_async(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncConnectionPool = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request_async(
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncConnectionPool = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request_async[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncConnectionPool = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper, bytes], Awaitable[T]] | Callable[[ResponseWrapper], T] | Callable[[ResponseWrapper], Awaitable[T]], 
    **request_kwargs, 
) -> T:
    ...
async def request_async[T](
    url: string | SupportsGeturl | URL | Request, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncConnectionPool = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: None | EllipsisType | bool | Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper, bytes], Awaitable[T]] | Callable[[ResponseWrapper], T] | Callable[[ResponseWrapper], Awaitable[T]] = None, 
    **request_kwargs, 
) -> ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T:
    if session is None:
        session = AsyncConnectionPool(**dict(get_all_items(
            request_kwargs, *_INIT_ASYNC_CLIENT_KWARGS)))
        setattr(session, "cookies", CookieJar())
    if cookies is None:
        cookies = getattr(session, "cookies", None)
    if isinstance(data, PathLike):
        data = open(data, "rb")
    if isinstance(data, Buffer):
        data = bytes(data)
    body: None | bytes | Iterable[Buffer] | AsyncIterable[Buffer] | SupportsRead[bytes] = data
    if isinstance(url, Request):
        request = copy(url)
        if isinstance(data, SupportsRead):
            data = bio_chunk_async_iter(data)
        request.stream  = enforce_stream(data, name="content")
        request.headers = include_request_headers(request.headers, url=request.url, content=data)
    else:
        if isinstance(data, (Buffer, SupportsRead)):
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                headers=headers, 
                ensure_bytes=True, 
                async_=True, 
            )
        else:
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                data=data, 
                files=files, 
                json=json, 
                headers=headers, 
                ensure_bytes=True, 
                async_=True, 
            )
            body = data = cast(None | bytes | AsyncIterator[Buffer], request_args["data"])
        request = Request(
            method=enforce_bytes(request_args["method"], name="method"), 
            url=enforce_url(request_args["url"], name="url"), 
            headers=enforce_headers(request_args["headers"], name="headers"), # type: ignore
            content=data, 
            extensions=request_kwargs.get("extensions"), 
        )
        request.headers = include_request_headers(request.headers, url=request.url, content=data)
    raw_headers = [(k, v) for k, v in request.headers if k.lower() != b"host"]
    request_url = bytes(request.url).decode("utf-8")
    no_default_cookie_header = True
    for keyb, _ in request.headers:
        if keyb.lower() == b"cookie":
            no_default_cookie_header = False
            break
    else:
        if cookies:
            cookie_bytes = bytes(cookies_to_str(cookies, request_url), "latin-1")
        else:
            cookie_bytes = b""
        request.headers.append((b"cookie", cookie_bytes))
    response_cookies = CookieJar()
    while True:
        response = ResponseWrapper(request, await session.handle_async_request(request))
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        if cookies is not None:
            if isinstance(cookies, BaseCookie):
                for key, val in response.headers.items():
                    if val and key.lower() in ("set-cookie", "set-cookie2"):
                        cookies.load(val)
            else:
                cookies.extract_cookies(response, HTTPRequest(request_url)) # type: ignore
        response_cookies.extract_cookies(response, HTTPRequest(request_url)) # type: ignore
        status_code = response.status
        if follow_redirects and 300 <= status_code < 400:
            if location := response.headers.get("location"):
                request = copy(request)
                request_url = urljoin(request_url, location)
                request.url = enforce_url(request_url, name="url")
                if body and status_code in (307, 308):
                    if isinstance(body, SupportsRead):
                        try:
                            from asynctools import ensure_async
                            await ensure_async(body.seek)(0) # type: ignore
                            data = bio_chunk_async_iter(body)
                            request.stream = enforce_stream(data, name="content")
                        except Exception:
                            warn(f"unseekable-stream: {body!r}")
                    elif not isinstance(body, Buffer):
                        warn(f"failed to resend request body: {body!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        request.method = b"GET"
                    body = None
                    request.stream = enforce_stream(None, name="content")
                if no_default_cookie_header:
                    cookie_bytes = bytes(cookies_to_str(response_cookies if cookies is None else cookies, request_url), "latin-1")
                    request.headers[-1] = (b"cookie", cookie_bytes)
                request.headers = include_request_headers(raw_headers, url=request.url, content=None)
                if request.method != "HEAD":
                    await response.aread()
                await response.aclose()
                continue
        elif raise_for_status:
            response.raise_for_status()
        if parse is None:
            return response
        elif parse is ...:
            await response.aclose()
            return response
        async with aclosing(response):
            if isinstance(parse, bool):
                content = decompress_response(await response.aread(), response)
                if parse:
                    return parse_response(response, content)
                return content
            ac = argcount(parse)
            if ac == 1:
                ret = cast(Callable[[ResponseWrapper], T], parse)(response)
            else:
                content = decompress_response(await response.aread(), response)
                ret = cast(Callable[[ResponseWrapper, bytes], T], parse)(response, content)
            if isawaitable(ret):
                ret = await ret
            return ret


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
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Undefined | ConnectionPool = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper], T] = None, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T:
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
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Undefined | AsyncConnectionPool = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper, bytes], Awaitable[T]] | Callable[[ResponseWrapper], T] | Callable[[ResponseWrapper], Awaitable[T]] = None, 
    async_: Literal[True], 
    **request_kwargs, 
) -> Awaitable[ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T]:
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
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Undefined | ConnectionPool | AsyncConnectionPool = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper, bytes], Awaitable[T]] | Callable[[ResponseWrapper], T] | Callable[[ResponseWrapper], Awaitable[T]] = None, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T | Awaitable[ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T]:
    if async_:
        if session is undefined:
            session = _DEFAULT_ASYNC_CLIENT
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
            session=cast(None | AsyncConnectionPool, session), 
            parse=parse, # type: ignore 
            **request_kwargs, 
        )
    else:
        if session is undefined:
            session = _DEFAULT_CLIENT
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
            session=cast(None | ConnectionPool, session), 
            parse=parse, # type: ignore  
            **request_kwargs, 
        )

