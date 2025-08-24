#!/usr/bin/env python3
# coding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = ["request", "request_sync", "request_async"]

from collections import UserString
from collections.abc import (
    AsyncIterator, Awaitable, Buffer, Callable, Iterable, 
    Iterator, Mapping, 
)
from copy import copy
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable, signature
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal
from urllib.parse import urljoin
from warnings import warn

from argtools import argcount
from asynctools import ensure_async
from cookietools import cookies_to_str, update_cookies
from dicttools import get_all_items
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from tornado.httpclient import HTTPClient, AsyncHTTPClient, HTTPRequest, HTTPResponse
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

_REQUEST_KWARGS: Final = signature(HTTPRequest).parameters.keys()

if "__del__" not in AsyncHTTPClient.__dict__:
    setattr(AsyncHTTPClient, "__del__", AsyncHTTPClient.close)

_DEFAULT_CLIENT = HTTPClient()
_DEFAULT_ASYNC_CLIENT = AsyncHTTPClient()
_DEFAULT_COOKIE_JAR = CookieJar()
setattr(_DEFAULT_CLIENT, "cookies", _DEFAULT_COOKIE_JAR)
setattr(_DEFAULT_ASYNC_CLIENT, "cookies", _DEFAULT_COOKIE_JAR)


@overload
def request_sync(
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_CLIENT, 
    *, 
    parse: None = None, 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request_sync(
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_CLIENT, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> HTTPResponse:
    ...
@overload
def request_sync(
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_CLIENT, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request_sync[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_CLIENT, 
    *, 
    parse: Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T], 
    **request_kwargs, 
) -> T:
    ...
def request_sync[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_CLIENT, 
    *, 
    parse: None | EllipsisType | bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T] = None, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    def make_body_producer(it: Iterator, /):
        def do_write(write):
            for chunk in it:
                write(chunk)
        return do_write
    request_kwargs["follow_redirects"] = False
    if session is None:
        session = HTTPClient()
        setattr(session, "cookies", CookieJar())
    if cookies is None:
        cookies = getattr(session, "cookies", None)
    if isinstance(url, HTTPRequest):
        request = copy(url)
        data = None
    else:
        if isinstance(data, PathLike):
            data = open(data, "rb")
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
        request_kwargs.update(
            url=request_args["url"], 
            method=request_args["method"], 
            headers=request_args["headers"], 
        )
        body = cast(bytes | Iterator[bytes], request_args["data"])
        if body:
            if isinstance(body, bytes):
                request_kwargs["body"] = body
            else:
                request_kwargs["body_producer"] = make_body_producer(body)
        request = HTTPRequest(**dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
    request_url = request.url
    no_default_cookie_header = True
    for key in request.headers:
        if key.lower() == "cookie":
            no_default_cookie_header = False
            break
    else:
        request.headers["cookie"] = cookies_to_str(cookies, request_url) if cookies else ""
    response_cookies = CookieJar()
    while True:
        response = session.fetch(request, raise_error=False)
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        cur_response_cookies: BaseCookie = BaseCookie()
        for key, val in response.headers.get_all():
            if val and key.lower() in ("set-cookie", "set-cookie2"):
                cur_response_cookies.load(val)
        if cur_response_cookies:
            if cookies is not None:
                update_cookies(cookies, cur_response_cookies) # type: ignore
            update_cookies(response_cookies, cur_response_cookies)
        status_code = response.code
        if 300 <= status_code < 400 and follow_redirects:
            if location := response.headers.get("location"):
                request = copy(request)
                request_url = urljoin(request_url, location)
                if data and status_code in (307, 308):
                    if isinstance(data, SupportsRead):
                        try:
                            data.seek(0) # type: ignore
                            request.body_producer = make_body_producer(bio_chunk_iter(data))
                        except Exception:
                            warn(f"unseekable-stream: {data!r}")
                    elif not isinstance(data, Buffer):
                        warn(f"failed to resend request body: {data!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        request.method = "GET"
                    data = None
                    request.body = b""
                    request.body_producer = None
                if no_default_cookie_header:
                    request.headers["cookie"] = cookies_to_str(response_cookies if cookies is None else cookies, request_url)
                continue
        elif status_code >= 400 and raise_for_status:
            raise cast(Exception, response.error)
        if parse is None or parse is ...:
            return response
        if isinstance(parse, bool):
            content = response.body
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[HTTPResponse], T], parse)(response)
        else:
            return cast(Callable[[HTTPResponse, bytes], T], parse)(response, response.body)


@overload
async def request_async(
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncHTTPClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: None = None, 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request_async(
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncHTTPClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> HTTPResponse:
    ...
@overload
async def request_async(
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncHTTPClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request_async[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncHTTPClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse, bytes], Awaitable[T]] | Callable[[HTTPResponse], T] | Callable[[HTTPResponse], Awaitable[T]], 
    **request_kwargs, 
) -> T:
    ...
async def request_async[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncHTTPClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: None | EllipsisType | bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse, bytes], Awaitable[T]] | Callable[[HTTPResponse], T] | Callable[[HTTPResponse], Awaitable[T]] = None, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    def make_body_producer(it: AsyncIterator, /):
        async def do_write(write):
            async for chunk in it:
                write(chunk)
        return do_write
    request_kwargs["follow_redirects"] = False
    if session is None:
        session = AsyncHTTPClient()
        setattr(session, "cookies", CookieJar())
    if cookies is None:
        cookies = getattr(session, "cookies", None)
    if isinstance(url, HTTPRequest):
        request = copy(url)
        data = None
    else:
        if isinstance(data, PathLike):
            data = open(data, "rb")
        request_args = normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            files=files, 
            json=json, 
            headers=headers, 
            async_=True, 
            ensure_bytes=True, 
        )
        request_kwargs.update(
            url=request_args["url"], 
            method=request_args["method"], 
            headers=request_args["headers"], 
        )
        body = cast(bytes | AsyncIterator[bytes], request_args["data"])
        if body:
            if isinstance(body, bytes):
                request_kwargs["body"] = body
            else:
                request_kwargs["body_producer"] = make_body_producer(body)
        request = HTTPRequest(**dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
    request_url = request.url
    no_default_cookie_header = True
    for key in request.headers:
        if key.lower() == "cookie":
            no_default_cookie_header = False
            break
    else:
        request.headers["cookie"] = cookies_to_str(cookies, request_url) if cookies else ""
    response_cookies = CookieJar()
    while True:
        response = await session.fetch(request, raise_error=False)
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        cur_response_cookies: BaseCookie = BaseCookie()
        for key, val in response.headers.get_all():
            if val and key.lower() in ("set-cookie", "set-cookie2"):
                cur_response_cookies.load(val)
        if cur_response_cookies:
            if cookies is not None:
                update_cookies(cookies, cur_response_cookies) # type: ignore
            update_cookies(response_cookies, cur_response_cookies)
        status_code = response.code
        if 300 <= status_code < 400 and follow_redirects:
            if location := response.headers.get("location"):
                request = copy(request)
                request_url = urljoin(request_url, location)
                if data and status_code in (307, 308):
                    if isinstance(data, SupportsRead):
                        try:
                            await ensure_async(data.seek)(0) # type: ignore
                            request.body_producer = make_body_producer(bio_chunk_async_iter(data))
                        except Exception:
                            warn(f"unseekable-stream: {data!r}")
                    elif not isinstance(data, Buffer):
                        warn(f"failed to resend request body: {data!r}, when {status_code} redirects")
                else:
                    if status_code == 303:
                        request.method = "GET"
                    data = None
                    request.body = b""
                    request.body_producer = None
                if no_default_cookie_header:
                    request.headers["cookie"] = cookies_to_str(response_cookies if cookies is None else cookies, request_url)
                continue
        elif status_code >= 400 and raise_for_status:
            raise cast(Exception, response.error)
        if parse is None or parse is ...:
            return response
        if isinstance(parse, bool):
            content = response.body
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            ret = cast(Callable[[HTTPResponse], T], parse)(response)
        else:
            ret = cast(Callable[[HTTPResponse, bytes], T], parse)(response, response.body)
        if isawaitable(ret):
            ret = await ret
        return ret


@overload
def request[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Undefined | HTTPClient = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T] = None, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    ...
@overload
def request[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Undefined | AsyncHTTPClient = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse, bytes], Awaitable[T]] | Callable[[HTTPResponse], T] | Callable[[HTTPResponse], Awaitable[T]] = None, 
    async_: Literal[True], 
    **request_kwargs, 
) -> Awaitable[HTTPResponse | bytes | str | dict | list | int | float | bool | None | T]:
    ...
def request[T](
    url: string | SupportsGeturl | URL | HTTPRequest, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | Undefined | HTTPClient | AsyncHTTPClient = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse, bytes], Awaitable[T]] | Callable[[HTTPResponse], T] | Callable[[HTTPResponse], Awaitable[T]] = None, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T | Awaitable[HTTPResponse | bytes | str | dict | list | int | float | bool | None | T]:
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
            session=cast(None | AsyncHTTPClient, session), 
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
            session=cast(None | HTTPClient, session), 
            parse=parse, # type: ignore  
            **request_kwargs, 
        )

