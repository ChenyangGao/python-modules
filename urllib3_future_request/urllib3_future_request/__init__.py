#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 2)
__all__ = ["request_sync", "request_async", "request"]

from collections import UserString
from collections.abc import Awaitable, Buffer, Coroutine, Callable, Iterable, Mapping
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, IO, Literal
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request
from warnings import warn

from cookietools import (
    extract_cookies, cookies_dict_to_str, cookies_str_to_dict, cookie_header_for_url, 
)
from dicttools import dict_merge
from filewrap import SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response, get_length
from urllib3_future import PoolManager, AsyncPoolManager, HTTPResponse, AsyncHTTPResponse
from yarl import URL


type string = Buffer | str | UserString

if "__del__" not in PoolManager.__dict__:
    setattr(PoolManager, "__del__", PoolManager.clear)
if "close" not in AsyncPoolManager.__dict__:
    def close(self, /):
        from asynctools import run_async
        run_async(self.clear())
    setattr(AsyncPoolManager, "close", close)
if "__del__" not in AsyncPoolManager.__dict__:
    setattr(AsyncPoolManager, "close", getattr(AsyncPoolManager, "close"))

_DEFAULT_POOL = PoolManager(num_pools=64, maxsize=256)
_cookiejar = CookieJar()
setattr(_DEFAULT_POOL, "cookies", _cookiejar)
_DEFAULT_ASYNC_POOL = AsyncPoolManager(num_pools=64, maxsize=256)
setattr(_DEFAULT_ASYNC_POOL, "cookies", _cookiejar)
del _cookiejar


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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> HTTPResponse:
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
    *, 
    parse: Callable[[HTTPResponse, bytes], T], 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] = None, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["preload_content"] = not stream
    if session is None:
        session = PoolManager()
        if cookies is None:
            setattr(session, "cookies", CookieJar())
    body: Any
    if isinstance(url, Request):
        request  = url
        method   = request.method or "GET"
        url      = request.full_url
        data     = request.data
        if isinstance(data, PathLike):
            body = open(data, "rb")
        else:
            body = data
        headers_ = request.headers
    else:
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
    if cookies is None:
        cookies = getattr(session, "cookies", None)
    if cookies:
        cookies_dict = cookie_header_for_url(cookies, url)
    else:
        cookies_dict = {}
    if "cookie" in headers_:
        cookies_dict.update(cookies_str_to_dict(headers_["cookie"]))
    response_cookies = CookieJar()
    request_kwargs["redirect"] = False
    while True:
        if response_cookies:
            cookies_dict.update(cookie_header_for_url(response_cookies, url))
        headers_["cookie"] = cookies_dict_to_str(cookies_dict)
        response = cast(HTTPResponse, session.request(
            method=method, 
            url=url, 
            body=body, 
            headers=headers_, 
            **request_kwargs, 
        ))
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        setattr(response, "method", method)
        setattr(response, "url", url)
        if cookies is not None:
            extract_cookies(cookies, url, response) # type: ignore
        extract_cookies(response_cookies, url, response)
        status_code = response.status
        if redirect_location := follow_redirects and response.get_redirect_location():
            dict_merge(cookies_dict, ((cookie.name, cookie.value) for cookie in response_cookies))
            if cookies_dict:
                headers_["cookie"] = cookies_dict_to_str(cookies_dict)
            url = urljoin(url, redirect_location)
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
            response.drain_conn()
            continue
        elif raise_for_status and status_code >= 400:
            response.data
            raise HTTPError(
                url, 
                status_code, 
                response.reason or "", 
                response.headers, # type: ignore
                cast(IO[bytes], response), 
            )
        if parse is None:
            if method == "HEAD":
                response.drain_conn()
            return response
        elif parse is ...:
            try:
                if response.version < 20 and (
                    method == "HEAD" or 
                    (length := get_length(response)) is not None and length <= 10485760
                ):
                    response.drain_conn()
            finally:
                response.close()
            return response
        content = response.data
        if isinstance(parse, bool):
            if not parse:
                return content
            parse = cast(Callable, parse_response)
        return parse(response, content)


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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncPoolManager = _DEFAULT_ASYNC_POOL, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> AsyncHTTPResponse:
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncPoolManager = _DEFAULT_ASYNC_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncPoolManager = _DEFAULT_ASYNC_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncPoolManager = _DEFAULT_ASYNC_POOL, 
    *, 
    parse: Callable[[AsyncHTTPResponse, bytes], T] | Callable[[AsyncHTTPResponse, bytes], Awaitable[T]], 
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | AsyncPoolManager = _DEFAULT_ASYNC_POOL, 
    *, 
    parse: None | EllipsisType | bool | Callable[[AsyncHTTPResponse, bytes], T] | Callable[[AsyncHTTPResponse, bytes], Awaitable[T]] = None, 
    **request_kwargs, 
) -> AsyncHTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["preload_content"] = not stream
    if session is None:
        session = AsyncPoolManager()
        if cookies is None:
            setattr(session, "cookies", CookieJar())
    body: Any
    if isinstance(url, Request):
        request  = url
        method   = request.method or "GET"
        url      = request.full_url
        data     = request.data
        if isinstance(data, PathLike):
            body = open(data, "rb")
        else:
            body = data
        headers_ = request.headers
    else:
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
    if cookies is None:
        cookies = getattr(session, "cookies", None)
    if cookies:
        cookies_dict = cookie_header_for_url(cookies, url)
    else:
        cookies_dict = {}
    if "cookie" in headers_:
        cookies_dict.update(cookies_str_to_dict(headers_["cookie"]))
    response_cookies = CookieJar()
    request_kwargs["redirect"] = False
    while True:
        if response_cookies:
            cookies_dict.update(cookie_header_for_url(response_cookies, url))
        headers_["cookie"] = cookies_dict_to_str(cookies_dict)
        response = cast(AsyncHTTPResponse, await session.request(
            method=method, 
            url=url, 
            body=body, 
            headers=headers_, 
            **request_kwargs, 
        ))
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        setattr(response, "method", method)
        setattr(response, "url", url)
        if cookies is not None:
            extract_cookies(cookies, url, response) # type: ignore
        extract_cookies(response_cookies, url, response)
        status_code = response.status
        if redirect_location := follow_redirects and response.get_redirect_location():
            dict_merge(cookies_dict, ((cookie.name, cookie.value) for cookie in response_cookies))
            if cookies_dict:
                headers_["cookie"] = cookies_dict_to_str(cookies_dict)
            url = urljoin(url, redirect_location)
            if body and status_code in (307, 308):
                if isinstance(body, SupportsRead):
                    try:
                        from asynctools import ensure_async
                        await ensure_async(body.seek)(0) # type: ignore
                    except Exception:
                        warn(f"unseekable-stream: {body!r}")
                elif not isinstance(body, Buffer):
                    warn(f"failed to resend request body: {body!r}, when {status_code} redirects")
            else:
                if status_code == 303:
                    method = "GET"
                body = None
            await response.drain_conn()
            continue
        elif raise_for_status and status_code >= 400:
            await response.data
            raise HTTPError(
                url, 
                status_code, 
                response.reason or "", 
                response.headers, # type: ignore
                cast(IO[bytes], response), 
            )
        if parse is None:
            if method == "HEAD":
                await response.drain_conn()
            return response
        elif parse is ...:
            try:
                if response.version < 20 and (
                    method == "HEAD" or 
                    (length := get_length(response)) is not None and length <= 10485760
                ):
                    await response.drain_conn()
            finally:
                await response.close()
            return response
        content = await response.data
        if isinstance(parse, bool):
            if not parse:
                return content
            parse = cast(Callable, parse_response)
        ret = parse(response, content)
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] = None, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T:
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    *, 
    parse: None | EllipsisType| bool | Callable[[AsyncHTTPResponse, bytes], T] | Callable[[AsyncHTTPResponse, bytes], Awaitable[T]] = None, 
    async_: Literal[True], 
    **request_kwargs, 
) -> Coroutine[Any, Any, AsyncHTTPResponse | bytes | str | dict | list | int | float | bool | None | T]:
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
    stream: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] | Callable[[AsyncHTTPResponse, bytes], T] | Callable[[AsyncHTTPResponse, bytes], Awaitable[T]] = None, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> HTTPResponse | bytes | str | dict | list | int | float | bool | None | T | Coroutine[Any, Any, AsyncHTTPResponse | bytes | str | dict | list | int | float | bool | None | T]:
    if async_:
        return request_async( # type: ignore
            url=url, 
            method=method, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            follow_redirects=follow_redirects, 
            raise_for_status=raise_for_status, 
            stream=stream, 
            cookies=cookies, 
            parse=parse, # type: ignore[arg-type]
            **request_kwargs, 
        )
    else:
        return request_sync( # type: ignore
            url=url, 
            method=method, 
            params=params, 
            data=data, 
            json=json, 
            files=files, 
            headers=headers, 
            follow_redirects=follow_redirects, 
            raise_for_status=raise_for_status, 
            stream=stream, 
            cookies=cookies, 
            parse=parse, # type: ignore[arg-type]
            **request_kwargs, 
        )

