#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 8)
__all__ = ["request", "request_sync", "request_async"]

from collections import UserString
from collections.abc import (
    Awaitable, Buffer, Callable, Iterable, Mapping, 
)
from contextlib import aclosing, closing
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable, signature
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal

from argtools import argcount
from cookietools import update_cookies
from dicttools import get_all_items
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from httpx import (
    Cookies, Client, AsyncClient, HTTPTransport, AsyncHTTPTransport, 
    Limits, Request, Response, Timeout, 
)
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

_INIT_CLIENT_KWARGS: Final   = signature(Client).parameters.keys()
_BUILD_REQUEST_KWARGS: Final = signature(Client.build_request).parameters.keys() - {"self"}
_SEND_REQUEST_KWARGS: Final  = ("auth", "stream", "follow_redirects")

if "__del__" not in Client.__dict__:
    setattr(Client, "__del__", Client.close)
if "close" not in AsyncClient.__dict__:
    def close(self, /):
        from asynctools import run_async
        return run_async(self.aclose())
    setattr(AsyncClient, "close", close)
if "__del__" not in AsyncClient.__dict__:
    setattr(AsyncClient, "__del__", getattr(AsyncClient, "close"))
if "__del__" not in Response.__dict__:
    def __del__(self, /):
        stream = self.stream
        if self.is_closed or not stream:
            return
        from httpx import AsyncByteStream, SyncByteStream
        if isinstance(stream, SyncByteStream):
            self.close()
        elif isinstance(stream, AsyncByteStream):
            from asynctools import run_async
            return run_async(self.aclose())
    setattr(Response, "__del__", __del__)

_DEFAULT_CLIENT = Client(
    http2=True, 
    limits=Limits(max_connections=256, max_keepalive_connections=64, keepalive_expiry=10), 
    transport=HTTPTransport(retries=5), 
    timeout=Timeout(connect=5, read=60, write=60, pool=5), 
    verify=False, 
)
_DEFAULT_ASYNC_CLIENT = AsyncClient(
    http2=True, 
    limits=Limits(max_connections=256, max_keepalive_connections=64, keepalive_expiry=10), 
    transport=AsyncHTTPTransport(retries=5), 
    imeout=Timeout(connect=5, read=60, write=60, pool=5), 
    verify=False, 
)


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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Client = _DEFAULT_CLIENT, 
    *, 
    parse: None = None, 
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Client = _DEFAULT_CLIENT, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> Response:
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Client = _DEFAULT_CLIENT, 
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Client = _DEFAULT_CLIENT, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response], T], 
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Client = _DEFAULT_CLIENT, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["follow_redirects"] = follow_redirects
    request_kwargs.setdefault("stream", True)
    if session is None:
        init_kwargs = dict(get_all_items(request_kwargs, *_INIT_CLIENT_KWARGS))
        if "transport" not in init_kwargs:
            init_kwargs["transport"] = HTTPTransport(http2=True, retries=5)
        session = Client(**init_kwargs)
    if isinstance(url, Request):
        request = url
    else:
        if isinstance(data, PathLike):
            data = bio_chunk_iter(open(data, "rb"))
        elif isinstance(data, SupportsRead):
            data = bio_chunk_iter(data)
        request_kwargs.update(normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            files=files, 
            json=json, 
            headers=headers, 
        ))
        request = session.build_request(**dict(get_all_items(
            request_kwargs, *_BUILD_REQUEST_KWARGS)))
    if cookies is not None:
        if isinstance(cookies, BaseCookie):
            request_kwargs["cookies"] = update_cookies(CookieJar(), cookies)
        else:
            request_kwargs["cookies"] = cookies
    response = session.send(request, **dict(get_all_items(
        request_kwargs, *_SEND_REQUEST_KWARGS)))
    setattr(response, "session", session)
    if cookies is not None and response.cookies:
        update_cookies(cookies, response.cookies.jar) # type: ignore
    if response.status_code >= 400 and raise_for_status:
        response.raise_for_status()
    if parse is None:
        return response
    elif parse is ...:
        response.close()
        return response
    with closing(response):
        if isinstance(parse, bool):
            content = response.read()
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[Response], T], parse)(response)
        else:
            return cast(Callable[[Response, bytes], T], parse)(response, response.read())


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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | AsyncClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: None = None, 
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | AsyncClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> Response:
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | AsyncClient = _DEFAULT_ASYNC_CLIENT, 
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | AsyncClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]], 
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | AsyncClient = _DEFAULT_ASYNC_CLIENT, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["follow_redirects"] = follow_redirects
    request_kwargs.setdefault("stream", True)
    if session is None:
        init_kwargs = dict(get_all_items(request_kwargs, *_INIT_CLIENT_KWARGS))
        if "transport" not in init_kwargs:
            init_kwargs["transport"] = AsyncHTTPTransport(http2=True, retries=5)
        session = AsyncClient(**init_kwargs)
    if isinstance(url, Request):
        request = url
    else:
        if isinstance(data, PathLike):
            data = bio_chunk_async_iter(open(data, "rb"))
        elif isinstance(data, SupportsRead):
            data = bio_chunk_async_iter(data)
        request_kwargs.update(normalize_request_args(
            method=method, 
            url=url, 
            params=params, 
            data=data, 
            files=files, 
            json=json, 
            headers=headers, 
            async_=True, 
        ))
        request = session.build_request(**dict(get_all_items(
            request_kwargs, *_BUILD_REQUEST_KWARGS)))
    if cookies is not None:
        if isinstance(cookies, BaseCookie):
            request_kwargs["cookies"] = update_cookies(CookieJar(), cookies)
        else:
            request_kwargs["cookies"] = cookies
    response = await session.send(request, **dict(get_all_items(
        request_kwargs, *_SEND_REQUEST_KWARGS)))
    setattr(response, "session", session)
    if cookies is not None and response.cookies:
        update_cookies(cookies, response.cookies.jar) # type: ignore
    if response.status_code >= 400 and raise_for_status:
        response.raise_for_status()
    if parse is None:
        return response
    elif parse is ...:
        await response.aclose()
        return response
    async with aclosing(response):
        if isinstance(parse, bool):
            content = await response.aread()
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            ret = cast(Callable[[Response], T] | Callable[[Response], Awaitable[T]], parse)(response)
        else:
            ret = cast(Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]], parse)(
                response, await response.aread())
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Undefined | Client = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Undefined | AsyncClient = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    async_: Literal[True], 
    **request_kwargs, 
) -> Awaitable[Response | bytes | str | dict | list | int | float | bool | None | T]:
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
    cookies: None | Cookies | CookieJar | BaseCookie = None, 
    session: None | Undefined | Client | AsyncClient = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T | Awaitable[Response | bytes | str | dict | list | int | float | bool | None | T]:
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
            session=cast(None | AsyncClient, session), 
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
            session=cast(None | Client, session), 
            parse=parse, # type: ignore  
            **request_kwargs, 
        )

