#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
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
from curl_cffi import AsyncSession, Response, Session
from curl_cffi.requests.session import BaseSession
from dicttools import get_all_items
from filewrap import bio_chunk_iter, bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

_INIT_SESSION_KWARGS: Final = signature(BaseSession).parameters.keys() | {"curl", "thread", "use_thread_local_curl"}
_INIT_ASYNC_SESSION_KWARGS: Final = signature(BaseSession).parameters.keys() | {"loop", "async_curl", "max_clients"}
_REQUEST_KWARGS: Final = signature(Session.request).parameters.keys() - {"self"}

if "__del__" not in Session.__dict__:
    setattr(Session, "__del__", Session.close)
if "__del__" not in AsyncSession.__dict__:
    def __del__(self, /):
        from asynctools import run_async
        return run_async(self.close())
    setattr(AsyncSession, "__del__", __del__)
if "__del__" not in Response.__dict__:
    def __del__(self, /):
        if self.stream_task:
            self.close()
        elif self.astream_task:
            from asynctools import run_async
            return run_async(self.aclose())
    setattr(Response, "__del__", __del__)

_DEFAULT_SESSION: Session = Session(verify=False)
_DEFAULT_ASYNC_SESSION: AsyncSession = AsyncSession(verify=False)


@overload
def request_sync(
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
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None = None, 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request_sync(
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
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> Response:
    ...
@overload
def request_sync(
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
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
def request_sync[T](
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
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response], T], 
    **request_kwargs, 
) -> T:
    ...
def request_sync[T](
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
    session: None | Session = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["allow_redirects"] = follow_redirects
    request_kwargs.setdefault("stream", True)
    if session is None:
        session = Session(**dict(get_all_items(request_kwargs, *_INIT_SESSION_KWARGS)))
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
    if cookies is not None:
        if isinstance(cookies, BaseCookie):
            request_kwargs["cookies"] = update_cookies(CookieJar(), cookies)
        else:
            request_kwargs["cookies"] = cookies
    response = session.request(**dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
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
            if response.stream_task:
                content = bytearray()
                for chunk in response.iter_content():
                    content += chunk
            else:
                content = response.content
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[Response], T], parse)(response)
        else:
            if response.stream_task:
                content = bytearray()
                for chunk in response.iter_content():
                    content += chunk
            else:
                content = response.content
            return cast(Callable[[Response, bytes], T], parse)(response, content)


@overload
async def request_async(
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
    session: None | AsyncSession = _DEFAULT_ASYNC_SESSION, 
    *, 
    parse: None = None, 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request_async(
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
    session: None | AsyncSession = _DEFAULT_ASYNC_SESSION, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> Response:
    ...
@overload
async def request_async(
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
    session: None | AsyncSession = _DEFAULT_ASYNC_SESSION, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request_async[T](
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
    session: None | AsyncSession = _DEFAULT_ASYNC_SESSION, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]], 
    **request_kwargs, 
) -> T:
    ...
async def request_async[T](
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
    session: None | AsyncSession = _DEFAULT_ASYNC_SESSION, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["allow_redirects"] = follow_redirects
    request_kwargs.setdefault("stream", True)
    if session is None:
        session = AsyncSession(**dict(get_all_items(request_kwargs, *_INIT_ASYNC_SESSION_KWARGS)))
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
    if cookies is not None:
        if isinstance(cookies, BaseCookie):
            request_kwargs["cookies"] = update_cookies(CookieJar(), cookies)
        else:
            request_kwargs["cookies"] = cookies
    response = await session.request(**dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
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
            if response.astream_task:
                content = await response.acontent()
            else:
                content = response.content
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            ret = cast(Callable[[Response], T] | Callable[[Response], Awaitable[T]], parse)(response)
        else:
            if response.astream_task:
                content = await response.acontent()
            else:
                content = response.content
            ret = cast(Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]], parse)(response, content)
        if isawaitable(ret):
            ret = await ret
        return ret


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
    session: None | Undefined | Session = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
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
    session: None | Undefined | AsyncSession = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    async_: Literal[True], 
    **request_kwargs, 
) -> Awaitable[Response | bytes | str | dict | list | int | float | bool | None | T]:
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
    session: None | Undefined | Session | AsyncSession = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[Response, bytes], T] | Callable[[Response, bytes], Awaitable[T]] | Callable[[Response], T] | Callable[[Response], Awaitable[T]] = None, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T | Awaitable[Response | bytes | str | dict | list | int | float | bool | None | T]:
    if async_:
        if session is undefined:
            session = _DEFAULT_ASYNC_SESSION
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
            session=cast(None | AsyncSession, session), 
            parse=parse, # type: ignore 
            **request_kwargs, 
        )
    else:
        if session is undefined:
            session = _DEFAULT_SESSION
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
            session=cast(None | Session, session), 
            parse=parse, # type: ignore  
            **request_kwargs, 
        )

