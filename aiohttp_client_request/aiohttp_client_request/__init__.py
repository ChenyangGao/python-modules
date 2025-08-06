#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 0)
__all__ = ["request"]

from asyncio import get_running_loop, run, run_coroutine_threadsafe
from collections import UserString
from collections.abc import Awaitable, Buffer, Callable, Iterable, Mapping
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
from inspect import isawaitable, signature
from os import PathLike
from sys import maxsize
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal

from aiohttp import ClientResponse, ClientSession
from argtools import argcount
from cookietools import update_cookies
from dicttools import get_all_items
from filewrap import bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

_REQUEST_KWARGS: Final = signature(ClientSession._request).parameters.keys() - {"self"}
_DEFAULT_SESSION: ClientSession


def _get_default_session():
    global _DEFAULT_SESSION
    try:
        return _DEFAULT_SESSION
    except NameError:
        _DEFAULT_SESSION = ClientSession()
        return _DEFAULT_SESSION


def _async_session_del(self, /, __old_del=ClientSession.__del__):
    if not self.closed:
        try:
            try:
                loop = get_running_loop()
            except RuntimeError:
                run(self.close())
            else:
                run_coroutine_threadsafe(self.close(), loop)
        except Exception:
            pass
    __old_del(self)

setattr(ClientSession, "__del__", _async_session_del)


def _async_response_del(self, /, __old_del=ClientResponse.__del__):
    if not self.closed:
        self.release()
        self.close()
    __old_del(self)

setattr(ClientResponse, "__del__", _async_response_del)


@overload
async def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> ClientResponse:
    ...
@overload
async def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request[T](
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: Callable[[ClientResponse], T] | Callable[[ClientResponse], Awaitable[T]] | Callable[[ClientResponse, bytes], T] | Callable[[ClientResponse, bytes], Awaitable[T]], 
    **request_kwargs, 
) -> T:
    ...
async def request[T](
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: None | EllipsisType| bool | Callable[[ClientResponse], T] | Callable[[ClientResponse], Awaitable[T]] | Callable[[ClientResponse, bytes], T] | Callable[[ClientResponse, bytes], Awaitable[T]] = None, 
    **request_kwargs, 
) -> ClientResponse | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs.pop("stream", None)
    if follow_redirects:
        request_kwargs["allow_redirects"] = True
        request_kwargs.setdefault("max_redirects", maxsize)
    else:
        request_kwargs["allow_redirects"] = False
    if session is undefined:
        session = _get_default_session()
    elif session is None:
        session = ClientSession()
    session = cast(ClientSession, session)
    setattr(session, "cookies", session.cookie_jar)
    if isinstance(data, PathLike):
        data = bio_chunk_async_iter(open(data, "rb"))
    elif isinstance(data, SupportsRead):
        data = bio_chunk_async_iter(data)
    request_kwargs.update(normalize_request_args(
        method=method, 
        url=url, 
        params=params, 
        data=data, 
        json=json, 
        headers=headers, 
    ))
    request_kwargs["str_or_url"] = request_kwargs["url"]
    if cookies is not None:
        if isinstance(cookies, CookieJar):
            request_kwargs["cookies"] = update_cookies(SimpleCookie(), cookies)
        else:
            request_kwargs["cookies"] = cookies
    response = await session._request(
        **dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
    setattr(response, "session", session)
    response_cookies = response.cookies
    if cookies is not None and response_cookies:
        update_cookies(cookies, response_cookies) # type: ignore
    if raise_for_status:
        response.raise_for_status()
    if parse is None:
        return response
    async with response:
        if parse is ...:
            return response
        elif isinstance(parse, bool):
            content = await response.read()
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            ret = cast(Callable[[ClientResponse], T] | Callable[[ClientResponse], Awaitable[T]], parse)(response)
        else:
            ret = cast(Callable[[ClientResponse, bytes], T] | Callable[[ClientResponse, bytes], Awaitable[T]], parse)(
                response, (await response.read()))
        if isawaitable(ret):
            ret = await ret
        return ret

