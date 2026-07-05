#!/usr/bin/env python3
# coding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 3)
__all__ = ["request"]

from collections import UserString
from collections.abc import (
    AsyncGenerator, AsyncIterable, Awaitable, Buffer, Callable, 
    Iterable, Mapping, 
)
from http.cookiejar import CookieJar
from http.cookies import BaseCookie
from inspect import isawaitable
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Literal

from blacksheep.client.session import ClientSession
from blacksheep.client.exceptions import UnsupportedRedirect
from blacksheep.common.types import normalize_headers
from blacksheep.contents import Content, StreamedContent
from blacksheep.exceptions import HTTPException
from blacksheep.messages import Request, Response
from cookietools import update_cookies
from ensure import ensure_buffer
from filewrap import bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import decompress_response, parse_response
from multidict import CIMultiDict
from yarl import URL
from undefined import undefined, Undefined


type string = Buffer | str | UserString

_DEFAULT_SESSION: ClientSession


def _get_default_session() -> ClientSession:
    global _DEFAULT_SESSION
    try:
        return _DEFAULT_SESSION
    except NameError:
        _DEFAULT_SESSION = ClientSession(follow_redirects=False)
        return _DEFAULT_SESSION


if "__del__" not in ClientSession.__dict__:
    def close(self, /):
        from asynctools import run_async
        try:
            run_async(ClientSession.close(self))
        except:
            pass
    setattr(ClientSession, "__del__", close)


class ResponseWrapper:

    def __init__(self, response: Response):
        self.response = response
        self.headers = CIMultiDict((str(k, "latin-1"), str(v, "latin-1")) for k, v in response.headers)

    def __dir__(self, /) -> list[str]:
        s = set(super().__dir__())
        s.update(dir(self.response))
        return sorted(s)

    def __getattr__(self, attr, /):
        return getattr(self.response, attr)

    def __repr__(self, /):
        return f"{type(self).__qualname__}({self.response!r})"


@overload
async def request(
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
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> Response:
    ...
@overload
async def request(
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
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
async def request(
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
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
    ...
@overload
async def request[T](
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
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper, bytes], Awaitable[T]], 
    **request_kwargs, 
) -> T:
    ...
async def request[T](
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
    session: None | Undefined | ClientSession = undefined, 
    *, 
    parse: None | EllipsisType | bool | Callable[[ResponseWrapper, bytes], T] | Callable[[ResponseWrapper, bytes], Awaitable[T]] = None, 
    **request_kwargs, 
) -> ResponseWrapper | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs.pop("stream", None)
    if session is undefined:
        session = _get_default_session()
    elif session is None:
        session = ClientSession()
    session = cast(ClientSession, session)
    session.follow_redirects = False
    if isinstance(url, Request):
        request = url
    else:
        content: None | Content = None
        if isinstance(data, Content):
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                headers=headers, 
            )
            content = data
        else:
            if isinstance(data, PathLike):
                data = bio_chunk_async_iter(open(data, "rb"))
            elif isinstance(data, SupportsRead):
                data = bio_chunk_async_iter(data)
            request_args = normalize_request_args(
                method=method, 
                url=url, 
                params=params, 
                data=data, 
                files=files, 
                json=json, 
                headers=headers, 
                async_=True, 
            )
        headers_ = request_args["headers"] or {}
        request = Request(
            request_args["method"], 
            bytes(request_args["url"], "utf-8"), 
            normalize_headers(headers_), 
        )
        if data := request_args["data"]:
            content_type = bytes(headers_.get("content-type") or "application/octet-stream", "latin-1")
            if isinstance(data, Content):
                content = data
            elif isinstance(data, Buffer):
                if not isinstance(data, bytes):
                    data = bytes(data)
                content = Content(content_type, data)
            else:
                if not isinstance(data, AsyncGenerator):
                    async def as_gen(data, /):
                        if isinstance(data, AsyncIterable):
                            async for chunk in data:
                                yield ensure_buffer(chunk)
                        else:
                            for chunk in data:
                                yield ensure_buffer(chunk)
                    data = as_gen(data)
                content = StreamedContent(content_type, data)
        if content:
            request = request.with_content(content)
    if cookies is not None:
        # from datetime import datetime
        # from blacksheep.client.cookies import Cookie as BlackSheepCookie
        if isinstance(cookies, BaseCookie):
            # request.cookies.update((name, BlackSheepCookie(name, morsel.value, **{
            #     "expires": morsel.get("expires") and datetime.strptime(morsel["expires"], "%a, %d-%b-%Y %H:%M:%S GMT"), 
            #     "domain": morsel.get("domain"), 
            #     "path": morsel.get("path"), 
            #     "secure": bool(morsel.get("secure")), 
            #     "http_only": bool(morsel.get("httponly")), 
            #     "max_age": int(morsel.get("max-age") or -1), 
            # })) for name, morsel in cookies.items())
            request.cookies.update((name, morsel.value) for name, morsel in cookies.items())
        else:
            # request.cookies.update((cookie.name, BlackSheepCookie(cookie.name, cookie.value, **{
            #     "expires": cookie.expires and datetime.fromtimestamp(cookie.expires), 
            #     "domain": cookie.domain, 
            #     "path": cookie.path, 
            #     "secure": bool(cookie.secure), 
            #     "http_only": bool(cookie._rest and cookie._rest.get("HttpOnly")),      
            #     "max_age": int(cookie._rest and cookie._rest.get("Max-Age") or -1), 
            # })) for cookie in cookies if cookie.value is not None)
            request.cookies.update((cookie.name, cookie.value) for cookie in cookies if cookie.value is not None)
    while True:
        resp = await session.send(request)
        setattr(resp, "session", session)
        resp_cookies = resp.cookies
        if cookies is not None and resp_cookies:
            update_cookies(cookies, resp_cookies) # type: ignore
        response = ResponseWrapper(resp)
        if resp.is_redirect():
            if follow_redirects:
                try:
                    session.update_request_for_redirect(request, resp)
                    await response.read()
                    continue
                except UnsupportedRedirect:
                    pass
        elif raise_for_status and resp.status >= 400:
            await response.read()
            raise HTTPException(resp.status, resp.reason)
        if parse is None:
            if request.method == "HEAD":
                await response.read()
            return response
        elif parse is ...:
            await response.read()
            return response
        res = decompress_response(await response.read(), response)
        if isinstance(parse, bool):
            if not parse:
                return res
            parse = cast(Callable, parse_response)
        ret = parse(response, res)
        if isawaitable(ret):
            ret = await ret
        return ret

