#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = ["HTTPClient", "HTTPError", "request"]

from collections import UserString
from collections.abc import Awaitable, Buffer, Callable, Iterable, Mapping
from http.cookiejar import CookieJar
from http.cookies import BaseCookie, SimpleCookie
from inspect import isawaitable, signature
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal

from aiosonic import HTTPClient as BaseHTTPClient, HttpResponse # type: ignore
from aiosonic.http_parser import add_header # type: ignore
from argtools import argcount
from cookietools import update_cookies
from dicttools import get_all_items, iter_items
from filewrap import bio_chunk_async_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from yarl import URL


type string = Buffer | str | UserString

_INIT_SEESION_KWARGS: Final = signature(BaseHTTPClient).parameters.keys()
_REQUEST_KWARGS: Final = signature(BaseHTTPClient.request).parameters.keys() - {"self"}


class HTTPClient(BaseHTTPClient):

    def __del__(self, /):
        if connector := self.connector:
            from asynctools import run_async
            return run_async(connector.cleanup())

    def _add_cookies_to_request(self, host: str, headers):
        """Add cookies to request."""
        if any(header.lower() == "cookie" for header, _ in iter_items(headers)):
            return
        for domain, cookies in self.cookies_map.items():
            if domain == host or host.endswith(domain):
                cookies_str = cookies.output(header="Cookie:")
                for cookie_data in cookies_str.split("\r\n"):
                    add_header(headers, *cookie_data.split(": ", 1))

    def _save_new_cookies(self, host: str, response: BaseCookie | HttpResponse):
        """Save new cookies in map."""
        if isinstance(response, BaseCookie):
            cookies = response
        else:
            cookies = response.cookies
        if cookies:
            cookies_map = self.cookies_map
            for name, morsel in cookies.items():
                domain = morsel["domain"] or ""
                if domain not in cookies_map:
                    cookies_map[domain] = SimpleCookie()
                cookies_map[domain][name] = morsel

_DEFAULT_SESSION: HTTPClient = HTTPClient(handle_cookies=True, verify_ssl=False)


class HTTPError(OSError):

    def __init__(
        self, 
        /, 
        *args, 
        url: str, 
        status: int, 
        method: str, 
        response: HttpResponse, 
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
async def request(
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
    session: None | HTTPClient = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> HttpResponse:
    ...
@overload
async def request(
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
    session: None | HTTPClient = _DEFAULT_SESSION, 
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
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_SESSION, 
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
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | BaseCookie = None, 
    session: None | HTTPClient = _DEFAULT_SESSION, 
    *, 
    parse: Callable[[HttpResponse], T] | Callable[[HttpResponse], Awaitable[T]] | Callable[[HttpResponse, bytes], T] | Callable[[HttpResponse, bytes], Awaitable[T]], 
    **request_kwargs, 
) -> T:
    ...
async def request[T](
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
    session: None | HTTPClient = _DEFAULT_SESSION, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HttpResponse], T] | Callable[[HttpResponse], Awaitable[T]] | Callable[[HttpResponse, bytes], T] | Callable[[HttpResponse, bytes], Awaitable[T]] = None, 
    **request_kwargs, 
) -> HttpResponse | bytes | str | dict | list | int | float | bool | None | T:
    request_kwargs["follow"] = follow_redirects
    if session is None:
        session = HTTPClient(**dict(get_all_items(request_kwargs, *_INIT_SEESION_KWARGS)))
    session = cast(HTTPClient, session)
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
    if cookies:
        if isinstance(cookies, BaseCookie):
            session._save_new_cookies("", cookies)
        else:
            session._save_new_cookies("", update_cookies(BaseCookie(), cookies))
    response = await session.request(**dict(get_all_items(request_kwargs, *_REQUEST_KWARGS)))
    setattr(response, "session", session)
    response_cookies = response.cookies
    if cookies is not None and response_cookies:
        update_cookies(cookies, response_cookies) # type: ignore
    if response.status_code >= 400 and raise_for_status:
        raise HTTPError(
            url=request_kwargs["url"], 
            status=response.status_code, 
            method=request_kwargs["method"], 
            response=response, 
        )
    if parse is None:
        return response
    try:
        if parse is ...:
            return response
        elif isinstance(parse, bool):
            content = await response.content()
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            ret = cast(Callable[[HttpResponse], T] | Callable[[HttpResponse], Awaitable[T]], parse)(response)
        else:
            ret = cast(Callable[[HttpResponse, bytes], T] | Callable[[HttpResponse, bytes], Awaitable[T]], parse)(
                response, (await response.content()))
        if isawaitable(ret):
            ret = await ret
        return ret
    finally:
        response.__del__()

