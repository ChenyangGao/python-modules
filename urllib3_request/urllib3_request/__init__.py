#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 4)
__all__ = ["request"]

from collections import UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, IO, Literal
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit
from urllib.request import Request
from warnings import warn

from argtools import argcount
from cookietools import extract_cookies, cookies_dict_to_str, cookies_str_to_dict
from dicttools import dict_merge
from filewrap import SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import parse_response
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse
from urllib3.util.url import _normalize_host as normalize_host
from yarl import URL


type string = Buffer | str | UserString

if "__del__" not in PoolManager.__dict__:
    setattr(PoolManager, "__del__", PoolManager.clear)

_DEFAULT_POOL = PoolManager(maxsize=128)
setattr(_DEFAULT_POOL, "cookies", CookieJar())


def origin_tuple(url: str, /) -> tuple[str, str, int]:
    urlp   = urlsplit(url)
    scheme = urlp.scheme or "http"
    host   = normalize_host(urlp.hostname or "", scheme) or ""
    port   = urlp.port or (443 if scheme == "https" else 80)
    return (scheme, host, port)


def is_same_origin(url1: str, url2: str, /) -> bool:
    if url2.startswith("/"):
        return True
    elif url1.startswith("/"):
        return False
    return origin_tuple(url1) == origin_tuple(url2)


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
    stream: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
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
    stream: bool = True, 
    cookies: None | CookieJar | SimpleCookie = None, 
    session: None | PoolManager = _DEFAULT_POOL, 
    *, 
    parse: None | EllipsisType| bool | Callable[[HTTPResponse, bytes], T] | Callable[[HTTPResponse], T] = None, 
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
    if "cookie" in headers_:
        cookies_dict = cookies_str_to_dict(headers_["cookie"] or "")
    else:
        cookies_dict = {}
        if cookies:
            netloc_endswith = urlsplit(url).netloc.endswith
            if isinstance(cookies, CookieJar):
                dict_merge(cookies_dict, (
                    (cookie.name, val)
                    for cookie in cookies 
                    if (val := cookie.value) is not None and ((domain := cookie.domain) or netloc_endswith(domain))
                ))
            else:
                dict_merge(cookies_dict, (
                    (name, val)
                    for name, morsel in cookies.items()
                    if (val := morsel.value) is not None and ((domain := morsel.get("domain", "")) or netloc_endswith(domain))
                ))
        headers_["cookie"] = cookies_dict_to_str(cookies_dict)
    response_cookies = CookieJar()
    request_kwargs["redirect"] = False
    while True:
        response = cast(HTTPResponse, session.request(
            method=method, 
            url=url, 
            body=body, 
            headers=headers_, 
            **request_kwargs, 
        ))
        setattr(response, "session", session)
        setattr(response, "cookies", response_cookies)
        if cookies is not None:
            extract_cookies(cookies, url, response) # type: ignore
        extract_cookies(response_cookies, url, response)
        status_code = response.status
        if status_code >= 400 and raise_for_status:
            raise HTTPError(
                url, 
                status_code, 
                response.reason or "", 
                response.headers, # type: ignore
                cast(IO[bytes], response), 
            )
        elif redirect_location := follow_redirects and response.get_redirect_location():
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
        if parse is None:
            return response
        elif parse is ...:
            response.close()
            return response
        with response:
            if isinstance(parse, bool):
                content = response.read()
                if parse:
                    return parse_response(response, content)
                return content
            ac = argcount(parse)
            if ac == 1:
                return cast(Callable[[HTTPResponse], T], parse)(response)
            else:
                return cast(Callable[[HTTPResponse, bytes], T], parse)(
                    response, response.read())

