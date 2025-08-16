#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 2)
__all__ = ["Response", "request"]

from atexit import register
from collections import deque, UserString
from collections.abc import Buffer, Callable, Iterable, Mapping
from contextlib import closing, contextmanager
from http import HTTPStatus
from http.client import HTTPMessage
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
from io import BytesIO
from os import PathLike
from types import EllipsisType
from typing import cast, overload, Any, Final, Literal
from urllib.error import HTTPError
from urllib.parse import urljoin

from argtools import argcount
from cookietools import cookies_to_str, extract_cookies
from filewrap import bio_chunk_iter, SupportsRead
from http_request import normalize_request_args, SupportsGeturl
from http_response import decompress_response, parse_response
import pycurl
from pycurl import Curl
from undefined import undefined, Undefined
from yarl import URL


type string = Buffer | str | UserString

COOKIE_JAR: Final = CookieJar()
CURL_DEQUE: Final[deque[Curl]] = deque()

@register
def release(*, _dq=CURL_DEQUE):
    for curl in _dq:
        curl.close()


@contextmanager
def loop_curl(
    curl_deque: deque[Curl] = CURL_DEQUE, 
    factory: Callable[[], Curl] = Curl, 
):
    try:
        curl = curl_deque.popleft()
    except IndexError:
        curl = factory()
    try:
        yield curl
    finally:
        curl_deque.append(curl)


class Response:

    def __init__(
        self, 
        /, 
        url: str, 
        content: Buffer | SupportsRead[bytes], 
        headers: HTTPMessage, 
        status: int = 200, 
        **kwargs, 
    ):
        self.url = url
        self.content = content
        self.headers = headers
        self.status = status
        self.__dict__.update(kwargs)

    def __repr__(self, /) -> str:
        cls = type(self)
        status_code = self.status
        return f"<{cls.__module__}.{cls.__qualname__} [{status_code} {HTTPStatus(status_code).phrase}] at {hex(id(self))}>"

    def __getattr__(self, attr, /):
        return getattr(self.content, attr)

    def info(self, /) -> HTTPMessage:
        return self.headers

    def raise_for_status(self, /):
        status_code = self.status
        if status_code >= 400:
            raise HTTPError(
                self.url, 
                status_code, 
                HTTPStatus(status_code).phrase, 
                self.headers, 
                self.content, # type: ignore
            )

    def is_redirect(self, /) -> bool:
        return 300 <= self.status < 400

    @property
    def data(self, /):
        content = self.content
        if isinstance(content, Buffer):
            return content
        self.content = content = decompress_response(content.read(-1), self)
        return content

    def json(self, /):
        from json import loads
        return loads(self.data)


@overload
def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = COOKIE_JAR, 
    curl: None | Undefined | Curl = undefined, 
    *, 
    parse: None | EllipsisType = None, 
    **request_kwargs, 
) -> Response:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = COOKIE_JAR, 
    curl: None | Undefined | Curl = undefined, 
    *, 
    parse: Literal[False], 
    **request_kwargs, 
) -> bytes:
    ...
@overload
def request(
    url: string | SupportsGeturl | URL, 
    method: string = "GET", 
    params: None | string | Mapping | Iterable[tuple[Any, Any]] = None, 
    data: Any = None, 
    json: Any = None, 
    files: None | Mapping[string, Any] | Iterable[tuple[string, Any]] = None, 
    headers: None | Mapping[string, string] | Iterable[tuple[string, string]] = None, 
    follow_redirects: bool = True, 
    raise_for_status: bool = True, 
    cookies: None | CookieJar | SimpleCookie = COOKIE_JAR, 
    curl: None | Undefined | dict | Curl = undefined, 
    *, 
    parse: Literal[True], 
    **request_kwargs, 
) -> bytes | str | dict | list | int | float | bool | None:
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
    cookies: None | CookieJar | SimpleCookie = COOKIE_JAR, 
    curl: None | Undefined | dict | Curl = undefined, 
    *, 
    parse: Callable[[Response, bytes], T] | Callable[[Response], T], 
    **request_kwargs, 
) -> T:
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
    cookies: None | CookieJar | SimpleCookie = COOKIE_JAR, 
    curl: None | Undefined | dict | Curl = undefined, 
    *, 
    parse: None | EllipsisType| bool | Callable[[Response, bytes], T] | Callable[[Response], T] = None, 
    **request_kwargs, 
) -> Response | bytes | str | dict | list | int | float | bool | None | T:
    if curl is undefined:
        with loop_curl() as curl:
            return request(**locals())
    elif isinstance(curl, dict):
        with loop_curl(**curl) as curl:
            return request(**locals())
    elif curl is None:
        with closing(Curl()) as curl:
            return request(**locals())
    curl = cast(Curl, curl)
    if isinstance(data, PathLike):
        data = open(data, "rb")
    if isinstance(data, SupportsRead):
        data = bio_chunk_iter(data)
    request_args = normalize_request_args(
        method=method, 
        url=url, 
        params=params, 
        data=data, 
        json=json, 
        files=files, 
        headers=headers, 
    )
    method = request_args["method"]
    url = request_args["url"]
    data = cast(None | Buffer | Iterable, request_args["data"])
    if data:
        if isinstance(data, Buffer):
            data = bytes(data)
        else:
            data = map(bytes, data)
    headers_ = request_args["headers"]
    need_set_cookie = "cookie" not in headers_
    response_cookies = CookieJar()
    setopt = curl.setopt
    setopt(pycurl.FOLLOWLOCATION, 0)
    while True:
        buffer = BytesIO()
        response_headers = HTTPMessage()
        n = 0
        status_line = ""
        def header_function(header_line: bytes, /):
            nonlocal n, status_line
            n += 1
            header = str(header_line, "latin-1")
            if n == 1:
                status_line = header
            else:
                name, colon, value = header.partition(":")
                if colon:
                    response_headers.set_raw(name.strip(), value.strip())
        if need_set_cookie:
            if cookies:
                headers_["cookie"] = cookies_to_str(cookies, url)
            elif response_cookies:
                headers_["cookie"] = cookies_to_str(response_cookies, url)
        setopt(pycurl.CUSTOMREQUEST, method)
        setopt(pycurl.URL, url)
        curl.setopt(pycurl.NOBODY, method == "HEAD")
        setopt(pycurl.UPLOAD, 0)
        if data:
            if isinstance(data, bytes):
                setopt(pycurl.POSTFIELDS, data)
            else:
                setopt(pycurl.UPLOAD, 1)
                setopt(pycurl.READFUNCTION, lambda _: next(data, b""))
        else:
            setopt(pycurl.POSTFIELDS, b"")
        setopt(pycurl.HTTPHEADER, [f"{k}: {v}" for k, v in headers_.items()])
        setopt(pycurl.WRITEFUNCTION, buffer.write)
        setopt(pycurl.HEADERFUNCTION, header_function)
        curl.perform()
        buffer.seek(0)
        status_code = curl.getinfo(pycurl.RESPONSE_CODE)
        response = Response(url, buffer, response_headers, status_code, status_line=status_line)
        setattr(response, "curl", curl)
        setattr(response, "cookies", response_cookies)
        extract_cookies(response_cookies, url, response)
        if cookies is not None:
            extract_cookies(cookies, url, response) # type: ignore
        if 300 <= status_code < 400 and follow_redirects:
            url = urljoin(url, response_headers["location"])
            if status_code == 303:
                method = "GET"
                data = None
            continue
        elif raise_for_status:
            response.raise_for_status()
        if parse is None or parse is ...:
            return response
        if isinstance(parse, bool):
            content = decompress_response(buffer.getvalue(), response)
            if parse:
                return parse_response(response, content)
            return content
        ac = argcount(parse)
        if ac == 1:
            return cast(Callable[[Response], T], parse)(response)
        else:
            content = decompress_response(buffer.getvalue(), response)
            return cast(Callable[[Response, bytes], T], parse)(
                response, content)

