#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 8)
__all__ = [
    "create_cookie", "create_morsel", "to_cookie", "to_morsel", 
    "cookie_to_morsel", "morsel_to_cookie", "cookies_str_to_dict", 
    "cookies_dict_to_str", "extract_cookies", "update_cookies", 
    "iter_resp_cookies", 
]

from calendar import timegm
from collections.abc import Buffer, Iterable, Iterator, Mapping, Sequence
from copy import copy
from datetime import datetime
from functools import partial
from http.cookiejar import Cookie, CookieJar
from http.cookies import Morsel, SimpleCookie
from re import compile as re_compile
from time import gmtime, strftime, strptime, time
from typing import cast, Any
from urllib.request import Request

from dicttools import get, iter_items


CRE_COOKIE_SEP_split = re_compile(r";\s*").split


def create_cookie(
    name: str, 
    value: str | Cookie | Morsel | Mapping, 
    **kwargs, 
) -> Cookie:
    if isinstance(value, str):
        pass
    elif isinstance(value, Cookie):
        if not kwargs:
            cookie = copy(value)
            if name:
                cookie.name = name
            return cookie
        kwargs = {**value.__dict__, **kwargs}
        kwargs.setdefault("rest", kwargs.pop("_rest", {"HttpOnly": None}))
        value = value.value or ""
    elif isinstance(value, Morsel):
        kwargs = {
            **value, 
            "rest": {
                "HttpOnly": value.get("httponly"), 
                "SameSite": value.get("samesite"), 
                "Max-Age": value.get("max-age"), 
            }, 
            **kwargs, 
        }
        value = value.value
    else:
        kwargs = {**value, **kwargs}
        value = kwargs.get("value", "")
    if not name:
        name = cast(str, kwargs["name"])
    if not name:
        raise ValueError(f"please provide a name for value {value!r}")
    if expires := kwargs.get("expires"):
        if isinstance(expires, datetime):
            kwargs["expires"] = expires.timestamp()
        elif isinstance(expires, str):
            kwargs["expires"] = timegm(strptime(expires, "%a, %d-%b-%Y %H:%M:%S GMT"))
    elif max_age := kwargs.get("max-age"):
        try:
            kwargs["expires"] = int(time()) + int(max_age)
        except ValueError:
            raise TypeError(f"max-age: {max_age!r} must be integer")
    result: dict[str, Any] = {
        "version": 0, 
        "name": name, 
        "value": value, 
        "port": None, 
        "domain": "", 
        "path": "/", 
        "secure": False, 
        "expires": None, 
        "discard": True, 
        "comment": None, 
        "comment_url": None, 
        "rest": {"HttpOnly": None}, 
        "rfc2109": False, 
    }
    result.update(e for e in kwargs.items() if e[0] in result)
    result["port_specified"] = bool(result["port"])
    result["domain_specified"] = bool(result["domain"])
    result["domain_initial_dot"] = result["domain"].startswith(".")
    result["path_specified"] = bool(result["path"])
    return Cookie(**result)


def create_morsel(
    name: str, 
    value: str | Cookie | Morsel | Mapping, 
    **kwargs, 
) -> Morsel:
    morsel: Morsel
    if isinstance(value, str):
        morsel = Morsel()
        morsel.set(name, value, value)
    elif isinstance(value, Cookie):
        kwargs = {**value.__dict__, **kwargs}
        kwargs.setdefault("rest", kwargs.pop("_rest", {"HttpOnly": None}))
        if not name:
            name = cast(str, kwargs["name"])
        if not name:
            raise ValueError(f"please provide a name for value {value!r}")
        value = cast(str, kwargs.get("value") or "")
        morsel = Morsel()
        morsel.set(name, value, value)
    elif isinstance(value, Morsel):
        morsel = copy(value)
        if name:
            setattr(morsel, "_key", name)
        if not kwargs:
            return morsel
    else:
        kwargs = {**value, **kwargs}
        if not name:
            name = cast(str, kwargs["name"])
        if not name:
            raise ValueError(f"please provide a name for value {value!r}")
        value  = cast(str, kwargs.get("value") or "")
        morsel = Morsel()
        morsel.set(name, value, value)
    if expires := kwargs.get("expires"):
        if isinstance(expires, datetime):
            morsel["expires"] = expires.strftime("%a, %d-%b-%Y %H:%M:%S GMT")
        elif isinstance(expires, (int, float)):
            morsel["expires"] = strftime("%a, %d-%b-%Y %H:%M:%S GMT", gmtime(expires))
    setdefault = morsel.setdefault
    setdefault("expires", kwargs.get("expires") or "")
    setdefault("path", kwargs.get("path") or "")
    setdefault("comment", kwargs.get("comment") or "")
    setdefault("domain", kwargs.get("domain") or "")
    setdefault("secure", kwargs.get("secure"))
    setdefault("version", kwargs.get("version"))
    rest = kwargs.pop("rest", None) or {}
    setdefault("httponly", rest.get("HttpOnly"))
    setdefault("max-age", rest.get("Max-Age", ""))
    setdefault("samesite", rest.get("SameSite", ""))
    return morsel


def to_cookie(cookie, /, name: str = "") -> Cookie:
    if isinstance(cookie, str):
        if not name:
            raise ValueError(f"please provide a name for value {cookie!r}")
        return create_cookie(name, cookie)
    elif isinstance(cookie, Morsel):
        return morsel_to_cookie(cookie)
    elif isinstance(cookie, Cookie):
        cookie = copy(cookie)
        if name:
            cookie.name = name
        return cookie
    else:
        if isinstance(cookie, Mapping):
            getval = partial(get, cookie)
        else:
            getval = partial(getattr, cookie)
        name = getval("name", name)
        if not name:
            raise ValueError(f"please provide a name for value {cookie!r}")
        kwargs = {
            "name": name, 
            "value": getval("value", ""), 
            "version": getval("version", None) or getval("Version", None), 
            "expires": getval("expires", None), 
            "port": getval("port", None), 
            "domain": getval("domain", "") or getval("Domain", ""), 
            "domain_initial_dot": getval("domain_initial_dot", ""), 
            "path": getval("path", ""), 
            "secure": getval("secure", "") or getval("Secure", ""), 
            "discard": getval("discard", ""), 
            "comment": getval("comment", "") or getval("Comment", ""), 
            "comment_url": getval("comment_url", ""), 
            "rest": getval("rest", None) or {}, 
        }
        rest: dict = kwargs["rest"]
        for keys in [
            ("Max-Age", "max-age", "max_age"), 
            ("HttpOnly", "httponly", "http_only"), 
            ("SameSite", "samesite", "same_site"), 
        ]:
            for key in keys:
                val = getval(key, None)
                if val is not None:
                    rest[keys[0]] = str(val)
                    break
    return create_cookie(**kwargs)


def to_morsel(cookie, /, name: str = "") -> Morsel:
    if isinstance(cookie, str):
        if not name:
            raise ValueError(f"please provide a name for value {cookie!r}")
        return create_morsel(name, cookie)
    elif isinstance(cookie, Morsel):
        morsel = copy(cookie)
        if name:
            setattr(morsel, "_key", name)
        return morsel
    elif isinstance(cookie, Cookie):
        return cookie_to_morsel(cookie)
    else:
        if isinstance(cookie, Mapping):
            getval = partial(get, cookie)
        else:
            getval = partial(getattr, cookie)
        name = getval("name", name)
        if not name:
            raise ValueError(f"please provide a name for value {cookie!r}")
        kwargs = {
            "name": name, 
            "value": getval("value", ""), 
            "version": getval("version", None) or getval("Version", None), 
            "expires": getval("expires", None), 
            "domain": getval("domain", "") or getval("Domain", ""), 
            "path": getval("path", ""), 
            "secure": getval("secure", "") or getval("Secure", ""), 
            "comment": getval("comment", "") or getval("Comment", ""), 
            "rest": getval("rest", None) or {}, 
        }
        rest: dict = kwargs["rest"]
        for keys in [
            ("Max-Age", "max-age", "max_age"), 
            ("HttpOnly", "httponly", "http_only"), 
            ("SameSite", "samesite", "same_site"), 
        ]:
            for key in keys:
                val = getval(key, None)
                if val is not None:
                    rest[keys[0]] = str(val)
                    break
        return create_morsel(**kwargs)


def cookie_to_morsel(cookie: Cookie, /) -> Morsel:
    morsel: Morsel = Morsel()
    value = cookie.value or ""
    morsel.set(cookie.name, value, value)
    expires: Any
    if expires := cookie.expires:
        if isinstance(expires, datetime):
            expires = expires.strftime("%a, %d-%b-%Y %H:%M:%S GMT")
        elif isinstance(expires, (int, float)):
            expires = strftime("%a, %d-%b-%Y %H:%M:%S GMT", gmtime(expires))
    rest = getattr(cookie, "_rest", {})
    morsel.update({
        "expires": expires, 
        "path": cookie.path, 
        "comment": getattr(cookie, "comment", "") or "", 
        "domain": cookie.domain, 
        "max-age": getattr(cookie, "max_age", ""), 
        "secure": getattr(cookie, "secure", ""), 
        "httponly": getattr(cookie, "http_only", "") or rest.get("HttpOnly", ""), 
        "version": getattr(cookie, "version", ""), 
        "samesite": getattr(cookie, "same_site", "") or rest.get("SameSite", ""), 
    })
    return morsel


def morsel_to_cookie(cookie: Morsel, /) -> Cookie:
    if expires := cookie.get("expires"):
        if isinstance(expires, datetime):
            expires = expires.timestamp()
        elif isinstance(expires, str):
            expires = timegm(strptime(expires, "%a, %d-%b-%Y %H:%M:%S GMT"))
    elif max_age := cookie.get("max-age"):
        try:
            expires = int(time()) + int(max_age)
        except ValueError:
            raise TypeError(f"max-age: {max_age} must be integer")
    return create_cookie(
        comment=cookie["comment"], 
        comment_url=bool(cookie["comment"]), 
        discard=False, 
        domain=cookie["domain"], 
        expires=expires, 
        name=cookie.key, 
        path=cookie["path"], 
        rest={
            "HttpOnly": cookie["httponly"], 
            "SameSite": cookie["samesite"], 
        }, 
        secure=bool(cookie["secure"]), 
        value=cookie.value, 
        version=cookie["version"] or 0, 
    )


def cookies_str_to_dict(cookies: str, /) -> dict[str, str]:
    return dict(cookie.split("=", 1) for cookie in CRE_COOKIE_SEP_split(cookies) if cookie)


def cookies_dict_to_str(
    cookies: CookieJar | SimpleCookie | Mapping[str, Any] | Iterable[Any], 
    /, 
) -> str:
    if isinstance(cookies, CookieJar):
        cookie_it: Iterator[tuple[str, None | str]] = ((cookie.name, cookie.value) for cookie in cookies)
    elif isinstance(cookies, SimpleCookie):
        cookie_it = ((name, morsel.value) for name, morsel in cookies.items())
    elif isinstance(cookies, Mapping):
        cookie_it = ((name, cookie if isinstance(cookie, str) else cookie.value) for name, cookie in iter_items(cookies))
    else:
        cookie_it = (
            cookie[:2] if isinstance(cookie, tuple) else 
                ((cookie.key if isinstance(cookie, Morsel) else getattr(cookie, "name", "")), getattr(cookie, "value", None)) 
            for cookie in cookies
        )
    return "; ".join(f"{key}={val}" for key, val in cookie_it if key and val is not None)


def extract_cookies[T: (SimpleCookie, CookieJar)](
    cookies: T, 
    url: str, 
    response, 
) -> T:
    if hasattr(response, "headers"):
        headers = response.headers
    elif hasattr(response, "getheaders"):
        headers = response.getheaders()
    elif hasattr(response, "cookies"):
        update_cookies(cookies, response.cookies)
        return cookies
    elif isinstance(response, (CookieJar, SimpleCookie)):
        update_cookies(cookies, response)
        return cookies
    else:
        headers = response
    if isinstance(cookies, SimpleCookie):
        for key, val in iter_items(headers):
            key = key.lower()
            if key in (b"set-cookie", b"set-cookie2"):
                val = str(val, "latin-1")
            elif key not in ("set-cookie", "set-cookie2"):
                continue
            cookies.load(val)
    else:
        cookies.extract_cookies(response, Request(url)) # type: ignore
    return cookies


def update_cookies[T: (SimpleCookie, CookieJar)](
    cookies1: T, 
    cookies2, 
    /, 
) -> T:
    if isinstance(cookies1, SimpleCookie):
        if isinstance(cookies2, SimpleCookie):
            morsels: Iterable[tuple[str, Morsel]] = cookies2.items()
        elif isinstance(cookies2, CookieJar):
            morsels = ((cookie.name, to_morsel(cookie, name=cookie.name)) for cookie in cookies2)
        elif isinstance(cookies2, Mapping):
            morsels = ((k, v if isinstance(v, Morsel) else to_morsel(v, name=k)) for k, v in iter_items(cookies2))
        else:
            morsels = ((c.key, c) for c in (c if isinstance(c, Morsel) else to_morsel(c) for c in cookies2))
        cookies1.update(morsels)
    else:
        if isinstance(cookies2, SimpleCookie):
            cookies: Iterable = cookies2.values()
        elif isinstance(cookies2, CookieJar):
            cookies = cookies2
        elif isinstance(cookies2, Mapping):
            cookies = (to_cookie(v, name=k) for k, v in iter_items(cookies2))
        else:
            cookies = cookies2
        set_cookie = cookies1.set_cookie
        for cookie in cookies:
            if not isinstance(cookie, Cookie):
                cookie = to_cookie(cookie)
            set_cookie(cookie)
    return cookies1


def iter_resp_cookies(resp, /) -> Iterator[tuple[str, None | str]]:
    if hasattr(resp, "cookies"):
        cookies = resp.cookies
        if isinstance(cookies, CookieJar):
            for cookie in cookies:
                yield cookie.name, cookie.value
        elif isinstance(cookies, SimpleCookie):
            for name, morsel in cookies.items():
                yield name, morsel.value
        elif isinstance(cookies, Mapping):
            for name, cookie in iter_items(cookies):
                if not isinstance(cookie, str):
                    cookie = cookie.value
                yield name, cookie
        else:
            for cookie in cookies:
                if isinstance(cookie, Sequence):
                    yield cookie[0], cookie[1]
                elif isinstance(cookie, Morsel):
                    yield cookie.key, cookie.value
                else:
                    yield cookie.name, cookie.value
    else:
        if hasattr(resp, "headers"):
            headers = resp.headers
        elif hasattr(resp, "getheaders"):
            headers = resp.getheaders()
        else:
            return
        cookies = SimpleCookie()
        for k, v in iter_items(headers):
            k = k.lower()
            if isinstance(k, Buffer):
                k = str(k, "utf-8")
            if isinstance(v, Buffer):
                v = str(v, "utf-8")
            if k == "set-cookie" or k == "set-cookie2":
                cookies.load(v)
        for name, morsel in cookies.items():
            yield name, morsel.value

