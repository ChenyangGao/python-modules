#!/usr/bin/env python3
# coding: utf-8

"""Tools for making decorators
"""

__author__  = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 4)
__all__ = ["decorated", "optional", "method_optional", "currying", "partialize"]

from collections.abc import Callable
from functools import update_wrapper as _update_wrapper
from inspect import signature
from typing import cast, overload, Any, Concatenate

from undefined import undefined


def bind[R](func: Callable[..., R], /, *args, **kwds) -> Callable[..., R]:
    return lambda *pargs, **kargs: func(*args, *pargs, **{**kwds, **kargs})


def update_wrapper(f, g, /):
    if f is g or not callable(f):
        return f
    return _update_wrapper(f, g)


def decorated[**Args, R, T](
    f: Callable[Concatenate[Callable[Args, R], Args], T] | Callable[Concatenate[Any, Callable[Args, R], Args], T], 
    /, 
) -> Callable[[Callable[Args, R]], Callable[Args, T]]:
    """Transform the 2-layers decorator into 1-layer.

    1. Decorator as a function

        .. code:: python
            @decorated
            def decorator(func, /, *args, **kwds):
                ...
                return func(*args, **kwds)

            # Roughly equivalent to:

            def decorator(func):
                def wrapper(*args, **kwds)
                    ...
                    return func(*args, **kwds)
                return functools.update_wrapper(wrapper, func)

    2. Decorator as an instance method 

        .. code:: python

            class Foo:
                @decorated
                def decorator(self, func, /, *args, **kwds):
                    ...
                    return func(*args, **kwds)

            # Roughly equivalent to:

            class Foo:
                def decorator(self, func):
                    def wrapper(*args, **kwds)
                        ...
                        return func(*args, **kwds)
                    return functools.update_wrapper(wrapper, func)

    3. Decorator as a class method 

        .. code:: python

            class Foo:
                @decorated
                @classmethod
                def decorator(cls, func, /, *args, **kwds):
                    ...
                    return func(*args, **kwds)

            # Roughly equivalent to:

            class Foo:
                @classmethod
                def decorator(cls, func):
                    def wrapper(*args, **kwds)
                        ...
                        return func(*args, **kwds)
                    return functools.update_wrapper(wrapper, func)
    """
    return update_wrapper(lambda *a: update_wrapper(bind(f, *a), a[-1]), f)


def optional[**Args, R](
    f: Callable[Args, R], 
    /, 
) -> Callable:
    """Transforming a decorator factory that accepts arguments (with defaults) 
    into a decorator that can be used with optional arguments.

    >>> @optional
    ... def foo(func, /, bar="bar", baz="baz"):
    ...     def wrapper(*args, **kwds):
    ...         print(bar)
    ...         r = func(*args, **kwds)
    ...         print(baz)
    ...         return r
    ...     return wrapper
    ... 
    >>> @foo 
    ... def baba(): 
    ...     print("baba")
    ... 
    >>> baba()
    bar
    baba
    baz
    >>> @foo(bar="bar1", baz="baz3")
    ... def baba2(): 
    ...     print("baba2") 
    ... 
    >>> baba2()
    bar1
    baba2
    baz3
    """
    nargs = 0
    for param in signature(f).parameters.values():
        if param.kind is param.POSITIONAL_ONLY:
            nargs += 1
        elif param.kind is param.POSITIONAL_OR_KEYWORD and param.default is param.empty:
            nargs += 1
        if nargs >= 2:
            break
    def wrapped(*args: Args.args, **kwds: Args.kwargs):
        len_args = len(args)
        if len_args >= nargs:
            return update_wrapper(f(*args, **kwds), args[-1])
        return bind(f, *args, **kwds)
    return update_wrapper(wrapped, f)


def currying[**Args, R](
    f: Callable[Args, R], 
    /, 
) -> Callable[Args, R]:
    """
    """
    bind_args = signature(f).bind
    def wrapper(*args, **kwds):
        try:
            bind_args(*args, **kwds)
        except TypeError as exc:
            if (exc.args 
                and isinstance(exc.args[0], str)
                and exc.args[0].startswith("missing a required")
            ):
                return bind(wrapper, *args, **kwds)
            raise
        return f(*args, **kwds)
    return update_wrapper(wrapper, f)


@overload
def partialize[**Args, R](
    f: None = None, 
    /, 
    sentinel: Any = undefined, 
) -> Callable[[Callable[Args, R]], Callable[Args, R]]:
    ...
@overload
def partialize[**Args, R](
    f: Callable[Args, R], 
    /, 
    sentinel: Any = undefined, 
) -> Callable[Args, R]:
    ...
def partialize[**Args, R](
    f: None | Callable[Args, R] = None, 
    /, 
    sentinel: Any = undefined, 
) -> Callable[Args, R] | Callable[[Callable[Args, R]], Callable[Args, R]]:
    """
    """
    if f is None:
        return cast(
            Callable[[Callable[Args, R]], Callable[Args, R]], 
            bind(partialize, sentinel=sentinel), 
        )
    bind_args = signature(f).bind
    def wrap(_paix, _pargs, _kargs, /):
        def wrapper(*args, **kwargs):
            pargs = _pargs.copy()
            j = len(pargs)
            for i, e in zip(_paix, args[j:]):
                pargs[i] = e
                j += 1
            pargs.extend(args[j:])
            try:
                bound = bind_args(*pargs, **kwargs)
            except TypeError as exc:
                if (exc.args 
                    and isinstance(exc.args[0], str)
                    and exc.args[0].startswith("missing a required")
                ):
                    return bind(wrapper, *args, **kwargs)
                raise
            else:
                bound.apply_defaults()
            if sentinel in bound.args or sentinel in bound.kwargs.values():
                return wrap(
                    [i for i, e in enumerate(args) if e is sentinel], 
                    list(args), kwargs)
            return f(*args, **kwargs)
        return bind(update_wrapper(wrapper, f), *_pargs, **_kargs)
    return wrap([], [], {})

