#!/usr/bin/env python3
# coding: utf-8

__author__  = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 1)
__all__ = [
    "is_eq", "hash_eq", "as_is", "callby", "call", "apply", 
    "bind", "compose", "chain", "pipe", "chain1", "pipe1", 
    "f_not", "f_swap", "repeat_call", "partialindex", 
]

from collections.abc import Callable, Iterable, Iterator, Mapping
from functools import partial, reduce, update_wrapper
from typing import overload, Any, Concatenate


def is_eq(x, y, /) -> bool:
    return x is y or x == y


def hash_eq(x, y, /) -> bool:
    return hash(x) == hash(y) and x == y


def as_is[T](x: T, /) -> T:
   return x


@overload
def callby[A, R](
    arg: A, 
    func: Callable[[A], R] | R, 
    /, 
    predicate: None = None, 
) -> R:
    ...
@overload
def callby[A, R](
    arg: A, 
    func: Callable[[A], R] | R,  
    /, 
    predicate: Callable[[A], bool], 
) -> A | R:
    ...
def callby[A, R](
    arg: A, 
    func: Callable[[A], R] | R,  
    /, 
    predicate: None | Callable[[A], bool] = None, 
) -> A | R:
    if not (predicate is None or predicate(arg)):
        return arg
    if callable(func):
        return func(arg)
    return func


def call[**Args, R](
    func: Callable[Args, R] | R, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> R:
    if callable(func):
        return func(*args, **kwds)
    return func


def apply[R](
  func: Callable[..., R] | R, 
  /, 
  args: Iterable = (), 
  kwargs: Mapping = {}, 
) -> R:
    if callable(func):
        return func(*args, **kwargs)
    return func


def bind[**Args, R](
  func: Callable[Args, R] | R, 
  /, 
  *args: Args.args, 
  **kwds: Args.kwargs, 
) -> Callable[..., R]:
    if callable(func):
        return partial(func, *args, **kwds)
    return lambda: func


def compose[**Args, R, T](
    f: Callable[Args, R], 
    g: Callable[[R], T], 
    /, 
) -> Callable[Args, T]:
    return lambda *a, **k: g(f(*a, **k))


def chain[**Args](f: Callable[Args, Any], *fs: Callable) -> Callable[Args, Any]:
    return lambda *a, **k: reduce(callby, fs, f(*a, **k))


def pipe[R](f: Callable[..., R], /, *fs: Callable) -> Callable[..., R]:
    return chain(*reversed(fs), f)


def chain1(*fs: Callable) -> Callable:
    return lambda x, /: reduce(callby, fs, x)


def pipe1(*fs: Callable) -> Callable:
    return chain1(*reversed(fs))


def f_not[**Args](
    func: Callable[Args, Any], 
    /, 
) -> Callable[Args, bool]:
    def wrapper(*args: Args.args, **kwds: Args.kwargs) -> bool:
        return not func(*args, **kwds)
    return update_wrapper(wrapper, func)


def f_swap[A1, A2, **Args, R](
    func: Callable[Concatenate[A1, A2, Args], R], 
) -> Callable[Concatenate[A2, A1, Args], R]:
    def wrapper(y: A2, x: A1, /, *args: Args.args, **kwds: Args.kwargs) -> R:
        return func(x, y, *args, **kwds)
    return update_wrapper(wrapper, func)


def repeat_call[R](func: Callable[[], R], times: int = -1, /) -> Iterator[R]:
    if times < 0:
        while True:
            yield func()
    else:
        for _ in range(times):
            yield func()


class partialindex:
    __slots__ = "func", "bound_args"

    def __init__(
        self, 
        /, 
        func: Callable, 
        bound_args: Mapping[int, Any] | Iterable[tuple[int, Any]], 
    ):
        if isinstance(func, partialindex):
            self.func: Callable = func.func
            self.bound_args: dict = dict(func.bound_args)
            self.bound_args.update(bound_args)
        else:
            self.func = func
            self.bound_args = dict(bound_args)

    def __call__(self, /, *args, **kwds):
        bound_args = self.bound_args
        if not bound_args:
            return self.func(*args, **kwds)
        pargs: list = []
        add_arg = pargs.append
        def load_bounds():
            try:
                while True:
                    add_arg(bound_args[len(pargs)])
            except KeyError:
                pass
        for _, arg in zip(repeat_call(load_bounds), args):
            add_arg(arg)
        if len(bound_args) + len(args) != len(pargs):
            from itertools import chain
            _ = type("", (), {"__repr__": staticmethod(lambda: "???")})()
            arg_str = ", ".join(f"[{i}]={v!r}" for i, v in enumerate(chain(
                pargs, 
                (bound_args.get(i, _) for i in range(len(pargs), max(bound_args)+1))
            )))
            raise TypeError(f"missing argument: found hollow index at {len(pargs)}\n    |_ {arg_str}")
        return self.func(*pargs, **kwds)

    def __repr__(self):
        cls = type(self)
        return f"{cls.__qualname__}.{cls.__module__}({self.func!r}, {self.bound_args!r})"

