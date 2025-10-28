#!/usr/bin/env python3
# coding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 10)
__all__ = ["DirEntry", "iterdir_generic", "walk_generic", "iterdir", "walk"]

from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
from datetime import datetime
from inspect import isasyncgenfunction, isawaitable
from os import (
    fspath, lstat, scandir, stat, stat_result, 
    DirEntry as _DirEntry, PathLike, 
)
from os.path import (
    abspath, commonpath, basename, isfile, isdir, 
    islink, realpath, 
)
from typing import cast, overload, Any, Final, Literal, Never

from asynctools import ensure_aiter
from texttools import format_mode, format_size


STAT_FIELDS: Final = (
    "mode", "ino", "dev", "nlink", "uid", "gid",
    "size", "atime", "mtime", "ctime", 
)
STAT_ST_FIELDS: Final = tuple(
    f for f in dir(stat_result) 
    if f.startswith("st_")
)


class DirEntryMeta(type):

    def __instancecheck__(cls, inst, /):
        if isinstance(inst, _DirEntry):
            return True
        return super().__instancecheck__(inst)

    def __subclasscheck__(cls, sub, /):
        if issubclass(sub, _DirEntry):
            return True
        return super().__subclasscheck__(sub)


class DirEntry[AnyStr: (bytes, str)](metaclass=DirEntryMeta):
    __slots__ = ("name", "path")

    name: AnyStr
    path: AnyStr

    def __init__(self, /, path: AnyStr | PathLike[AnyStr]):
        if isinstance(path, (_DirEntry, DirEntry)):
            name = path.name
            path = path.path
        else:
            path = fspath(path)
            name = basename(path)
            path = abspath(path)
        super().__setattr__("name", name)
        super().__setattr__("path", path)

    def __fspath__(self, /) -> AnyStr:
        return self.path

    def __repr__(self, /) -> str:
        return f"<{type(self).__qualname__} {self.path!r}>"

    def __setattr__(self, key, val, /) -> Never:
        raise AttributeError("can't set attributes")

    def inode(self, /) -> int:
        return lstat(self.path).st_ino

    def is_dir(self, /, *, follow_symlinks: bool = True) -> bool:
        if follow_symlinks:
            return isdir(self.path)
        else:
            return not islink(self.path) and isdir(self.path)

    def is_file(self, /, *, follow_symlinks: bool = True) -> bool:
        if follow_symlinks:
            return isfile(self.path)
        else:
            return islink(self.path) or isfile(self.path) 

    is_symlink = islink

    def stat(self, /, *, follow_symlinks: bool = True) -> stat_result:
        if follow_symlinks:
            return stat(self.path)
        else:
            return lstat(self.path)

    def stat_dict(
        self, 
        /, 
        *, 
        follow_symlinks: bool = True, 
        with_st: bool = False, 
    ) -> dict[str, Any]:
        stat = self.stat(follow_symlinks=follow_symlinks)
        if with_st:
            return dict(zip(STAT_ST_FIELDS, (getattr(stat, a) for a in STAT_ST_FIELDS)))
        else:
            return dict(zip(STAT_FIELDS, stat))

    def stat_info(self, /, *, follow_symlinks: bool = True) -> dict[str, Any]:
        stat_info: dict[str, Any] = self.stat_dict(follow_symlinks=follow_symlinks)
        stat_info["atime_str"] = str(datetime.fromtimestamp(stat_info["atime"]))
        stat_info["mtime_str"] = str(datetime.fromtimestamp(stat_info["mtime"]))
        stat_info["ctime_str"] = str(datetime.fromtimestamp(stat_info["ctime"]))
        stat_info["size_str"] = format_size(stat_info["size"])
        stat_info["mode_str"] = format_mode(stat_info["mode"])
        stat_info["path"] = self.path
        stat_info["name"] = self.name
        stat_info["is_dir"] = self.is_dir()
        stat_info["is_link"] = self.is_symlink()
        return stat_info


def _isdir(path, /) -> bool:
    is_dir = getattr(path, "isdir", None)
    if is_dir is None:
        is_dir = getattr(path, "is_dir", None)
    if callable(is_dir):
        is_dir = is_dir()
    return bool(is_dir)


async def _isdir_async(path, /) -> bool:
    is_dir = getattr(path, "isdir", None)
    if is_dir is None:
        is_dir = getattr(path, "is_dir", None)
    if callable(is_dir):
        is_dir = is_dir()
    if isawaitable(is_dir):
        is_dir = await is_dir
    return bool(is_dir)


def _iterdir_bfs[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]], 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] = None, 
    predicate: None | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> Iterator[P]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir
    dq: deque[tuple[int, Any]] = deque()
    push, pop = dq.append, dq.popleft
    push((0, top))
    while dq:
        depth, entry = pop()
        depth += 1
        can_step_in = max_depth < 0 or depth < max_depth
        try:
            for entry in iterdir(entry):
                pred = True if predicate is None else predicate(entry)
                if pred is 0:
                    continue
                if pred and depth >= min_depth:
                    yield entry
                if can_step_in and pred is not 1 and isdir(entry):
                    push((depth, entry))
        except OSError as e:
            if callable(onerror):
                onerror(e)
            elif onerror:
                raise


async def _iterdir_bfs_async[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    predicate: None | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> AsyncIterator[P]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir_async
    dq: deque[tuple[int, Any]] = deque()
    push, pop = dq.append, dq.popleft
    push((0, top))
    while dq:
        depth, entry = pop()
        depth += 1
        can_step_in = max_depth < 0 or depth < max_depth
        try:
            async for entry in ensure_aiter(iterdir(entry)):
                pred = True if predicate is None else predicate(entry)
                if isawaitable(pred):
                    pred = await pred
                if pred is 0:
                    continue
                if pred and depth >= min_depth:
                    yield entry
                if can_step_in and pred is not 1:
                    is_dir = isdir(entry)
                    if isawaitable(is_dir):
                        is_dir = await is_dir
                    if is_dir:
                        push((depth, entry))
        except OSError as e:
            if callable(onerror):
                ret = onerror(e)
                if isawaitable(ret):
                    await ret
            elif onerror:
                raise


def _iterdir_dfs[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]], 
    topdown: bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] = None, 
    predicate: None | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> Iterator[P]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir
    if min_depth > 1:
        global_yield_me = False
        min_depth -= 1
    else:
        global_yield_me = True
    try:
        max_depth -= max_depth > 0
        for entry in iterdir(top):
            pred = True if predicate is None else predicate(entry)
            if pred is 0:
                continue
            yield_me = global_yield_me and pred
            if yield_me and topdown:
                yield entry
            if max_depth and pred is not 1 and isdir(entry):
                yield from _iterdir_dfs(
                    entry, 
                    iterdir=iterdir, 
                    topdown=topdown, 
                    min_depth=min_depth, 
                    max_depth=max_depth, 
                    isdir=isdir, 
                    predicate=predicate, 
                    onerror=onerror, 
                )
            if yield_me and not topdown:
                yield entry
    except OSError as e:
        if callable(onerror):
            onerror(e)
        elif onerror:
            raise


async def _iterdir_dfs_async[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    topdown: bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    predicate: None | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> AsyncIterator[P]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir_async
    if min_depth > 1:
        global_yield_me = False
        min_depth -= 1
    else:
        global_yield_me = True
    try:
        max_depth -= max_depth > 0
        async for entry in ensure_aiter(iterdir(top)):
            pred = True if predicate is None else predicate(entry)
            if isawaitable(pred):
                pred = await pred
            if pred is 0:
                continue
            yield_me = global_yield_me and pred
            if yield_me and topdown:
                yield entry
            if max_depth and pred is not 1:
                is_dir = isdir(entry)
                if isawaitable(is_dir):
                    is_dir = await is_dir
                if is_dir:
                    async for subentry in _iterdir_dfs_async(
                        entry, 
                        iterdir=iterdir, 
                        topdown=topdown, 
                        min_depth=min_depth, 
                        max_depth=max_depth, 
                        isdir=isdir, 
                        predicate=predicate, 
                        onerror=onerror, 
                    ):
                        yield subentry
            if yield_me and not topdown:
                yield entry
    except OSError as e:
        if callable(onerror):
            ret = onerror(e)
            if isawaitable(ret):
                await ret
        elif onerror:
            raise


@overload
def iterdir_generic[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]], 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] = None, 
    predicate: None | bool | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[P]:
    ...
@overload
def iterdir_generic[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    predicate: None | bool | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[P]:
    ...
def iterdir_generic[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    predicate: None | bool | Callable[[P], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[P] | AsyncIterator[P]:
    """遍历目录树

    :param top: 顶层目录
    :param iterdir: 从目录中获取所有的直属的文件和目录
    :param topdown: 如果是 True，自顶向下深度优先搜索；如果是 False，自底向上深度优先搜索；如果是 None，广度优先搜索
    :param min_depth: 最小深度，`top` 本身为 0
    :param max_depth: 最大深度，< 0 时不限
    :param isdir: 判断是不是目录
    :param predicate: 调用以筛选遍历得到的路径

        - 如果为 None，则不做筛选
        - 如果为 True，则只输出文件
        - 如果为 False，则只输出目录
        - 否则，就是 Callable。对于返回值
            - 如果为 True，则输出它，且会继续搜索它的子树
            - 如果为 False，则跳过它，但会继续搜索它的子树
            - 如果为 1，则输出它，但跳过它的子树
            - 如果为 0，则跳过它，且跳过它的子树

    :param onerror: 处理 OSError 异常。如果是 True，抛出异常；如果是 False，忽略异常；如果是调用，以异常为参数调用之
    :param async_: 是否异步

    :return: 遍历得到的文件或目录的迭代器
    """
    if isasyncgenfunction(iterdir):
        async_ = True
    if isinstance(predicate, bool):
        if async_:
            if isdir is None:
                isdir = _isdir_async
            if predicate is True:
                async def predicate(e, /):
                    is_dir = cast(Callable, isdir)(e)
                    if isawaitable(is_dir):
                        is_dir = await is_dir
                    return not is_dir
            elif predicate is False:
                predicate = isdir
        else:
            if isdir is None:
                isdir = _isdir
            if predicate is True:
                predicate = lambda e: not isdir(e)
            elif predicate is False:
                predicate = isdir
    if topdown is None:
        if async_:
            return _iterdir_bfs_async(
                top, 
                iterdir=iterdir, 
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, 
                predicate=predicate, 
                onerror=onerror, 
            )
        else:
            return _iterdir_bfs(
                top, 
                iterdir=iterdir, # type: ignore
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, # type: ignore
                predicate=predicate, 
                onerror=onerror, 
            )
    else:
        if async_:
            return _iterdir_dfs_async(
                top, 
                iterdir=iterdir, 
                topdown=topdown, 
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, 
                predicate=predicate, 
                onerror=onerror, 
            )
        else:
            return _iterdir_dfs(
                top, 
                iterdir=iterdir, # type: ignore
                topdown=topdown, 
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, # type: ignore
                predicate=predicate, 
                onerror=onerror, 
            )


def _walk_bfs[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]], 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> Iterator[tuple[Any, list[P], list[P]]]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir
    dq: deque[tuple[int, Any]] = deque()
    push, pop = dq.append, dq.popleft
    push((0, top))
    while dq:
        depth, entry = pop()
        depth += 1
        can_step_in = max_depth < 0 or depth < max_depth
        try:
            files: list[P] = []
            dirs: list[P] = []
            for entry in iterdir(entry):
                if isdir(entry):
                    if can_step_in:
                        push((depth, entry))
                    dirs.append(entry)
                else:
                    files.append(entry)
        except OSError as e:
            if callable(onerror):
                onerror(e)
            elif onerror:
                raise
        else:
            if depth >= min_depth:
                yield top, dirs, files


async def _walk_bfs_async[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> AsyncIterator[tuple[Any, list[P], list[P]]]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir_async
    dq: deque[tuple[int, Any]] = deque()
    push, pop = dq.append, dq.popleft
    push((0, top))
    while dq:
        depth, entry = pop()
        depth += 1
        can_step_in = max_depth < 0 or depth < max_depth
        try:
            files: list[P] = []
            dirs: list[P] = []
            async for entry in ensure_aiter(iterdir(entry)):
                is_dir = isdir(entry)
                if isawaitable(is_dir):
                    is_dir = await is_dir
                if is_dir:
                    if can_step_in:
                        push((depth, entry))
                    dirs.append(entry)
                else:
                    files.append(entry)
            if depth >= min_depth:
                yield top, dirs, files
        except OSError as e:
            if callable(onerror):
                ret = onerror(e)
                if isawaitable(ret):
                    await ret
            elif onerror:
                raise


def _walk_dfs[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]], 
    topdown: bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> Iterator[tuple[Any, list[P], list[P]]]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir
    if min_depth > 1:
        yield_me = False
        min_depth -= 1
    else:
        yield_me = True
    try:
        files: list[P] = []
        dirs: list[P] = []
        for entry in iterdir(top):
            if isdir(entry):
                dirs.append(entry)
            else:
                files.append(entry)
    except OSError as e:
        if callable(onerror):
            onerror(e)
        elif onerror:
            raise
    else:
        if yield_me and topdown:
            yield top, dirs, files
        max_depth -= max_depth > 0
        if max_depth:
            for entry in dirs:
                yield from _walk_dfs(
                    entry, 
                    iterdir=iterdir, 
                    topdown=topdown, 
                    min_depth=min_depth, 
                    max_depth=max_depth, 
                    isdir=isdir, 
                    onerror=onerror, 
                )
        if yield_me and not topdown:
            yield top, dirs, files


async def _walk_dfs_async[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    topdown: bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
) -> AsyncIterator[tuple[Any, list[P], list[P]]]:
    if not max_depth or min_depth > max_depth > 0:
        return
    if isdir is None:
        isdir = _isdir_async
    if min_depth > 1:
        yield_me = False
        min_depth -= 1
    else:
        yield_me = True
    try:
        files: list[P] = []
        dirs: list[P] = []
        async for entry in ensure_aiter(iterdir(top)):
            is_dir = isdir(entry)
            if isawaitable(is_dir):
                is_dir = await is_dir
            if is_dir:
                dirs.append(entry)
            else:
                files.append(entry)
    except OSError as e:
        if callable(onerror):
            ret = onerror(e)
            if isawaitable(ret):
                await ret
        elif onerror:
            raise
    else:
        if yield_me and topdown:
            yield top, dirs, files
        max_depth -= max_depth > 0
        if max_depth:
            for entry in dirs:
                async for subentry in _walk_dfs_async(
                    entry, 
                    iterdir=iterdir, 
                    topdown=topdown, 
                    min_depth=min_depth, 
                    max_depth=max_depth, 
                    isdir=isdir, 
                    onerror=onerror, 
                ):
                    yield subentry
        if yield_me and not topdown:
            yield top, dirs, files


@overload
def walk_generic[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]], 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[tuple[Any, list[P], list[P]]]:
    ...
@overload
def walk_generic[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[tuple[Any, list[P], list[P]]]:
    ...
def walk_generic[P](
    top, 
    /, 
    iterdir: Callable[..., Iterable[P]] | Callable[..., AsyncIterable[P]], 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = 1, 
    isdir: None | Callable[[P], bool] | Callable[[P], Awaitable[bool]] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[tuple[Any, list[P], list[P]]] | AsyncIterator[tuple[Any, list[P], list[P]]]:
    """遍历目录树

    :param top: 顶层目录
    :param iterdir: 从目录中获取所有的直属的文件和目录
    :param topdown: 如果是 True，自顶向下深度优先搜索；如果是 False，自底向上深度优先搜索；如果是 None，广度优先搜索
    :param min_depth: 最小深度，`top` 本身为 0
    :param max_depth: 最大深度，< 0 时不限
    :param isdir: 判断是不是目录
    :param onerror: 处理 OSError 异常。如果是 True，抛出异常；如果是 False，忽略异常；如果是调用，以异常为参数调用之
    :param async_: 是否异步

    :return: 遍历得到 (父目录, 文件列表, 目录列表) 的 3 元组迭代器
    """
    if isasyncgenfunction(iterdir):
        async_ = True
    if topdown is None:
        if async_:
            return _walk_bfs_async(
                top, 
                iterdir=iterdir, 
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, 
                onerror=onerror, 
            )
        else:
            return _walk_bfs(
                top, 
                iterdir=iterdir, # type: ignore
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, # type: ignore
                onerror=onerror, 
            )
    else:
        if async_:
            return _walk_dfs_async(
                top, 
                iterdir=iterdir, 
                topdown=topdown, 
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, 
                onerror=onerror, 
            )
        else:
            return _walk_dfs(
                top, 
                iterdir=iterdir, # type: ignore
                topdown=topdown, 
                min_depth=min_depth, 
                max_depth=max_depth, 
                isdir=isdir, # type: ignore
                onerror=onerror, 
            )


@overload
def iterdir(
    top: None = None, 
    /, 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = -1, 
    predicate: None | bool | Callable[[DirEntry[str]], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    follow_symlinks: bool = False, 
) -> Iterator[DirEntry[str]]:
    ...
@overload
def iterdir[AnyStr: (bytes, str)](
    top: AnyStr | PathLike[AnyStr], 
    /, 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = -1, 
    predicate: None | bool | Callable[[DirEntry[AnyStr]], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    follow_symlinks: bool = False, 
) -> Iterator[DirEntry[AnyStr]]:
    ...
def iterdir[AnyStr: (bytes, str)](
    top: None | AnyStr | PathLike[AnyStr] = None, 
    /, 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = -1, 
    predicate: None | bool | Callable[[DirEntry[AnyStr]], Any] = None, 
    onerror: bool | Callable[[OSError], Any] = False, 
    follow_symlinks: bool = False, 
) -> Iterator[DirEntry]:
    """遍历目录树

    :param top: 顶层目录路径，默认为当前工作目录
    :param topdown: 如果是 True，自顶向下深度优先搜索；如果是 False，自底向上深度优先搜索；如果是 None，广度优先搜索
    :param min_depth: 最小深度，`top` 本身为 0
    :param max_depth: 最大深度，< 0 时不限
    :param predicate: 调用以筛选遍历得到的路径

        - 如果为 None，则不做筛选
        - 如果为 True，则只输出文件
        - 如果为 False，则只输出目录
        - 否则，就是 Callable。对于返回值
            - 如果为 True，则输出它，且会继续搜索它的子树
            - 如果为 False，则跳过它，但会继续搜索它的子树
            - 如果为 1，则输出它，但跳过它的子树
            - 如果为 0，则跳过它，且跳过它的子树

    :param onerror: 处理 OSError 异常。如果是 True，抛出异常；如果是 False，忽略异常；如果是调用，以异常为参数调用之
    :param follow_symlinks: 是否跟进符号连接（如果为 False，则会把符号链接视为文件，即使它指向目录）

    :return: 遍历得到的路径的迭代器
    """
    if top is None:
        top = cast(DirEntry[AnyStr], DirEntry("."))
    else:
        top = DirEntry(top)
    if follow_symlinks:
        realtop = realpath(top)
        isdir = lambda e, /: e.is_dir(follow_symlinks=follow_symlinks) and \
                             commonpath((t := (realtop, realpath(e)))) not in t
    else:
        isdir = lambda e, /: e.is_dir(follow_symlinks=follow_symlinks)
    return iterdir_generic(
        top, 
        iterdir=scandir, # type: ignore
        topdown=topdown, 
        min_depth=min_depth, 
        max_depth=max_depth, 
        isdir=isdir, 
        predicate=predicate, 
        onerror=onerror, 
    )


@overload
def walk(
    top: None = None, 
    /, 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = -1, 
    onerror: bool | Callable[[OSError], Any] = False, 
    follow_symlinks: bool = False, 
) -> Iterator[tuple[DirEntry[str], list[DirEntry[str]], list[DirEntry[str]]]]:
    ...
@overload
def walk[AnyStr: (bytes, str)](
    top: AnyStr | PathLike[AnyStr], 
    /, 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = -1, 
    onerror: bool | Callable[[OSError], Any] = False, 
    follow_symlinks: bool = False, 
) -> Iterator[tuple[DirEntry[AnyStr], list[DirEntry[AnyStr]], list[DirEntry[AnyStr]]]]:
    ...
def walk[AnyStr: (bytes, str)](
    top: None | AnyStr | PathLike[AnyStr] = None, 
    /, 
    topdown: None | bool = True, 
    min_depth: int = 1, 
    max_depth: int = -1, 
    onerror: bool | Callable[[OSError], Any] = False, 
    follow_symlinks: bool = False, 
) -> Iterator[tuple[DirEntry, list[DirEntry], list[DirEntry]]]:
    """遍历目录树

    :param top: 顶层目录路径，默认为当前工作目录
    :param topdown: 如果是 True，自顶向下深度优先搜索；如果是 False，自底向上深度优先搜索；如果是 None，广度优先搜索
    :param min_depth: 最小深度，`top` 本身为 0
    :param max_depth: 最大深度，< 0 时不限
    :param onerror: 处理 OSError 异常。如果是 True，抛出异常；如果是 False，忽略异常；如果是调用，以异常为参数调用之
    :param follow_symlinks: 是否跟进符号连接（如果为 False，则会把符号链接视为文件，即使它指向目录）

    :return: 遍历得到 (父目录, 文件列表, 目录列表) 的 3 元组迭代器
    """
    if top is None:
        top = cast(DirEntry[AnyStr], DirEntry("."))
    else:
        top = DirEntry(top)
    if follow_symlinks:
        realtop = realpath(top)
        isdir = lambda e, /: e.is_dir(follow_symlinks=follow_symlinks) and \
                             commonpath((t := (realtop, realpath(e)))) not in t
    else:
        isdir = lambda e, /: e.is_dir(follow_symlinks=follow_symlinks)
    return walk_generic(
        top, 
        iterdir=scandir, # type: ignore
        topdown=topdown, 
        min_depth=min_depth, 
        max_depth=max_depth, 
        isdir=isdir, 
        onerror=onerror, 
    )

