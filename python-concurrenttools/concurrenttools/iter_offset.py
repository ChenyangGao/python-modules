#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "iter_offset_threaded", "iter_offset_async", "iter_offset", 
    "iter_offset_multi_threaded", "iter_offset_multi_async", 
    "iter_offset_multi", 
]

from asyncio import (
    create_task, shield, sleep as async_sleep, wait_for, TimeoutError as AsyncTimeoutError, 
    Semaphore as AsyncSemaphore, Task, 
)
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from copy import copy
from time import sleep, time
from typing import cast, overload, Literal

from asynctools import ensure_coroutine

from .basic import killable_executor


def iter_offset_threaded[T](
    call: Callable[[dict], T], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    first_page_size: int = 0, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> Iterator[T]:
    """多线程拉取可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是开始索引、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param first_page_size: 第 1 次拉取的分页大小，如果指定此参数且不等于 ``page_size``，则会等待这次请求返回，才会开始后续
    :param key_offset: 偏移索引字段，索引默认从 0 开始
    :param key_limit: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制

    :return: 迭代器，产生每次请求的数据（可能乱序）
    """
    assert page_size > 0
    if first_page_size <= 0:
        first_page_size = page_size
    if max_workers and max_workers < 0:
        max_workers = None
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    offset = payload.setdefault(key_offset, 0)
    payload[key_limit] = first_page_size
    cur_page_size = first_page_size
    last_call_ts: float = 0
    if max_workers == 0:
        while True:
            try:
                if cooldown > 0 and (delta := last_call_ts + cooldown - time()) > 0:
                    sleep(delta)
                resp = call(payload)
                last_call_ts = time()
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if check_for_stop(offset, cur_page_size, resp):
                    break
                else:
                    offset = payload[key_offset] = payload[key_offset] + cur_page_size
                    if cur_page_size != page_size:
                        payload[key_limit] = page_size
                        cur_page_size = page_size
    else:
        with killable_executor(ThreadPoolExecutor(max_workers)) as executor:
            submit = executor.submit
            def make_future(payload: dict, /) -> Future:
                nonlocal last_call_ts
                last_call_ts = time()
                return submit(call, copy(payload))
            dq: deque[tuple[Future, int]] = deque()
            push, pop = dq.append, dq.popleft
            push((make_future(payload), offset))
            max_offset: None | int = None
            while dq:
                future, offset = pop()
                try:
                    if cur_page_size == page_size:
                        resp = future.result(max(0, last_call_ts + cooldown - time()))
                    else:
                        resp = future.result()
                except BaseException as e:
                    if future.done():
                        if future.exception() is not e:
                            if not isinstance(e, TimeoutError):
                                raise
                            push((future, offset))
                            continue
                        if not retry_for_exception(e):
                            raise
                        push((make_future({**payload, key_offset: offset}), offset))
                    elif isinstance(e, TimeoutError):
                        push((future, offset))
                    else:
                        raise
                else:
                    yield resp
                    if check_for_stop(offset, cur_page_size, resp):
                        if max_offset is None or max_offset > offset:
                            max_offset = offset
                            for _ in range(len(dq)):
                                future, offset = pop()
                                if offset > max_offset:
                                    future.cancel()
                                else:
                                    push((future, offset))
                    if cur_page_size != page_size:
                        cur_page_size = page_size
                        payload[key_limit] = page_size
                if max_offset is None:
                    payload[key_offset] += page_size
                    push((make_future(payload), payload[key_offset]))


async def iter_offset_async[T](
    call: Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    first_page_size: int = 0, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> AsyncIterator[T]:
    """异步拉取可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是开始索引、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param first_page_size: 第 1 次拉取的分页大小，如果指定此参数且不等于 ``page_size``，则会等待这次请求返回，才会开始后续
    :param key_offset: 偏移索引字段，索引默认从 0 开始
    :param key_limit: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则无数量限制

    :return: 异步迭代器，产生每次请求的数据（可能乱序）
    """
    assert page_size > 0
    if first_page_size <= 0:
        first_page_size = page_size
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    offset = payload.setdefault(key_offset, 0)
    payload[key_limit] = first_page_size
    cur_page_size = first_page_size
    last_call_ts: float = 0
    if max_workers == 0:
        while True:
            try:
                if cooldown > 0 and (delta := last_call_ts + cooldown - time()) > 0:
                    await async_sleep(delta)
                resp = await call(payload)
                last_call_ts = time()
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if check_for_stop(offset, cur_page_size, resp):
                    break
                else:
                    offset = payload[key_offset] = payload[key_offset] + cur_page_size
                    if cur_page_size != page_size:
                        cur_page_size = page_size
                        payload[key_limit] = page_size
    else:
        if not (max_workers is None or max_workers < 0):
            sema = AsyncSemaphore(max_workers)
            async def call(payload: dict, /, call=call) -> T:
                async with sema:
                    return await call(payload)
        def make_task(payload: dict, /) -> Task:
            nonlocal last_call_ts
            last_call_ts = time()
            return create_task(ensure_coroutine(call(payload)))
        dq: deque[tuple[Task, int]] = deque()
        push, pop = dq.append, dq.popleft
        push((make_task(payload), offset))
        max_offset: None | int = None
        while dq:
            task, offset = pop()
            try:
                if cur_page_size == page_size:
                    resp = await wait_for(shield(task), max(0, last_call_ts + cooldown - time()))
                else:
                    resp = await task
            except BaseException as e:
                if task.done():
                    if task.exception() is not e:
                        if not isinstance(e, AsyncTimeoutError):
                            raise
                        push((task, offset))
                        continue
                    if not retry_for_exception(e):
                        raise e
                    push((make_task({**payload, key_offset: offset}), offset))
                elif isinstance(e, AsyncTimeoutError):
                    push((task, offset))
                else:
                    raise
            else:
                yield resp
                if check_for_stop(offset, cur_page_size, resp):
                    max_offset = offset
                    for _ in range(len(dq)):
                        task, offset = pop()
                        if offset > max_offset:
                            task.cancel()
                        else:
                            push((task, offset))
                if cur_page_size != page_size:
                    cur_page_size = page_size
                    payload[key_limit] = page_size
            if max_offset is None:
                payload[key_offset] += page_size
                push((make_task(payload), payload[key_offset]))


@overload
def iter_offset[T](
    call: Callable[[dict], T], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    first_page_size: int = 0, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def iter_offset[T](
    call: Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    first_page_size: int = 0, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def iter_offset[T](
    call: Callable[[dict], T] | Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    first_page_size: int = 0, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """拉取可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是开始索引、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param first_page_size: 第 1 次拉取的分页大小，如果指定此参数且不等于 ``page_size``，则会等待这次请求返回，才会开始后续
    :param key_offset: 偏移索引字段，索引默认从 0 开始
    :param key_limit: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则无数量限制
    :param async_: 是否一步

    :return: 迭代器，产生每次请求的数据（可能乱序）
    """
    return (iter_offset_async if async_ else iter_offset_threaded)(
        call, # type: ignore
        payload, 
        check_for_stop=check_for_stop, 
        retry_for_exception=retry_for_exception, 
        page_size=page_size, 
        first_page_size=first_page_size, 
        key_offset=key_offset, 
        key_limit=key_limit, 
        cooldown=cooldown, 
        max_workers=max_workers, 
    )


def iter_offset_multi_threaded[T](
    call: Callable[[dict], T], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> Iterator[T]:
    """多线程拉取多个可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param iter_payload: 迭代以获取请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param payload: 请求的参数
    :param page_size: 分页大小
    :param key_offset: 偏移索引字段，索引默认从 0 开始
    :param key_limit: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制

    :return: 迭代器
    """
    assert page_size > 0
    if max_workers and max_workers < 0:
        max_workers = None
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    last_call_ts: float = 0
    dq_payload = deque(iter_payload)
    if not dq_payload:
        return
    for payload_ in dq_payload:
        if payload:
            payload_.update(payload)
        payload_.setdefault(key_offset, 0)
        payload_[key_limit] = page_size
    get_payload, put_payload = dq_payload.popleft, dq_payload.append
    if max_workers == 0:
        while dq_payload:
            payload = get_payload()
            try:
                if cooldown > 0 and (delta := last_call_ts + cooldown - time()) > 0:
                    sleep(delta)
                resp = call(payload)
                last_call_ts = time()
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if not check_for_stop(payload[key_offset], page_size, resp):
                    payload[key_offset] += page_size
                    put_payload(payload)
    else:
        def _iter_payload():
            while dq_payload:
                payload = get_payload()
                if d_max_page.get(id(payload)) is None:
                    payload[key_offset] += page_size
                    yield payload
                    put_payload(payload)
        next_payload = _iter_payload().__next__
        with killable_executor(ThreadPoolExecutor(max_workers)) as executor:
            submit = executor.submit
            def make_future(payload: dict, /) -> Future:
                nonlocal last_call_ts
                last_call_ts = time()
                return submit(call, copy(payload))
            dq_futures: deque[tuple[Future, int, dict]] = deque()
            get_future, put_future = dq_futures.popleft, dq_futures.append
            while dq_payload:
                payload = get_payload()
                put_future((make_future(payload), payload[key_offset], payload))
            d_max_page: dict[int, int] = {}
            while dq_futures:
                future, offset, payload = get_future()
                try:
                    resp = future.result(max(0, last_call_ts + cooldown - time()))
                except BaseException as e:
                    if future.done():
                        if future.exception() is not e:
                            if not isinstance(e, TimeoutError):
                                raise
                            put_future((future, offset, payload))
                            continue
                        if not retry_for_exception(e):
                            raise
                        put_future((make_future({**payload, key_offset: offset}), offset, payload))
                    elif isinstance(e, TimeoutError):
                        put_future((future, offset, payload))
                    else:
                        raise
                else:
                    yield resp
                    if check_for_stop(offset, page_size, resp):
                        max_page = d_max_page.get(id(payload))
                        if max_page is None or max_page > offset:
                            max_page = d_max_page[id(payload)] = offset
                            for _ in range(len(dq_futures)):
                                future, offset, payload_ = get_future()
                                if payload is payload_ and offset > max_page:
                                    future.cancel()
                                else:
                                    put_future((future, offset, payload_))
                with suppress(StopIteration):
                    payload = next_payload()
                    put_future((make_future(payload), payload[key_offset], payload))


async def iter_offset_multi_async[T](
    call: Callable[[dict], Awaitable[T]], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_offset = "offset", 
    key_limit = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> AsyncIterator[T]:
    """异步拉取多个可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param iter_payload: 迭代以获取请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param payload: 请求的参数
    :param page_size: 分页大小
    :param key_offset: 偏移索引字段，索引默认从 0 开始
    :param key_limit: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则无数量限制

    :return: 异步迭代器
    """
    assert page_size > 0
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    last_call_ts: float = 0
    dq_payload = deque(iter_payload)
    if not dq_payload:
        return
    for payload_ in dq_payload:
        if payload:
            payload_.update(payload)
        payload_.setdefault(key_offset, 0)
        payload_[key_limit] = page_size
    get_payload, put_payload = dq_payload.popleft, dq_payload.append
    if max_workers == 0:
        while dq_payload:
            payload = get_payload()
            try:
                if cooldown > 0 and (delta := last_call_ts + cooldown - time()) > 0:
                    await async_sleep(delta)
                resp = await call(payload)
                last_call_ts = time()
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if not check_for_stop(payload[key_offset], page_size, resp):
                    payload[key_offset] += page_size
                    put_payload(payload)
    else:
        def _iter_payload():
            while dq_payload:
                payload = get_payload()
                if d_max_page.get(id(payload)) is None:
                    payload[key_offset] += page_size
                    yield payload
                    put_payload(payload)
        next_payload = _iter_payload().__next__
        if not (max_workers is None or max_workers < 0):
            sema = AsyncSemaphore(max_workers)
            async def call(payload: dict, /, call=call) -> T:
                async with sema:
                    return await call(payload)
        def make_task(payload: dict, /) -> Task:
            nonlocal last_call_ts
            last_call_ts = time()
            return create_task(ensure_coroutine(call(payload)))
        dq_tasks: deque[tuple[Task, int, dict]] = deque()
        get_task, put_task = dq_tasks.popleft, dq_tasks.append
        while dq_payload:
            payload = get_payload()
            put_task((make_task(payload), payload[key_offset], payload))
        d_max_page: dict[int, int] = {}
        while dq_tasks:
            task, offset, payload = get_task()
            try:
                resp = await wait_for(shield(task), max(0, last_call_ts + cooldown - time()))
            except BaseException as e:
                if task.done():
                    if task.exception() is not e:
                        if not isinstance(e, AsyncTimeoutError):
                            raise
                        put_task((task, offset, payload))
                        continue
                    if not retry_for_exception(e):
                        raise e
                    put_task((make_task({**payload, key_offset: offset}), offset, payload))
                elif isinstance(e, AsyncTimeoutError):
                    put_task((task, offset, payload))
                else:
                    raise
            else:
                yield resp
                if check_for_stop(offset, page_size, resp):
                    max_page = d_max_page.get(id(payload))
                    if max_page is None or max_page > offset:
                        max_page = d_max_page[id(payload)] = offset
                        for _ in range(len(dq_tasks)):
                            task, offset, payload_ = get_task()
                            if payload is payload_ and offset > max_page:
                                task.cancel()
                            else:
                                put_task((task, offset, payload_))
            with suppress(StopIteration):
                payload = next_payload()
                put_task((make_task(payload), payload[key_offset], payload))


@overload
def iter_offset_multi[T](
    call: Callable[[dict], T], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def iter_offset_multi[T](
    call: Callable[[dict], Awaitable[T]], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def iter_offset_multi[T](
    call: Callable[[dict], T] | Callable[[dict], Awaitable[T]], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_offset = "offset", 
    key_limit = "limit", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """拉取多个可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param iter_payload: 迭代以获取请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param payload: 请求的参数
    :param page_size: 分页大小
    :param key_offset: 偏移索引字段，索引默认从 0 开始
    :param key_limit: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制
    :param async_: 是否异步

    :return: 迭代器
    """
    return (iter_offset_multi_async if async_ else iter_offset_multi_threaded)(
        call, # type: ignore
        iter_payload, 
        check_for_stop=check_for_stop, 
        retry_for_exception=retry_for_exception, 
        payload=payload, 
        page_size=page_size, 
        key_offset=key_offset, 
        key_limit=key_limit, 
        cooldown=cooldown, 
        max_workers=max_workers, 
    )

