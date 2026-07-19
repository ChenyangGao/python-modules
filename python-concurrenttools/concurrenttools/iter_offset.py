#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["iter_offset_threaded", "iter_offset_async", "iter_offset"]

from asyncio import (
    shield, sleep as async_sleep, wait_for, 
    Semaphore as AsyncSemaphore, Task, TaskGroup, 
)
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
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
    """多线程并发拉取可随机定位的分页数据

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
        dq: deque[tuple[Future, int]] = deque()
        push, pop = dq.append, dq.popleft
        with killable_executor(ThreadPoolExecutor(max_workers)) as executor:
            submit = executor.submit
            def make_future(args: None | dict = None, /) -> Future:
                nonlocal last_call_ts
                if args is None:
                    args = copy(payload)
                last_call_ts = time()
                return submit(call, args)
            max_offset: None | int = None
            reach_end = False
            future = make_future()
            while True:
                try:
                    if cur_page_size == page_size:
                        resp = future.result(max(0, last_call_ts + cooldown - time()))
                    else:
                        resp = future.result()
                except BaseException as e:
                    if not future.done():
                        if not reach_end:
                            payload[key_offset] += cur_page_size
                            push((make_future(), payload[key_offset]))
                        continue
                    if future.exception() is not e:
                        continue
                    if not retry_for_exception(e):
                        raise
                    push((make_future({**payload, key_offset: offset}), offset))
                else:
                    yield resp
                    if check_for_stop(offset, cur_page_size, resp):
                        if max_offset is None or max_offset > offset:
                            max_offset = offset
                        reach_end = True
                    if cur_page_size != page_size:
                        cur_page_size = page_size
                        payload[key_limit] = page_size
                will_continue = False
                while dq:
                    future, offset = pop()
                    if max_offset is None or offset < max_offset:
                        will_continue = True
                        break
                    future.cancel()
                if will_continue:
                    continue
                elif reach_end:
                    break
                offset = payload[key_offset] = payload[key_offset] + page_size
                if max_offset is not None and offset > max_offset:
                    break
                future = make_future()


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
    """异步并发拉取可随机定位的分页数据

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
        dq: deque[tuple[Task, int]] = deque()
        push, pop = dq.append, dq.popleft
        exc: None | BaseException = None
        async with TaskGroup() as tg:
            create_task = tg.create_task
            def make_task(args: None | dict = None, /) -> Task:
                nonlocal last_call_ts
                if args is None:
                    args = copy(payload)
                last_call_ts = time()
                return create_task(ensure_coroutine(call(args)))
            max_offset: None | int = None
            reach_end = False
            task = make_task()
            while True:
                try:
                    if cur_page_size == page_size:
                        resp = await wait_for(shield(task), max(0, last_call_ts + cooldown - time()))
                    else:
                        resp = await task
                except BaseException as e:
                    if not task.done():
                        if not reach_end:
                            payload[key_offset] += cur_page_size
                            push((make_task(), payload[key_offset]))
                        continue
                    if task.exception() is not e:
                        continue
                    if not retry_for_exception(e):
                        exc = e
                        break
                    push((make_task({**payload, key_offset: offset}), offset))
                else:
                    yield resp
                    if check_for_stop(offset, cur_page_size, resp):
                        if max_offset is None or max_offset > offset:
                            max_offset = offset
                        reach_end = True
                    if cur_page_size != page_size:
                        cur_page_size = page_size
                        payload[key_limit] = page_size
                will_continue = False
                while dq:
                    task, offset = pop()
                    if max_offset is None or offset < max_offset:
                        will_continue = True
                        break
                    task.cancel()
                if will_continue:
                    continue
                elif reach_end:
                    break
                offset = payload[key_offset] = payload[key_offset] + page_size
                if max_offset is not None and offset > max_offset:
                    break
                task = make_task()
        if exc is not None:
            raise exc


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
    """并发拉取可随机定位的分页数据

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

