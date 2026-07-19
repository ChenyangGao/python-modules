#!/usr/bin/env python3
# encoding: utf-8

__all__ = ["iter_page_threaded", "iter_page_async", "iter_page"]

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


def iter_page_threaded[T](
    call: Callable[[dict], T], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> Iterator[T]:
    """多线程并发拉取不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
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
    page = payload.setdefault(key_page, 1)
    payload[key_page_size] = page_size
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
                if check_for_stop(page, page_size, resp):
                    break
                else:
                    payload[key_page] += 1
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
            max_page: None | int = None
            reach_end = False
            future = make_future()
            while True:
                try:
                    resp = future.result(max(0, last_call_ts + cooldown - time()))
                except BaseException as e:
                    if not future.done():
                        if not reach_end:
                            payload[key_page] += 1
                            push((make_future(), payload[key_page]))
                        continue
                    if future.exception() is not e:
                        continue
                    if not retry_for_exception(e):
                        raise
                    push((make_future({**payload, key_page: page}), page))
                else:
                    yield resp
                    if check_for_stop(page, page_size, resp):
                        if max_page is None or max_page > page:
                            max_page = page
                        reach_end = True
                will_continue = False
                while dq:
                    future, page = pop()
                    if max_page is None or page < max_page:
                        will_continue = True
                        break
                    future.cancel()
                if will_continue:
                    continue
                elif reach_end:
                    break
                page = payload[key_page] = payload[key_page] + 1
                if max_page is not None and page > max_page:
                    break
                future = make_future()


async def iter_page_async[T](
    call: Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> AsyncIterator[T]:
    """异步并发拉取不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
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
    page = payload.setdefault(key_page, 1)
    payload[key_page_size] = page_size
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
                if check_for_stop(page, page_size, resp):
                    break
                else:
                    payload[key_page] += 1
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
            max_page: None | int = None
            reach_end = False
            task = make_task()
            while True:
                try:
                    resp = await wait_for(shield(task), max(0, last_call_ts + cooldown - time()))
                except BaseException as e:
                    if not task.done():
                        if not reach_end:
                            payload[key_page] += 1
                            push((make_task(), payload[key_page]))
                        continue
                    if task.exception() is not e:
                        continue
                    if not retry_for_exception(e):
                        exc = e
                        break
                    push((make_task({**payload, key_page: page}), page))
                else:
                    yield resp
                    if check_for_stop(page, page_size, resp):
                        if max_page is None or max_page > page:
                            max_page = page
                        reach_end = True
                will_continue = False
                while dq:
                    task, page = pop()
                    if max_page is None or page < max_page:
                        will_continue = True
                        break
                    task.cancel()
                if will_continue:
                    continue
                elif reach_end:
                    break
                page = payload[key_page] = payload[key_page] + 1
                if max_page is not None and page > max_page:
                    break
                task = make_task()
        if exc is not None:
            raise exc


@overload
def iter_page[T](
    call: Callable[[dict], T], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def iter_page[T](
    call: Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def iter_page[T](
    call: Callable[[dict], T] | Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """拉取不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制
    :param async_: 是否异步

    :return: 迭代器
    """
    return (iter_page_async if async_ else iter_page_threaded)(
        call, # type: ignore
        payload, 
        check_for_stop=check_for_stop, 
        retry_for_exception=retry_for_exception, 
        page_size=page_size, 
        key_page=key_page, 
        key_page_size=key_page_size, 
        cooldown=cooldown, 
        max_workers=max_workers, 
    )

