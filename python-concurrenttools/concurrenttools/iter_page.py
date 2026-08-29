#!/usr/bin/env python3
# encoding: utf-8

__all__ = [
    "iter_page_threaded", "iter_page_async", "iter_page", 
    "iter_page_multi_threaded", "iter_page_multi_async", 
    "iter_page_multi", 
]

from asyncio import (
    sleep as async_sleep, CancelledError as AsyncCancelledError, 
    Lock as AsyncLock, Queue as AsyncQueue, Task, TaskGroup, 
)
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from inspect import isawaitable
from itertools import count, repeat
from os import cpu_count
from queue import Queue
from threading import Lock
from time import sleep, time
from typing import cast, overload, Literal

from argtools import argcount

from .basic import run_as_async, run_as_thread


def squeeze_range(min: int, max: int, /) -> Iterator[int]:
    while min <= max:
        yield min
        min += 1
        if min > max:
            break
        yield max
        max -= 1


def iter_page_threaded[T](
    call: Callable[[dict], T], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    max_page: int | Callable[[], int] | Callable[[T], int] = 0, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> Iterator[T]:
    """多线程拉取不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param max_page: 最大页数，如果 <= 0，则不作限定。如果可调用，不接受参数时，直接调用它以获取最大页数，否则，接受 ``call`` 的返回数据来获取最大页数
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制

    :return: 迭代器
    """
    assert page_size > 0
    if max_workers is None or max_workers < 0:
        max_workers = min(32, (cpu_count() or 1) + 4)
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    page = payload.setdefault(key_page, 1)
    payload[key_page_size] = page_size
    last_call_ts: float = 0
    get_max_page: None | Callable[[T], int] = None
    if max_workers == 0:
        if callable(max_page):
            if argcount(max_page):
                get_max_page = max_page # type: ignore
                max_page = 0
            else:
                max_page = max_page() # type: ignore
            max_page = cast(int, max_page)
        while max_page <= 0 or page <= max_page:
            try:
                if cooldown > 0:
                    delta = last_call_ts + cooldown - time()
                    if delta > 0:
                        sleep(delta)
                    last_call_ts = time()
                resp = call(payload)
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if max_page <= 0 and get_max_page:
                    max_page = get_max_page(resp)
                if not check_for_stop(page, page_size, resp):
                    page = payload[key_page] = payload[key_page] + 1
    else:
        if callable(max_page):
            if argcount(max_page):
                get_max_page = max_page # type: ignore
                max_page = 0
            else:
                fu = run_as_thread(max_page) # type: ignore
                max_page = 0
                fu.add_done_callback(lambda fu, /: fu.exception() is None and set_max_page(fu.result()))
            max_page = cast(int, max_page)
        task_list: list[None | Future] = []
        task_page: list[int] = []
        task_ids: set[int] = set()
        discard_task_id = task_ids.discard
        def countdown(task_id: int, /):
            task_list[task_id] = None
            discard_task_id(task_id)
        def set_max_page(page: int, /):
            nonlocal max_page
            max_page = cast(int, max_page)
            if 0 < max_page <= page:
                return
            max_page = page
            for i, p in enumerate(task_page):
                task = task_list[i]
                if task and p > page:
                    task.cancel()
                    countdown(i)
        def page_iter(start: int = payload[key_page], /):
            page = start - 1
            if max_page <= 0:
                for page in count(start):
                    yield page
                    if max_page > 0:
                        break
            yield from squeeze_range(page + 1, max_page)
        next_page = page_iter().__next__
        running = True
        def request(task_id: int, /):
            nonlocal running, last_call_ts
            try:
                retry = False
                while running:
                    if retry:
                        retry = False
                    else:
                        with lock:
                            page = next_page()
                    if 0 < max_page < page:
                        continue
                    task_page[task_id] = page
                    if cooldown > 0:
                        with lock:
                            delta = last_call_ts + cooldown - time()
                            if delta > 0:
                                sleep(delta)
                            if not running:
                                return
                            if 0 < max_page < page:
                                continue
                            last_call_ts = time()
                    try:
                        resp = call({**payload, key_page: page})
                    except BaseException as e:
                        if retry_for_exception(e):
                            retry = True
                        else:
                            put((False, e))
                            return
                    else:
                        if max_page <= 0 and get_max_page:
                            set_max_page(get_max_page(resp))
                        if check_for_stop(page, page_size, resp):
                            set_max_page(page)
                        put((True, resp))
            except (StopIteration, CancelledError):
                pass
            except BaseException as e:
                running = False
                put((False, e))
                for i, task in enumerate(task_list):
                    if task:
                        task.cancel()
                        countdown(i)
            finally:
                countdown(task_id)
                if not task_ids:
                    put(None)
        if max_page > 0:
            max_workers = min(max_page, max_workers)
        executor = ThreadPoolExecutor(max_workers)
        try:
            submit = executor.submit
            lock = Lock()
            q: Queue = Queue(max_workers)
            get, put = q.get, q.put
            task_ids.update(range(max_workers))
            task_list.extend(repeat(None, max_workers))
            task_page.extend(repeat(0, max_workers))
            for i in range(max_workers):
                task_list[i] = submit(request, i)
            while True:
                resp = get()
                if resp is None:
                    break
                status, result = resp
                if status:
                    yield result
                elif not isinstance(result, CancelledError):
                    raise result
        finally:
            running = False
            executor.shutdown(wait=False, cancel_futures=True)


async def iter_page_async[T](
    call: Callable[[dict], Awaitable[T]], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    max_page: int | Callable[[], int] | Callable[[], Awaitable[int]] | Callable[[T], int] = 0, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> AsyncIterator[T]:
    """异步拉取不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param payload: 请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param page_size: 分页大小
    :param max_page: 最大页数，如果 <= 0，则不作限定。如果可调用，不接受参数时，直接调用它以获取最大页数，否则，接受 ``call`` 的返回数据来获取最大页数
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则自动确定

    :return: 异步迭代器
    """
    assert page_size > 0
    if max_workers is None or max_workers < 0:
        max_workers = 32
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    page = payload.setdefault(key_page, 1)
    payload[key_page_size] = page_size
    last_call_ts: float = 0
    get_max_page: None | Callable[[T], int] = None
    if max_workers == 0:
        if callable(max_page):
            if argcount(max_page):
                get_max_page = max_page # type: ignore
                max_page = 0
            else:
                ret = max_page() # type: ignore
                if isawaitable(ret):
                    max_page = await ret
                else:
                    max_page = ret
            max_page = cast(int, max_page)
        while max_page <= 0 or page <= max_page:
            try:
                if cooldown > 0:
                    delta = last_call_ts + cooldown - time()
                    if delta > 0:
                        await async_sleep(delta)
                    last_call_ts = time()
                resp = await call(payload)
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if max_page <= 0 and get_max_page:
                    max_page = get_max_page(resp)
                if not check_for_stop(page, page_size, resp):
                    page = payload[key_page] = payload[key_page] + 1
    else:
        if callable(max_page):
            if argcount(max_page):
                get_max_page = max_page # type: ignore
                max_page = 0
            else:
                fu: Future[int] = run_as_async(max_page) # type: ignore
                max_page = 0
                fu.add_done_callback(lambda fu, /: fu.exception() is None and set_max_page(fu.result()))
            max_page = cast(int, max_page)
        task_list: list[None | Task] = []
        task_page: list[int] = []
        task_ids: set[int] = set()
        discard_task_id = task_ids.discard
        def countdown(task_id: int, /):
            task_list[task_id] = None
            discard_task_id(task_id)
        def set_max_page(page: int, /):
            nonlocal max_page
            max_page = cast(int, max_page)
            if 0 < max_page <= page:
                return
            max_page = page
            for i, p in enumerate(task_page):
                task = task_list[i]
                if task and p > page:
                    task.cancel()
                    countdown(i)
        def page_iter(start: int = payload[key_page], /):
            page = start - 1
            if max_page <= 0:
                for page in count(start):
                    yield page
                    if max_page > 0:
                        break
            yield from squeeze_range(page + 1, max_page)
        next_page = page_iter().__next__
        running = True
        async def request(task_id: int, /):
            nonlocal running, last_call_ts
            try:
                retry = False
                while running:
                    if retry:
                        retry = False
                    else:
                        page = next_page()
                    if 0 < max_page < page:
                        continue
                    task_page[task_id] = page
                    if cooldown > 0:
                        async with lock:
                            delta = last_call_ts + cooldown - time()
                            if delta > 0:
                                await async_sleep(delta)
                            if not running:
                                return
                            if 0 < max_page < page:
                                continue
                            last_call_ts = time()
                    try:
                        resp = await call({**payload, key_page: page})
                    except BaseException as e:
                        if retry_for_exception(e):
                            retry = True
                        else:
                            await put((False, e))
                            return
                    else:
                        if max_page <= 0 and get_max_page:
                            set_max_page(get_max_page(resp))
                        if check_for_stop(page, page_size, resp):
                            set_max_page(page)
                        await put((True, resp))
            except (StopIteration, AsyncCancelledError):
                pass
            except BaseException as e:
                running = False
                await put((False, e))
                for i, task in enumerate(task_list):
                    if task:
                        task.cancel()
                        countdown(i)
            finally:
                countdown(task_id)
                if not task_ids:
                    await put(None)
        exc: None | BaseException = None
        try:
            if max_page > 0:
                max_workers = min(max_page, max_workers)
            async with TaskGroup() as tg:
                create_task = tg.create_task
                lock = AsyncLock()
                q: AsyncQueue = AsyncQueue(max_workers)
                get, put = q.get, q.put
                task_ids.update(range(max_workers))
                task_list.extend(repeat(None, max_workers))
                task_page.extend(repeat(0, max_workers))
                for i in range(max_workers):
                    task_list[i] = create_task(request(i))
                while True:
                    resp = await get()
                    if resp is None:
                        break
                    status, result = resp
                    if status:
                        yield result
                    elif not isinstance(result, AsyncCancelledError):
                        exc = result
                        break
            if exc is not None:
                raise exc
        finally:
            running = False


@overload
def iter_page[T](
    call: Callable[[dict], T], 
    payload: dict, 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    page_size: int = 100, 
    max_page: int | Callable[[], int] | Callable[[T], int] = 0, 
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
    max_page: int | Callable[[], int] | Callable[[], Awaitable[int]] | Callable[[T], int] = 0, 
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
    max_page: int | Callable[[], int] | Callable[[], Awaitable[int]] | Callable[[T], int] = 0, 
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
    :param max_page: 最大页数，如果 <= 0，则不作限定。如果可调用，不接受参数时，直接调用它以获取最大页数，否则，接受 ``call`` 的返回数据来获取最大页数
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
        max_page=max_page, # type: ignore
        key_page=key_page, 
        key_page_size=key_page_size, 
        cooldown=cooldown, 
        max_workers=max_workers, 
    )


def iter_page_multi_threaded[T](
    call: Callable[[dict], T], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> Iterator[T]:
    """多线程拉取多个不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param iter_payload: 迭代以获取请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param payload: 请求的参数
    :param page_size: 分页大小
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制

    :return: 迭代器
    """
    assert page_size > 0
    if max_workers is None or max_workers < 0:
        max_workers = min(32, (cpu_count() or 1) + 4)
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception) 
    dq_payload = deque(iter_payload)
    if not dq_payload:
        return
    for payload_ in dq_payload:
        if payload:
            payload_.update(payload)
        payload_.setdefault(key_page, 1)
        payload_[key_page_size] = page_size
    get_payload, put_payload = dq_payload.popleft, dq_payload.append
    last_call_ts: float = 0
    if max_workers == 0:
        while dq_payload:
            payload = get_payload()
            try:
                if cooldown > 0:
                    delta = last_call_ts + cooldown - time()
                    if delta > 0:
                        sleep(delta)
                    last_call_ts = time()
                resp = call(payload)
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if not check_for_stop(payload[key_page], page_size, resp):
                    payload[key_page] += 1
                    put_payload(payload)
    else:
        task_list: list[None | Future] = []
        task_page: list[tuple[dict, int]] = []
        task_ids: set[int] = set()
        discard_task_id = task_ids.discard
        def countdown(task_id: int, /):
            task_list[task_id] = None
            discard_task_id(task_id)
        d_max_page: dict[int, int] = {}
        def set_max_page(payload: dict, page: int, /):
            id_ = id(payload)
            if 0 < d_max_page.get(id_, 0) <= page:
                return
            d_max_page[id_] = page
            for i, (payload_, page_) in enumerate(task_page):
                task = task_list[i]
                if task and payload is payload_ and page < page_:
                    task.cancel()
                    countdown(i)
        def next_payload_page_iter():
            page_idx = {id(p): p[key_page] for p in dq_payload}
            while page_idx:
                for payload in dq_payload:
                    id_ = id(payload)
                    if page := page_idx.get(id_):
                        if 0 < d_max_page.get(id_, 0) < page:
                            page_idx.pop(id_, None)
                        else:
                            yield payload, page
                            page_idx[id_] += 1
        next_payload_page = next_payload_page_iter().__next__
        running = True
        def request(task_id: int, /):
            nonlocal running, last_call_ts
            try:
                retry = False
                while running:
                    if retry:
                        retry = False
                    else:
                        with lock:
                            payload, page = next_payload_page()
                    if 0 < d_max_page.get(id(payload), 0) < page:
                        continue
                    task_page[task_id] = (payload, page)
                    if cooldown > 0:
                        with lock:
                            delta = last_call_ts + cooldown - time()
                            if delta > 0:
                                sleep(delta)
                            if not running:
                                return
                            if 0 < d_max_page.get(id(payload), 0) < page:
                                continue
                            last_call_ts = time()
                    try:
                        resp = call({**payload, key_page: page})
                    except BaseException as e:
                        if retry_for_exception(e):
                            retry = True
                        else:
                            put((False, e))
                            return
                    else:
                        if check_for_stop(page, page_size, resp):
                            set_max_page(payload, page)
                        put((True, resp))
            except (StopIteration, CancelledError):
                pass
            except BaseException as e:
                running = False
                put((False, e))
                for i, task in enumerate(task_list):
                    if task:
                        task.cancel()
                        countdown(i)
            finally:
                countdown(task_id)
                if not task_ids:
                    put(None)
        executor = ThreadPoolExecutor(max_workers)
        try:
            submit = executor.submit
            lock = Lock()
            q: Queue = Queue(max_workers)
            get, put = q.get, q.put
            task_ids.update(range(max_workers))
            task_list.extend(repeat(None, max_workers))
            task_page.extend([({}, 0)] * max_workers)
            for i in range(max_workers):
                task_list[i] = submit(request, i)
            while True:
                resp = get()
                if resp is None:
                    break
                status, result = resp
                if status:
                    yield result
                elif not isinstance(result, CancelledError):
                    raise result
        finally:
            running = False
            executor.shutdown(wait=False, cancel_futures=True)


async def iter_page_multi_async[T](
    call: Callable[[dict], Awaitable[T]], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
) -> AsyncIterator[T]:
    """异步拉取多个不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param iter_payload: 迭代以获取请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param payload: 请求的参数
    :param page_size: 分页大小
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则无数量限制

    :return: 异步迭代器
    """
    assert page_size > 0
    if max_workers is None or max_workers < 0:
        max_workers = 32
    if retry_for_exception is None:
        retry_for_exception = lambda _, /: False
    elif isinstance(retry_for_exception, type) and issubclass(retry_for_exception, BaseException) or isinstance(retry_for_exception, tuple):
        retry_for_exception = lambda e, excs=retry_for_exception, /: isinstance(e, excs)
    retry_for_exception = cast(Callable, retry_for_exception)
    dq_payload = deque(iter_payload)
    if not dq_payload:
        return
    for payload_ in dq_payload:
        if payload:
            payload_.update(payload)
        payload_.setdefault(key_page, 1)
        payload_[key_page_size] = page_size
    get_payload, put_payload = dq_payload.popleft, dq_payload.append
    last_call_ts: float = 0
    if max_workers == 0:
        while dq_payload:
            payload = get_payload()
            try:
                if cooldown > 0:
                    delta = last_call_ts + cooldown - time()
                    if delta > 0:
                        await async_sleep(delta)
                    last_call_ts = time()
                resp = await call(payload)
            except BaseException as e:
                if not retry_for_exception(e):
                    raise
            else:
                yield resp
                if not check_for_stop(payload[key_page], page_size, resp):
                    payload[key_page] += 1
                    put_payload(payload)
    else:
        task_list: list[None | Task] = []
        task_page: list[tuple[dict, int]] = []
        task_ids: set[int] = set()
        discard_task_id = task_ids.discard
        def countdown(task_id: int, /):
            task_list[task_id] = None
            discard_task_id(task_id)
        d_max_page: dict[int, int] = {}
        def set_max_page(payload: dict, page: int, /):
            id_ = id(payload)
            if 0 < d_max_page.get(id_, 0) <= page:
                return
            d_max_page[id_] = page
            for i, (payload_, page_) in enumerate(task_page):
                task = task_list[i]
                if task and payload is payload_ and page < page_:
                    task.cancel()
                    countdown(i)
        def next_payload_page_iter():
            page_idx = {id(p): p[key_page] for p in dq_payload}
            while page_idx:
                for payload in dq_payload:
                    id_ = id(payload)
                    if page := page_idx.get(id_):
                        if 0 < d_max_page.get(id_, 0) < page:
                            page_idx.pop(id_, None)
                        else:
                            yield payload, page
                            page_idx[id_] += 1
        next_payload_page = next_payload_page_iter().__next__
        running = True
        async def request(task_id: int, /):
            nonlocal running, last_call_ts
            try:
                retry = False
                while running:
                    if retry:
                        retry = False
                    else:
                        payload, page = next_payload_page()
                    if 0 < d_max_page.get(id(payload), 0) < page:
                        continue
                    task_page[task_id] = (payload, page)
                    if cooldown > 0:
                        async with lock:
                            delta = last_call_ts + cooldown - time()
                            if delta > 0:
                                await async_sleep(delta)
                            if not running:
                                return
                            if 0 < d_max_page.get(id(payload), 0) < page:
                                continue
                            last_call_ts = time()
                    try:
                        resp = await call({**payload, key_page: page})
                    except BaseException as e:
                        if retry_for_exception(e):
                            retry = True
                        else:
                            await put((False, e))
                            return
                    else:
                        if check_for_stop(page, page_size, resp):
                            set_max_page(payload, page)
                        await put((True, resp))
            except (StopIteration, CancelledError):
                pass
            except BaseException as e:
                running = False
                await put((False, e))
                for i, task in enumerate(task_list):
                    if task:
                        task.cancel()
                        countdown(i)
            finally:
                countdown(task_id)
                if not task_ids:
                    await put(None)
        exc: None | BaseException = None
        try:
            async with TaskGroup() as tg:
                create_task = tg.create_task
                lock = AsyncLock()
                q: AsyncQueue = AsyncQueue(max_workers)
                get, put = q.get, q.put
                task_ids.update(range(max_workers))
                task_list.extend(repeat(None, max_workers))
                task_page.extend([({}, 0)] * max_workers)
                for i in range(max_workers):
                    task_list[i] = create_task(request(i))
                while True:
                    resp = await get()
                    if resp is None:
                        break
                    status, result = resp
                    if status:
                        yield result
                    elif not isinstance(result, CancelledError):
                        exc = result
                        break
            if exc is not None:
                raise exc
        finally:
            running = False


@overload
def iter_page_multi[T](
    call: Callable[[dict], T], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
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
def iter_page_multi[T](
    call: Callable[[dict], Awaitable[T]], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def iter_page_multi[T](
    call: Callable[[dict], T] | Callable[[dict], Awaitable[T]], 
    iter_payload: Iterable[dict], 
    /, 
    check_for_stop: Callable[[int, int, T], bool], 
    retry_for_exception: None | Callable[[BaseException], bool] | type[BaseException] | tuple[type[BaseException], ...] = None, 
    payload: None | dict = None, 
    page_size: int = 100, 
    key_page = "page", 
    key_page_size = "page_size", 
    cooldown: float = 0, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """拉取多个不可随机定位的分页数据

    :param call: 调用请求以获取响应数据
    :param iter_payload: 迭代以获取请求的参数
    :param check_for_stop: 检查是否要停止（没有下一页了），接受 3 个参数，分别是分页编号、分页大小和响应数据
    :param retry_for_exception: 检查以决定是否要抛出异常
    :param payload: 请求的参数
    :param page_size: 分页大小
    :param key_page: 分页编号字段，数值从 1 开始
    :param key_page_size: 分页大小字段
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作协程数，如果为 None 或 < 0，则无数量限制
    :param async_: 是否异步

    :return: 迭代器
    """
    return (iter_page_multi_async if async_ else iter_page_multi_threaded)(
        call, # type: ignore
        iter_payload, 
        check_for_stop=check_for_stop, 
        retry_for_exception=retry_for_exception, 
        payload=payload, 
        page_size=page_size, 
        key_page=key_page, 
        key_page_size=key_page_size, 
        cooldown=cooldown, 
        max_workers=max_workers, 
    )

