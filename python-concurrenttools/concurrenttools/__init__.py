#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 3)
__all__ = [
    "thread_batch", "thread_pool_batch", "async_batch", 
    "threaded", "run_as_thread", "asynchronized", "run_as_async", 
    "threadpool_map", "taskgroup_map", "conmap", "conmap_wrap", 
    "iter_pages", "Return", 
]

from asyncio import (
    ensure_future, get_event_loop, BaseEventLoop, CancelledError as AsyncCancelledError, 
    Future as AsyncFuture, Queue as AsyncQueue, Semaphore as AsyncSemaphore, TaskGroup, 
)
from collections import deque
from collections.abc import (
    AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Generator, Iterable, 
    Iterator, Mapping, 
)
from concurrent.futures import CancelledError, Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial, update_wrapper
from inspect import isawaitable, iscoroutinefunction, signature, Signature
from itertools import count, repeat
from os import cpu_count
from queue import Queue, SimpleQueue, Empty
from _thread import start_new_thread
from threading import Event, Lock, Semaphore, Thread
from typing import cast, overload, Any, ContextManager, Literal

from argtools import argcount
from asynctools import async_map, async_zip, ensure_coroutine, run_async
from decotools import optional


if "__del__" not in ThreadPoolExecutor.__dict__:
    setattr(ThreadPoolExecutor, "__del__", lambda self, /: self.shutdown(wait=False, cancel_futures=True))
if "__del__" not in TaskGroup.__dict__:
    setattr(TaskGroup, "__del__", lambda self, /: run_async(self.__aexit__(None, None, None)))


class Return:
    def __init__(self, value, /):
        self.value = value


def has_keyword_async(request: Callable | Signature, /) -> bool:
    if callable(request):
        try:
            request = signature(request)
        except (ValueError, TypeError):
            return False
    params = request.parameters
    param = params.get("async_")
    return bool(param and param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY))


def thread_batch[T, V](
    work: Callable[[T], V] | Callable[[T, Callable], V], 
    tasks: Iterable[T], 
    callback: None | Callable[[V], Any] = None, 
    max_workers: None | int = None, 
):
    ac = argcount(work)
    if ac < 1:
        raise TypeError(f"{work!r} should accept a positional argument as task")
    with_submit = ac > 1
    if max_workers is None or max_workers <= 0:
        max_workers = min(32, (cpu_count() or 1) + 4)

    q: Queue[T | object] = Queue()
    get, put, task_done = q.get, q.put, q.task_done
    sentinal = object()
    lock = Lock()
    nthreads = 0
    running = True

    def worker():
        task: T | object
        while running:
            try:
                task = get(timeout=1)
            except Empty:
                continue
            if task is sentinal:
                put(sentinal)
                break
            task = cast(T, task)
            try:
                if with_submit:
                    r = cast(Callable[[T, Callable], V], work)(task, submit)
                else:
                    r = cast(Callable[[T], V], work)(task)
                if callback is not None:
                    callback(r)
            except BaseException:
                pass
            finally:
                task_done()

    def submit(task):
        nonlocal nthreads
        if nthreads < max_workers:
            with lock:
                if nthreads < max_workers:
                    start_new_thread(worker, ())
                    nthreads += 1
        put(task)

    for task in tasks:
        submit(task)
    try:
        q.join()
    finally:
        running = False
        q.queue.clear()
        put(sentinal)


def thread_pool_batch[T, V](
    work: Callable[[T], V] | Callable[[T, Callable], V], 
    tasks: Iterable[T], 
    callback: None | Callable[[V], Any] = None, 
    max_workers: None | int = None, 
):
    ac = argcount(work)
    if ac < 1:
        raise TypeError(f"{work!r} should take a positional argument as task")
    with_submit = ac > 1
    if max_workers is None or max_workers <= 0:
        max_workers = min(32, (cpu_count() or 1) + 4)

    ntasks = 0
    lock = Lock()
    done_evt = Event()

    def works(task):
        nonlocal ntasks
        try:
            if with_submit:
                r = cast(Callable[[T, Callable], V], work)(task, submit)
            else:
                r = cast(Callable[[T], V], work)(task)
            if callback is not None:
                callback(r)
        finally:
            with lock:
                ntasks -= 1
            if not ntasks:
                done_evt.set()

    def submit(task):
        nonlocal ntasks
        with lock:
           ntasks += 1
        return create_task(works, task)

    pool = ThreadPoolExecutor(max_workers)
    try:
        create_task = pool.submit
        for task in tasks:
            submit(task)
        if ntasks:
            done_evt.wait()
    finally:
        pool.shutdown(False, cancel_futures=True)


async def async_batch[T, V](
    work: Callable[[T], Coroutine[None, None, V]] | Callable[[T, Callable], Coroutine[None, None, V]], 
    tasks: Iterable[T] | AsyncIterable[T], 
    callback: None | Callable[[V], Any] = None, 
    max_workers: None | int | AsyncSemaphore = None, 
):
    if max_workers is None:
        max_workers = 32
    if isinstance(max_workers, int):
        if max_workers > 0:
            sema = AsyncSemaphore()
        else:
            sema = None
    else:
        sema = max_workers
    ac = argcount(work)
    if ac < 1:
        raise TypeError(f"{work!r} should accept a positional argument as task")
    with_submit = ac > 1
    async def works(task, sema=sema):
        if sema is not None:
            async with sema:
                return await works(task, None)
        try:
            if with_submit:
                r = await cast(Callable[[T, Callable], Coroutine[None, None, V]], work)(task, submit)
            else:
                r = await cast(Callable[[T], Coroutine[None, None, V]], work)(task)
            if callback is not None:
                t = callback(r)
                if isawaitable(t):
                    await t
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            raise AsyncCancelledError from e
    async with TaskGroup() as tg:
        create_task = tg.create_task
        submit = lambda task, /: create_task(works(task))
        if isinstance(tasks, Iterable):
            for task in tasks:
                submit(task)
        else:
            async for task in tasks:
                submit(task)


@optional
def threaded[**Args, T](
    func: Callable[Args, T], 
    /, 
    lock: None | int | ContextManager = None, 
    **thread_init_kwds, 
) -> Callable[Args, Future[T]]:
    if isinstance(lock, int):
        lock = Semaphore(lock)
    def wrapper(*args: Args.args, **kwds: Args.kwargs) -> Future[T]:
        if lock is None:
            def asfuture():
                try: 
                    fu.set_result(func(*args, **kwds))
                except BaseException as e:
                    fu.set_exception(e)
        else:
            def asfuture():
                with lock:
                    try: 
                        fu.set_result(func(*args, **kwds))
                    except BaseException as e:
                        fu.set_exception(e)
        fu: Future[T] = Future()
        thread = fu.thread = Thread(target=asfuture, **thread_init_kwds) # type: ignore
        thread.start()
        return fu
    return update_wrapper(wrapper, func)


def run_as_thread[**Args, T](
    func: Callable[Args, T], 
    /, 
    *args: Args.args, 
    **kwargs: Args.kwargs, 
) -> Future[T]:
    return getattr(threaded, "__wrapped__")(func)(*args, **kwargs)


@optional
def asynchronized[**Args, T](
    func: Callable[Args, T], 
    /, 
    loop: None | BaseEventLoop = None, 
    new_thread: bool = False, 
    executor: Literal[False, None] | Executor = None, 
) -> Callable[Args, AsyncFuture[T]]:
    def run_in_thread(loop, future, args, kwargs, /):
        try:
            loop.call_soon_threadsafe(future.set_result, func(*args, **kwargs))
        except BaseException as e:
            loop.call_soon_threadsafe(future.set_exception, e)
    def wrapper(*args: Args.args, **kwargs: Args.kwargs) -> AsyncFuture[T]:
        nonlocal loop
        if not new_thread or iscoroutinefunction(func):
            return ensure_future(ensure_coroutine(func(*args, **kwargs)), loop=loop)
        if loop is None:
            loop = cast(BaseEventLoop, get_event_loop())
        if executor is False:
            future = loop.create_future()
            start_new_thread(run_in_thread, (loop, future, args, kwargs))
            return future
        return loop.run_in_executor(executor, partial(func, *args, **kwargs))
    return wrapper


def run_as_async[**Args, T](
    func: Callable[Args, T], 
    /, 
    *args: Args.args, 
    **kwargs: Args.kwargs, 
) -> AsyncFuture[T]:
    return getattr(asynchronized, "__wrapped__")(func)(*args, **kwargs)


def threadpool_map[T](
    func: Callable[..., T], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
) -> Iterator[T]:
    if max_workers is None or max_workers <= 0:
        max_workers = min(32, (cpu_count() or 1) + 4)
    if max_workers == 1:
        if arg_func is None:
            yield from map(partial(func, **kwargs), it, *its)
        else:
            for args in zip(it, *its):
                arg = arg_func(*args, **kwargs)
                if isinstance(arg, Return):
                    yield arg.value
                    continue
                yield func(*args, arg, **kwargs)
    else:
        queue: SimpleQueue[None | Future] = SimpleQueue()
        get, put = queue.get, queue.put_nowait
        executor = ThreadPoolExecutor(max_workers=max_workers)
        submit = executor.submit
        running = True
        def make_tasks():
            try:
                for args in zip(it, *its):
                    if not running:
                        break
                    try:
                        if arg_func is None:
                            put(submit(func, *args, **kwargs))
                        else:
                            arg = arg_func(*args, **kwargs)
                            if isinstance(arg, Return):
                                put(arg.value)
                                continue
                            put(submit(func, *args, arg, **kwargs))
                    except RuntimeError:
                        break
            finally:
                put(None)
        try:
            start_new_thread(make_tasks, ())
            with executor:
                while fu := get():
                    yield fu.result()
        finally:
            running = False
            executor.shutdown(wait=False, cancel_futures=True)


async def taskgroup_map[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
) -> AsyncIterator[T]:
    if max_workers is None or max_workers <= 0:
        max_workers = 32
    if max_workers == 1:
        if arg_func is None:
            async for ret in async_map(partial(func, **kwargs), it, *its):
                yield cast(T, ret)
        else:
            async for args in async_zip(it, *its):
                arg = arg_func(*args, **kwargs)
                if isawaitable(arg):
                    arg = await arg
                if isinstance(arg, Return):
                    yield arg.value
                    continue
                ret = func(*args, arg, **kwargs)
                if isawaitable(ret):
                    ret = await ret
                yield ret
    else:
        sema = AsyncSemaphore(max_workers)
        queue: AsyncQueue[None | AsyncFuture] = AsyncQueue()
        get, put = queue.get, queue.put_nowait
        async def call(args):
            async with sema:
                ret = func(*args, **kwargs)
                if isawaitable(ret):
                    ret = await ret
                return ret
        async def make_tasks():
            try:
                async for args in async_zip(it, *its):
                    if arg_func is not None:
                        arg = arg_func(*args, **kwargs)
                        if isawaitable(arg):
                            arg = await arg
                        if isinstance(arg, Return):
                            put(arg.value)
                            continue
                        args = (*args, arg)
                    put(create_task(call(args)))
            finally:
                put(None)
        async with TaskGroup() as tg:
            create_task = tg.create_task
            create_task(make_tasks())
            while task := await get():
                yield await task


@overload
def conmap[T](
    func: Callable[..., T], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
    async_: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def conmap[T](
    func: Callable[..., Awaitable[T]], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def conmap[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    if has_keyword_async(func):
        kwargs = {"async_": async_, **kwargs}
    map: Callable = taskgroup_map if async_ else threadpool_map
    return map(
        func, 
        it, 
        *its, 
        arg_func=arg_func, 
        max_workers=max_workers, 
        kwargs=kwargs, 
    )


@overload
def conmap_wrap[T](
    func: Callable[..., T], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
    async_: Literal[False] = False, 
) -> Iterator[tuple[tuple, None | T, None | BaseExceptionGroup]]:
    ...
@overload
def conmap_wrap[T](
    func: Callable[..., Awaitable[T]], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
    async_: Literal[True], 
) -> AsyncIterator[tuple[tuple, None | T, None | BaseExceptionGroup]]:
    ...
def conmap_wrap[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    it, 
    /, 
    *its, 
    arg_func: None | Callable = None, 
    max_workers: None | int = None, 
    kwargs: Mapping = {}, 
    async_: Literal[False, True] = False, 
) -> Iterator[tuple[tuple, None | T, None | BaseExceptionGroup]] | AsyncIterator[tuple[tuple, None | T, None | BaseExceptionGroup]]:
    method: Callable
    if async_:
        async def method(*args):
            try:
                ret = func(*args)
                if isawaitable(ret):
                    ret = await ret
                return args, ret, None
            except BaseException as e:
                return args, None, e
    else:
        def method(*args):
            try:
                return args, func(*args), None
            except BaseException as e:
                return args, None, e
    return conmap(
        method, 
        it, 
        *its, 
        arg_func=arg_func, 
        max_workers=max_workers, 
        kwargs=kwargs, 
        async_=async_, 
    )


@overload
def run_gen_step[T](
    gen_step: Generator[Any, Any, T] | Callable[[], Generator[Any, Any, T]], 
    async_: Literal[False] = False, 
) -> T:
    ...
@overload
def run_gen_step[T](
    gen_step: Generator[Any, Any, T] | Callable[[], Generator[Any, Any, T]], 
    async_: Literal[True], 
) -> Coroutine[Any, Any, T]:
    ...
def run_gen_step[T](
    gen_step: Generator[Any, Any, T] | Callable[[], Generator[Any, Any, T]], 
    async_: bool = False, 
) -> T | Coroutine[Any, Any, T]:
    if callable(gen_step):
        gen_step = gen_step()
    send = gen_step.send
    if async_:
        throw = gen_step.throw
        async def run():
            try:
                step: Awaitable = send(None)
                while True:
                    try:
                        val: Any = await step
                    except BaseException as e:
                        step = throw(e)
                    else:
                        step = send(val)
            except StopIteration as e:
                return e.value
        return run()
    else:
        try:
            step: Any = None
            while True:
                step = send(step)
        except StopIteration as e:
            return e.value


@overload
def iter_pages[T](
    func: Callable[[int], T], 
    is_last_page: Callable[[T], bool], 
    next_page: int | Callable[[], int] | Iterable[int] = 1, 
    max_workers: None | int = 1, 
    unordered: bool = True, 
    *, 
    async_: Literal[False] = False, 
) -> Iterator[T]:
    ...
@overload
def iter_pages[T](
    func: Callable[[int], Awaitable[T]], 
    is_last_page: Callable[[T], bool], 
    next_page: int | Callable[[], int] | Iterable[int] = 1, 
    max_workers: None | int = 1, 
    unordered: bool = True, 
    *, 
    async_: Literal[True], 
) -> AsyncIterator[T]:
    ...
def iter_pages[T](
    func: Callable[[int], T] | Callable[[int], Awaitable[T]], 
    is_last_page: Callable[[T], bool], 
    next_page: int | Callable[[], int] | Iterable[int] = 1, 
    max_workers: None | int = 1, 
    unordered: bool = True, 
    *, 
    async_: Literal[False, True] = False, 
) -> Iterator[T] | AsyncIterator[T]:
    """分页执行操作

    :param func: 操作函数
    :param is_last_page: 根据响应判断是否下一页，执行后可以报错（如果报错 StopIteration，则丢弃这次结果，并视为最后一页）
    :param next_page: 获取下一页的编号
    :param max_workers: 最大并发数，如果为 None 或 <= 0，则自动确定
    :param unordered: 是否允许乱序
    :param async_: 是否异步

    :return: 迭代器，产生每次操作的响应
    """
    if iscoroutinefunction(func):
        async_ = True
    if has_keyword_async(func):
        func = partial(func, async_=async_) # type: ignore
    if max_workers is None or max_workers <= 0:
        max_workers = 20 if async_ else None
    if isinstance(next_page, int):
        next_page = count(next_page).__next__
    elif not callable(next_page):
        next_page = iter(next_page).__next__
    if max_workers == 1:
        if async_:
            async def async_gen():
                not_last = True
                while not_last:
                    resp: T = await cast(Callable[[int], Awaitable[T]], func)(next_page())
                    try:
                        not_last = not is_last_page(resp)
                    except StopIteration:
                        not_last = False
                    yield resp
            return async_gen()
        else:
            def gen():
                not_last = True
                while not_last:
                    resp: T = cast(Callable[[int], T], func)(next_page())
                    try:
                        not_last = not is_last_page(resp)
                    except StopIteration:
                        not_last = False
                    yield resp
            return gen()
    sentinel = object()
    max_page: None | int = None
    if unordered:
        if async_:
            q: AsyncQueue | SimpleQueue = AsyncQueue()
        else:
            q = SimpleQueue()
        get, put = q.get, q.put_nowait
        task_list: list = []
        task_page: list[int] = []
        task_ids: set[int] = set()
        discard_task_id = task_ids.discard
        def countdown(task_id, /):
            task_list[task_id] = None
            discard_task_id(task_id)
            if not task_ids:
                put(sentinel)
        def request_unordered(task_id, /):
            nonlocal max_page
            try:
                page = next_page()
                while max_page is None or page < max_page:
                    task_page[task_id] = page
                    resp: T = yield func(page)
                    try:
                        is_last = is_last_page(resp)
                    except BaseException as e:
                        is_last = True
                        if isinstance(e, StopIteration):
                            put(resp)
                        else:
                            put(e)
                    if is_last:
                        max_page = page
                        for i, p in enumerate(task_page):
                            task = task_list[i]
                            if task and p > page:
                                task.cancel()
                                countdown(i)
                    page = next_page()
            except BaseException as e:
                put(e)
            finally:
                countdown(task_id)
        if async_:
            async def async_gen():
                n = cast(int, max_workers)
                async with TaskGroup() as tg:
                    create_task = tg.create_task
                    task_ids.update(range(n))
                    task_page.extend(repeat(0, n))
                    for i in range(n):
                        task_list.append(create_task(run_gen_step(request_unordered(i), True)))
                    while True:
                        resp = await get()
                        if resp is sentinel:
                            break
                        elif isinstance(resp, (CancelledError, AsyncCancelledError)):
                            continue
                        elif isinstance(resp, BaseException):
                            raise resp
                        yield resp
            return async_gen()
        else:
            def gen():
                executor = ThreadPoolExecutor(max_workers)
                try:
                    submit = executor.submit
                    n = executor._max_workers
                    task_ids.update(range(n))
                    task_page.extend(repeat(0, n))
                    for i in range(n):
                        task_list.append(submit(run_gen_step, request_unordered(i)))
                    while True:
                        resp = get()
                        if resp is sentinel:
                            break
                        elif isinstance(resp, (CancelledError, AsyncCancelledError)):
                            continue
                        elif isinstance(resp, BaseException):
                            raise resp
                        yield resp
                finally:
                    executor.shutdown(False, cancel_futures=True)
            return gen()
    else:
        @dataclass(slots=True)
        class PageTask:
            page: int
            task: Any
            def __bool__(self, /) -> bool:
                return self.task is not None
            def cancel(self, /):
                if self:
                    self.task.cancel()
                    self.task = None
        dq: deque[PageTask] = deque()
        push, pop = dq.append, dq.pop
        def request_ordered(page, add_task, /):
            nonlocal max_page
            if max_page is not None and page > max_page:
                return sentinel
            resp: T = yield func(page)
            exc: None | BaseException = None
            try:
                is_last = is_last_page(resp)
            except StopIteration:
                is_last = True
            except BaseException as e:
                is_last = True
                exc = e
            if is_last and (max_page is None or page < max_page):
                max_page = page
                for page_task in dq:
                    if page_task and page_task.page > page:
                        page_task.cancel()
            if exc is not None:
                raise exc
            elif not is_last:
                if async_:
                    add_task(next_page())
                else:
                    with lock:
                        add_task(next_page())
            return resp
        if async_:
            async def async_gen():
                async with TaskGroup() as tg:
                    create_task = tg.create_task
                    add_task: Callable[[int], Any] = lambda page: push(PageTask(
                        page, 
                        create_task(run_gen_step(request_ordered(page, add_task), True)), 
                    ))
                    try:
                        for _ in range(cast(int, max_workers)):
                            add_task(next_page())
                        while dq:
                            if page_task := pop():
                                resp = await page_task.task
                                if resp is not sentinel:
                                    yield resp
                    except (CancelledError, AsyncCancelledError):
                        pass
            return async_gen()
        else:
            lock = Lock()
            def gen():
                executor = ThreadPoolExecutor(max_workers)
                try:
                    n = executor._max_workers
                    submit = executor.submit
                    add_task: Callable[[int], Any] = lambda page: push(PageTask(
                        page, 
                        submit(run_gen_step, request_ordered(page, add_task)), 
                    ))
                    try:
                        for _ in range(n):
                            add_task(next_page())
                        while dq:
                            if page_task := pop():
                                resp = page_task.task.result()
                                if resp is not sentinel:
                                    yield resp
                    except (CancelledError, AsyncCancelledError):
                        pass
                finally:
                    executor.shutdown(False, cancel_futures=True)
            return gen()

# TODO: iter_pages_with_cooldown
