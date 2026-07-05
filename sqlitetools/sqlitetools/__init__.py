#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 1, 2)
__all__ = [
    "FetchType", "to_uri", "enclose", "connect", "context_cursor", 
    "execute", "executescript", "query", "find", "upsert_items", 
]

from collections import ChainMap, UserDict
from collections.abc import Buffer, Callable, Iterable, Mapping, Sequence
from contextlib import closing, contextmanager, suppress
from enum import IntEnum
from os import fsdecode, PathLike
from os.path import isabs
from platform import system
from re import compile as re_compile
from sqlite3 import connect as sqlite_connect
from typing import Any, Final, Literal, Self
from urllib.parse import urlencode

from sqlparse import format as sql_format, split as sql_split


CRE_COLNAME_sub: Final = re_compile(r" \[[^]]+\]$").sub
CRE_MULTI_SLASH_sub: Final = re_compile(r"/{2,}").sub
TRANSTAB_PATH_TO_URI: Final = {c: f"%{c:02x}" for c in b"?#"}
if system() == "Windows":
    TRANSTAB_PATH_TO_URI[ord("\\")] = "/"


class MappingAsDict(UserDict, dict): # type: ignore
    def __init__(self, data: Mapping, /):
        setattr(self, "data", data)


class FetchType(IntEnum):
    auto = 0
    any = 1
    one = 2
    dict = 3

    @classmethod
    def ensure(cls, val, /) -> Self:
        if isinstance(val, cls):
            return val
        if isinstance(val, str):
            with suppress(KeyError):
                return cls[val]
        return cls(val)


def to_uri(
    path: bytes | str | PathLike, 
    params: Any = None, 
) -> str:
    """把路径转换为 URI

    .. note::
        - https://sqlite.org/uri.html#the_uri_path
        - https://tools.ietf.org/html/rfc3986

    :param path: 路径
    :param params: 查询参数

    :return: 转换后的 URI
    """
    path = fsdecode(path).translate(TRANSTAB_PATH_TO_URI)
    path = CRE_MULTI_SLASH_sub("/", path)
    if isabs(path) and not path.startswith("/"):
        path = "/" + path
    if params:
        if isinstance(params, Buffer):
            params = str(params, "utf-8")
        elif not isinstance(params, str):
            params = urlencode(params)
        if params and not params.startswith("?"):
            params = "?" + params
    return f"file:{path}{params}"


def bind_row_factory(
    cursor, 
    /, 
    row_factory: None | int | str | FetchType | Callable = None, 
):
    """给游标绑定数据处理

    :param cursor: 游标
    :param row_factory: 对数据进行处理，然后返回处理后的值

        - 如果是 Callable，则调用然后返回它的值
        - 如果是 FetchType.auto，则当数据是 tuple 且长度为 1 时，返回第 1 个为位置的值，否则返回数据本身
        - 如果是 FetchType.any，则返回数据本身
        - 如果是 FetchType.one，则返回数据中第 1 个位置的值（索引为 0）
        - 如果是 FetchType.dict，则返回字典，键从游标中获取

    :return: 返回游标或者迭代器
    """
    if row_factory is not None:
        if not callable(row_factory):
            match FetchType.ensure(row_factory):
                case FetchType.auto:
                    def row_factory(_, record):
                        if isinstance(record, Sequence) and not isinstance(record, (str, Buffer)) and len(record) == 1:
                            return record[0]
                        return record
                case FetchType.one:
                    def row_factory(_, record, /):
                        if not isinstance(record, (str, Buffer)):
                            if isinstance(record, Sequence):
                                return record[0]
                            elif isinstance(record, Mapping):
                                return record[next(iter(record))]
                            elif isinstance(record, Iterable):
                                return next(iter(record))
                        return record
                case FetchType.dict:
                    try:
                        fields: Sequence = tuple(CRE_COLNAME_sub("", f[0]) for f in cursor.description)
                    except Exception:
                        fields = range(1<<15)
                    def row_factory(_, record, /):
                        if not isinstance(record, Mapping):
                            record = dict(zip(fields, record))
                        return record
                case _:
                    row_factory = None
    if row_factory is not None:
        if hasattr(cursor, "row_factory"):
            setattr(cursor, "row_factory", row_factory)
        else:
            def binder(row_factory, cursor, /):
                try:
                    for record in cursor:
                        yield row_factory(cursor, record)
                finally:
                    cursor.close()
            return binder(row_factory, cursor)
    return cursor


def enclose(
    name: str | Iterable[str], 
    encloser: str | tuple[str, str] = '"', 
) -> str:
    """在字段名外面添加包围符号

    :param name: 字段名，或者字段名的可迭代（将会用 '.' 进行连接）
    :param encloser: 包围符号

    :return: 处理后的字符串
    """
    if not isinstance(name, str):
        return ".".join(enclose(part, encloser) for part in name)
    if name.isidentifier():
        return name
    if isinstance(encloser, tuple):
        l, r = encloser
        return f"{l}{name}{r}"
    else:
        return f"{encloser}{name.replace(encloser, encloser * 2)}{encloser}"


def connect(
    db = ":memory:", 
    /, 
    check_same_thread: bool = False, 
    **connect_kwargs, 
):
    """返回连接对象
    """
    if isinstance(db, (bytes, str, PathLike)):
        connect_kwargs["check_same_thread"] = check_same_thread
        if isinstance(db, str) and db.startswith("file:"):
            connect_kwargs.setdefault("uri", db.startswith("file:"))
        return sqlite_connect(db, **connect_kwargs)
    if hasattr(db, "connection"):
        return db.connection
    return db


@contextmanager
def context_cursor(con, /, isolation_level: None | str = ""):
    """上下文管理器，创建一个 sqlite 数据库事务，会自动进行 commit 和 rollback

    :param con: 数据库连接或游标
    :param isolation_level: 隔离级别，如果为 None，则不开启事务

    :return: 上下文管理器，返回一个游标
    """
    if hasattr(con, "cursor"):
        cursor = con.cursor()
    elif isinstance(con, (bytes, str, PathLike)):
        cursor = connect(con).cursor()
    else:
        cursor = con
    if isolation_level is None:
        yield cursor
    else:
        with suppress(Exception):
            cursor.execute(f"BEGIN {isolation_level}")
        try:
            yield cursor
        except:
            with suppress(Exception):
                cursor.execute("ROLLBACK")
            raise
        else:
            with suppress(Exception):
                cursor.execute("COMMIT")


def _normalize_params(params, /) -> None | dict | Sequence:
    if params is not None:
        if isinstance(params, Mapping):
            if not isinstance(params, dict):
                params = MappingAsDict(params)
        elif isinstance(params, (str, Buffer)) or not isinstance(params, Sequence):
            params = params,
    return params


def execute(
    con, 
    /, 
    sql: str, 
    params: Any = None, 
    executemany: bool = False, 
    commit: bool = False, 
):
    """执行一个 sql 语句

    :param con: 数据库连接或游标
    :param sql: sql 语句
    :param params: 参数，用于填充 sql 中的占位符
    :param executemany: 如果为 True，调用 executemany 方法，否则调用 execute 方法
    :param commit: 是否提交事务

    :return: 游标
    """
    with context_cursor(con, (None, "")[commit]) as cursor:
        if executemany and params:
            cursor.executemany(sql, map(_normalize_params, params))
        else:
            params = _normalize_params(params)
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
        return cursor


def executescript(con, /, sql: str):
    """执行一个 sql 语句

    :param con: 数据库连接或游标
    :param sql: sql 语句
    :param commit: 是否提交事务

    :return: 游标
    """
    with context_cursor(con, None) as cursor:
        sql = sql_format(sql, strip_comments=True, strip_whitespace=True)
        if sql:
            if hasattr(con, "executescript"):
                cursor.executescript(sql)
            else:
                pragmas: list[str] = []
                stmts: list[str] = []
                for stmt in filter(None, sql_split(sql)):
                    startswith = stmt.strip().upper().startswith
                    if startswith("PRAGMA"):
                        pragmas.append(stmt)
                    elif not startswith("SELECT"):
                        stmts.append(stmt)
                execute = cursor.execute
                for sql in pragmas:
                    execute(sql)
                if stmts:
                    with context_cursor(cursor):
                        for sql in stmts:
                            execute(sql)
    return cursor


def query(
    con, 
    /, 
    sql: str, 
    params: Any = None, 
    row_factory: None | int | str | FetchType | Callable = None, 
):
    """执行一个 sql 查询语句，或者 DML 语句但有 RETURNING 子句（但不会主动 commit）

    :param con: 数据库连接或游标
    :param sql: sql 语句
    :param params: 参数，用于填充 sql 中的占位符
    :param row_factory: 对数据进行处理，然后返回处理后的值

        - 如果是 Callable，则调用然后返回它的值
        - 如果是 FetchType.auto，则当数据是 tuple 且长度为 1 时，返回第 1 个为位置的值，否则返回数据本身
        - 如果是 FetchType.any，则返回数据本身
        - 如果是 FetchType.one，则返回数据中第 1 个位置的值（索引为 0）
        - 如果是 FetchType.dict，则返回字典，键从游标中获取

    :return: 游标或者迭代器
    """
    cursor = execute(con, sql, params)
    return bind_row_factory(cursor, row_factory)


def find(
    con, 
    /, 
    sql: str, 
    params: Any = None, 
    default: Any = None, 
    row_factory: int | str | FetchType | Callable = "auto", 
):
    """执行一个 sql 查询语句，或者 DML 语句但有 RETURNING 子句（但不会主动 commit），返回一条数据

    :param con: 数据库连接或游标
    :param sql: sql 语句
    :param params: 参数，用于填充 sql 中的占位符
    :param default: 当没有数据返回时，作为默认值返回，如果是异常对象，则进行抛出
    :param row_factory: 对数据进行处理，然后返回处理后的值

        - 如果是 Callable，则调用然后返回它的值
        - 如果是 FetchType.auto，则当数据是 tuple 且长度为 1 时，返回第 1 个为位置的值，否则返回数据本身
        - 如果是 FetchType.any，则返回数据本身
        - 如果是 FetchType.one，则返回数据中第 1 个位置的值（索引为 0）
        - 如果是 FetchType.dict，则返回字典，键从游标中获取

    :return: 查询结果的第一条数据
    """
    with closing(query(con, sql, params)) as cursor:
        record = next(bind_row_factory(cursor, row_factory), default)
        if isinstance(record, BaseException):
            raise record
        return record


def upsert_items(
    con, 
    items: Mapping | Sequence[Mapping], 
    /, 
    extras: None | Mapping = None, 
    table: str = "data", 
    fields: Sequence[str] = (), 
    on_conflict: Literal["", "ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"] = "", 
    on_conflict_update_fields: Sequence[str] = (), 
    where: str = "", 
    commit: bool = False, 
):
    """往表中插入或更新数据

    .. note::
        - https://sqlite.org/lang_insert.html
        - https://sqlite.org/lang_upsert.html
        - https://sqlite.org/lang_conflict.html

    :param con: 数据库连接或游标
    :param items: 一组数据
    :param extras: 附加数据（如果和原数据存在 key 冲突，则将其替换）
    :param table: 表名
    :param fields: 需要插入的字段，如果有 extras，会将其字段合并进来
    :param on_conflict: 冲突策略，如果为空，则会执行 UPSERT
    :param on_conflict_update_fields: 当发生冲突时，更新的字段（``on_conflict`` 为空时才生效）
    :param where: 判断条件，即在符合的情况下才执行 UPSERT，否则相当于 IGNORE
    :param commit: 是否提交事务

    :return: 游标
    """
    if isinstance(items, Mapping):
        items = items,
    if extras:
        items = [ChainMap(item, extras) for item in items] # type: ignore
        if fields:
            fields = tuple(set(fields) | set(extras))
    if not fields:
        fields = tuple(items[0])
    if on_conflict:
        insert_conflict = " OR " + on_conflict
    else:
        insert_conflict = ""
    sql = f"""\
INSERT{insert_conflict} INTO {enclose(table)}({",".join(map(enclose, fields))})
VALUES ({",".join(map(":".__add__, fields))})"""
    if not on_conflict:
        if not on_conflict_update_fields:
            on_conflict_update_fields = fields
        sql += f"""
ON CONFLICT DO UPDATE SET {",".join(map("{0}=excluded.{0}".format, map(enclose, on_conflict_update_fields)))}"""
    if where:
        sql += "\nWHERE " + where
    return execute(con, sql, items, executemany=True, commit=commit)

