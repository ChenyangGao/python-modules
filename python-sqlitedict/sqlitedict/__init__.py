#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 3)
__all__ = ["SqliteDict", "SqliteTableDict"]

from collections.abc import Callable, Iterator, MutableMapping
from sqlite3 import connect, register_adapter, register_converter, Connection, Cursor

from orjson import dumps, loads
from sqlitetools import enclose, execute, find, query, AutoCloseConnection, AutoCloseCursor
from undefined import undefined


register_adapter(dict, dumps)
register_adapter(list, dumps)
register_converter("JSON", loads)


def call_with_lock(lock, func, /, *args, **kwds):
    if lock is None:
        return func(*args, **kwds)
    with lock:
        return func(*args, **kwds)


class SqliteDict(MutableMapping):

    def __init__(
        self, 
        dbfile=":memory:", 
        /, 
        dumps: None | Callable = None, 
        loads: None | Callable = None, 
        key_dumps: None | Callable = None, 
        key_loads: None | Callable = None, 
        value_dumps: None | Callable = None, 
        value_loads: None | Callable = None, 
        timeout: int | float = float("inf"), 
        uri: bool = False, 
        lock=None, 
    ):
        self.con = con = connect(
            dbfile, 
            autocommit=True, 
            check_same_thread=False, 
            timeout=timeout, 
            uri=uri, 
        )
        con.executescript("""\
PRAGMA journal_mode = wal;
CREATE TABLE IF NOT EXISTS dict(
  key BLOB UNIQUE NOT NULL, 
  value BLOB NOT NULL
);""")
        if key_dumps is None:
            key_dumps = dumps
        if key_loads is None:
            key_loads = loads
        if value_dumps is None:
            value_dumps = dumps
        if value_loads is None:
            value_loads = loads
        def execute(sql, params=None, /):
            if sql.startswith("SELECT"):
                lock_ = None
            else:
                lock_ = lock
            cur = con.cursor(AutoCloseCursor)
            if params:
                if len(params) == 1:
                    if key_dumps:
                        key, = params
                        params = key_dumps(key),
                elif key_dumps or value_dumps:
                    key, value = params
                    if key_dumps:
                        key = key_dumps(key)
                    if value_dumps:
                        value = value_dumps(value)
                    params = key, value
                call_with_lock(lock_, cur.execute, sql, params)
            else:
                call_with_lock(lock_, cur.execute, sql)
            return cur
        self.execute = execute
        if key_loads or value_loads:
            def row_factory(_, r, /):
                key = r[0]
                if len(r) == 1:
                    if type(key) is int:
                        return key
                    if value_loads:
                        key = value_loads(key)
                    return key
                if key_loads:
                    key = key_loads(key)
                if len(r) == 3:
                    return key
                value = r[2]
                if value_loads:
                    value = value_loads(value)
                return key, value
        else:
            def row_factory(_, r, /):
                if len(r) in (1, 3):
                    return r[0]
                return r
        con.row_factory = row_factory

    def __contains__(self, key, /) -> bool:
        cur = self.execute("SELECT 1 FROM dict WHERE key = ? LIMIT 1", (key,))
        return bool(next(cur, 0))

    def __del__(self, /):
        self.con.close()

    def __delitem__(self, key, /):
        cur = self.execute("DELETE FROM dict WHERE key = ?", (key,))
        if not cur.rowcount:
            raise KeyError(key)

    def __getitem__(self, key, /):
        cur = self.execute("SELECT value FROM dict WHERE key = ? LIMIT 1", (key,))
        for value in cur:
            return value
        raise KeyError(key)

    def __iter__(self, /) -> Iterator:
        return self.execute("SELECT key, NULL, NULL FROM dict")

    def __len__(self, /) -> int:
        return next(self.execute("SELECT COUNT(1) FROM dict"), 0)

    def __setitem__(self, key, value, /):
        self.execute("REPLACE INTO dict (key, value) VALUES (?, ?)", (key, value))

    def clear(self, /):
        self.execute("DELETE FROM dict")

    def pop(self, key, /, default=undefined):
        cur = self.execute("DELETE FROM dict WHERE key = ? RETURNING value", (key,))
        if (value := next(cur, default)) is undefined:
            raise KeyError(key)
        return value

    def iter_values(self, /):
        return self.execute("SELECT value FROM dict")

    def iter_items(self, /):
        return self.execute("SELECT key, value FROM dict")


class SqliteTableDict(MutableMapping):

    def __init__(
        self, 
        con, 
        /, 
        table: str = "data", 
        key: str | tuple[str, ...] = "id", 
        value: str | tuple[str, ...] = "data", 
        where: str = "", 
    ):
        if not isinstance(con, (Connection, Cursor)):
            con = connect(con, factory=AutoCloseConnection)
        self.con = con
        table = enclose(table)
        key_is_tuple = self._key_is_tuple = isinstance(key, tuple)
        value_is_tuple = self._value_is_tuple = isinstance(value, tuple)
        if key_is_tuple:
            self._key_len = len(key)
            key_str = ",".join(map(enclose, key))
            key_pred_str = " AND ".join(f"{k}=?" for k in map(enclose, key))
        else:
            self._key_len = 0
            key_str = enclose(key)
            key_pred_str = f"{key_str}=?"
        if value_is_tuple:
            self._value_len = len(value)
            value_str = ",".join(map(enclose, value))
            value_conflict_set_str = ",".join(f"{v}=excluded.{v}" for v in map(enclose, value))
        else:
            self._value_len = 0
            value_str = enclose(value)
            value_conflict_set_str = f"{value_str}=excluded.{value_str}"
        where_str = where
        if where:
            where_str = " AND " + where
        self._sql_delitem = f"DELETE FROM {table} WHERE {key_pred_str}{where_str}"
        self._sql_getitem = f"SELECT {value_str} FROM {table} WHERE {key_pred_str}{where_str} LIMIT 1"
        n_qmarks = ",".join("?" * ((self._key_len or 1) + (self._value_len or 1)))
        self._sql_setitem = f"INSERT INTO {table}({key_str},{value_str}) VALUES ({n_qmarks}) ON CONFLICT({key_str}) DO UPDATE SET {value_conflict_set_str}"""
        where_str = where
        if where:
            where_str = " WHERE " + where
        self._sql_iter = f"SELECT {key_str} FROM {table}{where_str}"
        self._sql_len = f"SELECT COUNT(1) FROM {table}{where_str}"
        self._sql_clear = f"DELETE FROM {table}{where_str}"
        self._sql_iter_values = f"SELECT {value_str} FROM {table}{where_str}"
        self._sql_iter_items = f"SELECT {key_str},{value_str} FROM {table}{where_str}"

    def __delitem__(self, key, /):
        cur = execute(
            self.con, 
            self._sql_delitem, 
            key, 
            commit=True, 
        )
        cur.close()
        if not cur.rowcount:
            raise KeyError(key)

    def __getitem__(self, key, /):
        return find(
            self.con, 
            self._sql_getitem, 
            key, 
            default=KeyError(key), 
            row_factory="any" if self._value_is_tuple else "one", 
        )

    def __setitem__(self, key, val, /):
        if not isinstance(key, tuple):
            key = key,
        if not isinstance(val, tuple):
            val = val,
        execute(self.con, self._sql_setitem, key + val, commit=True).close()

    def __iter__(self, /) -> Iterator:
        return query(
            self.con, 
            self._sql_iter, 
            row_factory="any" if self._key_is_tuple else "one", 
        )

    def __len__(self, /) -> int:
        return find(self.con, self._sql_len)

    def clear(self, /):
        execute(self.con, self._sql_clear, commit=True)

    def iter_values(self, /) -> Iterator:
        return query(
            self.con, 
            self._sql_iter_values, 
            row_factory="any" if self._value_is_tuple else "one", 
        )

    def iter_items(self, /) -> Iterator:
        key_len, val_len = self._key_len, self._value_len
        def row_factory(_, record):
            key = record[:key_len] if key_len else record[0]
            val = record[-val_len:] if val_len else record[-1]
            return key, val
        return query(
            self.con, 
            self._sql_iter_items, 
            row_factory=row_factory, 
        )

