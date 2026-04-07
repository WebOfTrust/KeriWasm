"""In-memory lmdb adapter for Pyodide/WASM environments.

keripy uses ``lmdb`` (C-extension) for persistent key-value storage.
There is no WASM wheel for lmdb — in the browser we use IndexedDB instead
via ``indexeddb_python.IndexedDBer``.

This module provides an in-memory dict-backed implementation of the subset
of the ``lmdb`` API that keripy actually calls.  Data is volatile — a page
refresh loses everything.  A future persistence layer can wrap this with
async load/save to IndexedDB without changing the synchronous interface
that keripy expects.

Implemented surface (matches keripy ``dbing.py`` and ``subing.py`` usage):

    lmdb.open(path, …)          → Environment
    env.open_db(key, dupsort)    → _Database
    env.begin(db, write, buffers)→ Transaction  (context manager)
    env.close()

    txn.get / put / delete
    txn.cursor()                 → Cursor

    cursor positioning:  set_range, set_key, first, last, prev
    cursor reading:      key, value, item, get
    cursor iteration:    iternext, iterprev, iternext_dup, __iter__
    cursor writing:      put, delete, replace
    cursor dupsort:      last_dup, count, iternext_dup
"""

from __future__ import annotations

from bisect import bisect_left


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class BadValsizeError(Exception):
    """Raised when a key or value violates size constraints."""


# ---------------------------------------------------------------------------
# Module-level constants (referenced at import time by keripy)
# ---------------------------------------------------------------------------

NOTLS = 0x200000
NORDAHEAD = 0x100000
NOSYNC = 0x10000
WRITEMAP = 0x80000


# ---------------------------------------------------------------------------
# _Database — one named sub-database
# ---------------------------------------------------------------------------


class _Database:
    """In-memory representation of a single named lmdb sub-database."""

    __slots__ = ("_name", "_dupsort", "_data")

    def __init__(self, name: bytes, dupsort: bool = False):
        self._name = name
        self._dupsort = dupsort
        # dupsort=False → dict[bytes, bytes]
        # dupsort=True  → dict[bytes, list[bytes]]  (sorted list of dup values)
        self._data: dict = {}


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


class _Cursor:
    """Cursor over a _Database's sorted keyspace."""

    __slots__ = ("_db", "_txn", "_keys", "_pos", "_dup_pos")

    def __init__(self, db: _Database, txn: _Transaction):
        self._db = db
        self._txn = txn
        self._keys: list[bytes] = sorted(db._data.keys())
        self._pos: int = -1  # current position in _keys (-1 = unpositioned)
        self._dup_pos: int = 0  # position within dup values for dupsort dbs

    # -- internal helpers ---------------------------------------------------

    def _refresh_keys(self):
        """Rebuild sorted key list after a mutation."""
        self._keys = sorted(self._db._data.keys())

    def _valid(self) -> bool:
        return 0 <= self._pos < len(self._keys)

    def _current_key(self) -> bytes:
        if self._valid():
            return self._keys[self._pos]
        return b""

    def _current_val(self):
        if not self._valid():
            return b""
        k = self._keys[self._pos]
        v = self._db._data.get(k, b"")
        if self._db._dupsort:
            if isinstance(v, list) and v:
                idx = min(self._dup_pos, len(v) - 1)
                return v[idx]
            return b""
        return v

    # -- positioning --------------------------------------------------------

    def first(self) -> bool:
        self._refresh_keys()
        if self._keys:
            self._pos = 0
            self._dup_pos = 0
            return True
        self._pos = -1
        return False

    def last(self) -> bool:
        self._refresh_keys()
        if self._keys:
            self._pos = len(self._keys) - 1
            self._dup_pos = 0
            if self._db._dupsort:
                v = self._db._data.get(self._keys[self._pos])
                if isinstance(v, list) and v:
                    self._dup_pos = len(v) - 1
            return True
        self._pos = -1
        return False

    def prev(self) -> bool:
        if self._pos > 0:
            self._pos -= 1
            self._dup_pos = 0
            return True
        self._pos = -1
        return False

    def set_range(self, key: bytes) -> bool:
        """Position at first key >= *key*."""
        self._refresh_keys()
        idx = bisect_left(self._keys, key)
        if idx < len(self._keys):
            self._pos = idx
            self._dup_pos = 0
            return True
        self._pos = -1
        return False

    def set_key(self, key: bytes) -> bool:
        """Position at exact *key* (first dup if dupsort)."""
        self._refresh_keys()
        idx = bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            self._pos = idx
            self._dup_pos = 0
            return True
        self._pos = -1
        return False

    # -- reading ------------------------------------------------------------

    def key(self) -> bytes:
        return self._current_key()

    def value(self):
        return self._current_val()

    def item(self) -> tuple:
        return (self._current_key(), self._current_val())

    def get(self, key: bytes):
        """Lookup *key* and position cursor there.  Returns value or None."""
        if self.set_key(key):
            return self._current_val()
        return None

    # -- iteration ----------------------------------------------------------

    def iternext(self, *, keys: bool = True, values: bool = True):
        """Yield forward from current position.  Matches real lmdb semantics:
        yields current item then advances."""
        self._refresh_keys()
        while self._valid():
            k = self._keys[self._pos]
            if self._db._dupsort:
                dups = self._db._data.get(k, [])
                if not isinstance(dups, list):
                    dups = [dups]
                while self._dup_pos < len(dups):
                    v = dups[self._dup_pos]
                    self._dup_pos += 1
                    if keys and values:
                        yield (k, v)
                    elif keys:
                        yield k
                    else:
                        yield v
                self._pos += 1
                self._dup_pos = 0
            else:
                v = self._db._data.get(k, b"")
                self._pos += 1
                if keys and values:
                    yield (k, v)
                elif keys:
                    yield k
                else:
                    yield v

    def iterprev(self):
        """Yield backward from current position."""
        self._refresh_keys()
        while self._valid():
            k = self._keys[self._pos]
            if self._db._dupsort:
                dups = self._db._data.get(k, [])
                if not isinstance(dups, list):
                    dups = [dups]
                while self._dup_pos >= 0 and self._dup_pos < len(dups):
                    v = dups[self._dup_pos]
                    self._dup_pos -= 1
                    yield (k, v)
                self._pos -= 1
                if self._valid():
                    nk = self._keys[self._pos]
                    nd = self._db._data.get(nk, [])
                    if isinstance(nd, list):
                        self._dup_pos = len(nd) - 1
                    else:
                        self._dup_pos = 0
            else:
                v = self._db._data.get(k, b"")
                self._pos -= 1
                yield (k, v)

    def __iter__(self):
        return self.iternext()

    # -- dupsort iteration --------------------------------------------------

    def iternext_dup(self):
        """Yield all duplicate values at the current key."""
        if not self._valid():
            return
        k = self._keys[self._pos]
        dups = self._db._data.get(k, [])
        if not isinstance(dups, list):
            dups = [dups]
        for v in dups:
            yield v

    def last_dup(self) -> bool:
        """Move to last duplicate value at current key."""
        if not self._valid():
            return False
        k = self._keys[self._pos]
        dups = self._db._data.get(k, [])
        if isinstance(dups, list) and dups:
            self._dup_pos = len(dups) - 1
            return True
        return False

    def count(self) -> int:
        """Count duplicate values at current key."""
        if not self._valid():
            return 0
        k = self._keys[self._pos]
        dups = self._db._data.get(k, [])
        if isinstance(dups, list):
            return len(dups)
        return 1

    # -- writing ------------------------------------------------------------

    def put(
        self, key: bytes, val: bytes, *, overwrite: bool = True, dupdata: bool = True
    ) -> bool:
        self._txn._check_write()
        if self._db._dupsort:
            dups = self._db._data.setdefault(key, [])
            if not isinstance(dups, list):
                dups = [dups]
                self._db._data[key] = dups
            if not dupdata:
                # dupdata=False means don't add if exact (key, val) exists
                if val in dups:
                    return False
            if val not in dups:
                dups.append(val)
                dups.sort()
            else:
                if overwrite:
                    idx = dups.index(val)
                    dups[idx] = val
            self._refresh_keys()
            self.set_key(key)
            return True
        else:
            if key in self._db._data and not overwrite:
                return False
            self._db._data[key] = val
            self._refresh_keys()
            self.set_key(key)
            return True

    def delete(self) -> bool:
        """Delete entry at cursor position.  Cursor moves to next item."""
        self._txn._check_write()
        if not self._valid():
            return False
        k = self._keys[self._pos]
        if self._db._dupsort:
            dups = self._db._data.get(k, [])
            if isinstance(dups, list) and dups:
                idx = min(self._dup_pos, len(dups) - 1)
                dups.pop(idx)
                if not dups:
                    del self._db._data[k]
            else:
                if k in self._db._data:
                    del self._db._data[k]
        else:
            if k in self._db._data:
                del self._db._data[k]
        self._refresh_keys()
        # cursor stays at same index (now pointing to next item or end)
        if self._pos >= len(self._keys):
            self._pos = len(self._keys)  # past end
        self._dup_pos = 0
        return True

    def replace(self, key: bytes, val: bytes) -> bool:
        """Replace value at *key* (used by LMDBer.setVer)."""
        self._txn._check_write()
        if self._db._dupsort:
            self._db._data[key] = [val]
        else:
            self._db._data[key] = val
        self._refresh_keys()
        self.set_key(key)
        return True


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


class _Transaction:
    """Synchronous transaction wrapping a _Database reference."""

    __slots__ = ("_db", "_env", "_write", "_closed")

    def __init__(self, env: Environment, db: _Database | None, write: bool):
        self._env = env
        self._db = db if db is not None else env._default_db
        self._write = write
        self._closed = False

    def _check_write(self):
        if not self._write:
            raise Exception("Attempt to write in a read-only transaction")

    # -- context manager ----------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._closed = True
        return False

    # -- operations ---------------------------------------------------------

    def get(self, key: bytes):
        data = self._db._data
        if self._db._dupsort:
            dups = data.get(key)
            if isinstance(dups, list) and dups:
                return dups[0]
            return None
        return data.get(key)

    def put(
        self, key: bytes, val: bytes, *, overwrite: bool = True, dupdata: bool = True
    ) -> bool:
        self._check_write()
        if self._db._dupsort:
            dups = self._db._data.setdefault(key, [])
            if not isinstance(dups, list):
                dups = [dups]
                self._db._data[key] = dups
            if not dupdata and val in dups:
                return False
            if val not in dups:
                dups.append(val)
                dups.sort()
                return True
            if overwrite:
                return True
            return False
        else:
            if key in self._db._data and not overwrite:
                return False
            self._db._data[key] = val
            return True

    def delete(self, key: bytes, val: bytes | None = None) -> bool:
        self._check_write()
        if self._db._dupsort:
            dups = self._db._data.get(key)
            if not isinstance(dups, list) or not dups:
                return False
            if val:
                if val in dups:
                    dups.remove(val)
                    if not dups:
                        del self._db._data[key]
                    return True
                return False
            else:
                del self._db._data[key]
                return True
        else:
            if key in self._db._data:
                del self._db._data[key]
                return True
            return False

    def cursor(self) -> _Cursor:
        return _Cursor(self._db, self)


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


class Environment:
    """In-memory replacement for ``lmdb.Environment``."""

    __slots__ = ("_path", "_dbs", "_default_db", "_readonly", "_closed")

    def __init__(
        self,
        path: str,
        *,
        max_dbs: int = 0,
        map_size: int = 0,
        mode: int = 0,
        readonly: bool = False,
    ):
        self._path = path
        self._readonly = readonly
        self._closed = False
        # The default (unnamed) database — used by getVer/setVer
        self._default_db = _Database(name=b"", dupsort=False)
        self._dbs: dict[bytes, _Database] = {}

    def open_db(self, key: bytes = b"", dupsort: bool = False) -> _Database:
        if key not in self._dbs:
            self._dbs[key] = _Database(name=key, dupsort=dupsort)
        return self._dbs[key]

    def begin(
        self, *, db: _Database | None = None, write: bool = False, buffers: bool = False
    ) -> _Transaction:
        return _Transaction(env=self, db=db, write=write)

    def close(self):
        self._closed = True


# ---------------------------------------------------------------------------
# Module-level open() function
# ---------------------------------------------------------------------------


def open(
    path: str,
    *,
    max_dbs: int = 0,
    map_size: int = 0,  # noqa: A001
    mode: int = 0,
    readonly: bool = False,
) -> Environment:
    """Drop-in for ``lmdb.open()``."""
    return Environment(
        path, max_dbs=max_dbs, map_size=map_size, mode=mode, readonly=readonly
    )
