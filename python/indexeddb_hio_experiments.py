"""
Worker-hosted hio/IndexedDB experiments for KeriWasm.

This module is intentionally KeriWasm-only. It exercises two questions:

1. Can raw async IndexedDB operations run reliably under upstream-style
   ``Doist.ado(real=True)`` using the create-task-and-poll pattern?
2. Can a sync-looking LMDBer subset sit on top of the async backend closely
   enough to preserve existing Suber/Komer call shapes for runtime CRUD?

The sync façade here is a spike, not a production design.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Callable, Iterable

from hio.base.doing import Doer, Doist
from hio.help import timing

from indexeddb_python import IndexedDBer, deleteDatabase, suffix, unsuffix


LogFn = Callable[[str, str], None]


class MirrorDivergenceError(RuntimeError):
    """Raised when the sync façade mirror no longer matches backend state."""


class _RequireError(AssertionError):
    """Private assertion helper for experiment failures."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise _RequireError(message)


def _log_section(log: LogFn, title: str) -> None:
    log("")
    log(title)
    log("-" * len(title))


def _normalize_store_name(db: Any) -> str:
    if isinstance(db, str):
        return db
    if isinstance(db, memoryview):
        db = bytes(db)
    if isinstance(db, bytes):
        return db.decode("utf-8")
    if hasattr(db, "decode"):
        return db.decode("utf-8")
    raise TypeError(f"Unsupported store handle type: {type(db)}")


def _normalize_key(key: bytes | str | memoryview) -> bytes:
    if isinstance(key, memoryview):
        return bytes(key)
    if isinstance(key, bytes):
        return key
    if isinstance(key, str):
        return key.encode("utf-8")
    raise TypeError(f"Unsupported key type: {type(key)}")


def _normalize_sep(sep: bytes | str) -> bytes:
    if isinstance(sep, bytes):
        return sep
    if isinstance(sep, str):
        return sep.encode("utf-8")
    raise TypeError(f"Unsupported separator type: {type(sep)}")


def _normalize_text(val: str | bytes | memoryview) -> bytes:
    if isinstance(val, memoryview):
        return bytes(val)
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return val.encode("utf-8")
    raise TypeError(f"Unsupported value type: {type(val)}")


@dataclass(frozen=True)
class PendingOp:
    """Deterministic write queued by the sync façade."""

    method: str
    kwargs: dict[str, Any]
    expected: Any = None


class IndexedDBEnv:
    """
    Minimal ``env.open_db`` surface so existing Suber/Komer call shapes work.

    Runtime CRUD only: stores must already exist in the opened backend.
    """

    def __init__(self, owner: "SyncIndexedDBFacade"):
        self.owner = owner

    def open_db(self, key: bytes | str, dupsort: bool = False):
        store = _normalize_store_name(key)
        if store not in self.owner.stores:
            raise KeyError(f"Store not configured in IndexedDB backend: {store}")
        self.owner._store_meta[store] = {"dupsort": bool(dupsort)}
        return store


class SyncIndexedDBFacade:
    """
    Experimental LMDBer-like sync façade over the async IndexedDB backend.

    Reads come from the in-memory mirror immediately. Writes enqueue explicit
    backend operations and are persisted later by ``await flush()`` under
    ``Doist.ado(real=True)``.
    """

    def __init__(self, backend: IndexedDBer):
        self.backend = backend
        self.stores = [_normalize_store_name(store) for store in (backend.stores or [])]
        self.env = IndexedDBEnv(self)
        self._mirror: dict[str, dict[bytes, bytes]] = {store: {} for store in self.stores}
        self._store_meta: dict[str, dict[str, Any]] = {}
        self._pending: list[PendingOp] = []
        self._diverged = False
        self._divergence_reason = ""

    @classmethod
    async def open(
        cls,
        name: str,
        stores: list[str],
        version: int = 1,
    ) -> "SyncIndexedDBFacade":
        backend = await IndexedDBer.open(name, stores, version=version)
        instance = cls(backend)
        await instance.prime()
        return instance

    @classmethod
    async def from_backend(cls, backend: IndexedDBer) -> "SyncIndexedDBFacade":
        instance = cls(backend)
        await instance.prime()
        return instance

    async def prime(self) -> None:
        """Load the current backend state into the in-memory mirror."""
        self._ensure_consistent()
        for store in self.stores:
            items = await self.backend.getTopItemIter(store)
            self._mirror[store] = {bytes(key): bytes(val) for key, val in items}

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def diverged(self) -> bool:
        return self._diverged

    def close(self) -> None:
        self.backend.close()

    def _ensure_consistent(self) -> None:
        if self._diverged:
            raise MirrorDivergenceError(self._divergence_reason)

    def _store_map(self, db: Any) -> dict[bytes, bytes]:
        self._ensure_consistent()
        store = _normalize_store_name(db)
        if store not in self._mirror:
            self._mirror[store] = {}
            if store not in self.stores:
                self.stores.append(store)
        return self._mirror[store]

    def _queue(self, method: str, *, expected: Any = None, **kwargs: Any) -> None:
        self._pending.append(PendingOp(method=method, kwargs=kwargs, expected=expected))

    def _sorted_items(self, db: Any) -> list[tuple[bytes, bytes]]:
        return sorted(self._store_map(db).items(), key=lambda item: item[0])

    def _iter_on_items(
        self, db: Any, *, key: bytes | None = None, on: int = 0, sep: bytes = b"."
    ) -> list[tuple[bytes, int, bytes]]:
        items: list[tuple[bytes, int, bytes]] = []
        for actual_key, val in self._sorted_items(db):
            try:
                prefix, ordinal = unsuffix(actual_key, sep=sep)
            except ValueError:
                continue
            if key is not None and prefix != key:
                continue
            if ordinal < on:
                continue
            items.append((prefix, ordinal, val))
        return items

    def cntAll(self, db: Any) -> int:
        return len(self._store_map(db))

    def getTopItemIter(self, db: Any, top: bytes = b"") -> list[tuple[bytes, bytes]]:
        top = _normalize_key(top) if top else b""
        return [
            (key, val)
            for key, val in self._sorted_items(db)
            if not top or key.startswith(top)
        ]

    def putVal(self, db: Any, key: bytes, val: bytes) -> bool:
        key = _normalize_key(key)
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )
        val = _normalize_text(val)
        store = _normalize_store_name(db)
        mirror = self._store_map(store)
        if key in mirror:
            return False
        mirror[key] = val
        self._queue("putVal", db=store, key=key, val=val, expected=True)
        return True

    def setVal(self, db: Any, key: bytes, val: bytes) -> bool:
        key = _normalize_key(key)
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )
        val = _normalize_text(val)
        store = _normalize_store_name(db)
        mirror = self._store_map(store)
        mirror[key] = val
        self._queue("setVal", db=store, key=key, val=val, expected=True)
        return True

    def getVal(self, db: Any, key: bytes) -> bytes | None:
        key = _normalize_key(key)
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )
        return self._store_map(db).get(key)

    def delVal(self, db: Any, key: bytes) -> bool:
        key = _normalize_key(key)
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )
        store = _normalize_store_name(db)
        mirror = self._store_map(store)
        if key not in mirror:
            return False
        del mirror[key]
        self._queue("delVal", db=store, key=key, expected=True)
        return True

    def delTop(self, db: Any, top: bytes = b"") -> bool:
        top = _normalize_key(top) if top else b""
        store = _normalize_store_name(db)
        mirror = self._store_map(store)
        doomed = [key for key in sorted(mirror) if not top or key.startswith(top)]
        if not doomed:
            return False
        for key in doomed:
            del mirror[key]
            self._queue("delVal", db=store, key=key, expected=True)
        return True

    def putOnVal(
        self, db: Any, key: bytes, on: int = 0, val: bytes | None = b"", *, sep: bytes = b"."
    ) -> bool:
        if val is None or not key:
            return False
        actual_key = suffix(_normalize_key(key), on, sep=_normalize_sep(sep))
        return self.putVal(db, actual_key, _normalize_text(val))

    def pinOnVal(
        self, db: Any, key: bytes, on: int = 0, val: bytes | None = b"", *, sep: bytes = b"."
    ) -> bool:
        if val is None or not key:
            return False
        return self.setOnVal(db=db, key=key, on=on, val=val, sep=sep)

    def setOnVal(
        self, db: Any, key: bytes, on: int = 0, val: bytes | None = b"", *, sep: bytes = b"."
    ) -> bool:
        if val is None or not key:
            return False
        key = _normalize_key(key)
        sep = _normalize_sep(sep)
        actual_key = suffix(key, on, sep=sep)
        store = _normalize_store_name(db)
        mirror = self._store_map(store)
        bval = _normalize_text(val)
        mirror[actual_key] = bval
        self._queue("setOnVal", db=store, key=key, on=on, val=bval, sep=sep, expected=True)
        return True

    def appendOnVal(self, db: Any, key: bytes, val: bytes, *, sep: bytes = b".") -> int:
        key = _normalize_key(key)
        sep = _normalize_sep(sep)
        if not key or val is None:
            raise ValueError(f"Bad append parameter: key={key!r} or val={val!r}")
        items = self._iter_on_items(db, key=key, sep=sep)
        on = items[-1][1] + 1 if items else 0
        self.setOnVal(db=db, key=key, on=on, val=_normalize_text(val), sep=sep)
        return on

    def getOnVal(self, db: Any, key: bytes, on: int = 0, *, sep: bytes = b".") -> bytes | None:
        if not key:
            return None
        actual_key = suffix(_normalize_key(key), on, sep=_normalize_sep(sep))
        return self._store_map(db).get(actual_key)

    def getOnItem(
        self, db: Any, key: bytes, on: int = 0, *, sep: bytes = b"."
    ) -> tuple[bytes, int, bytes] | None:
        val = self.getOnVal(db=db, key=key, on=on, sep=sep)
        if val is None:
            return None
        return (_normalize_key(key), on, val)

    def remOn(self, db: Any, key: bytes, on: int = 0, *, sep: bytes = b".") -> bool:
        if not key:
            return False
        store = _normalize_store_name(db)
        actual_key = suffix(_normalize_key(key), on, sep=_normalize_sep(sep))
        mirror = self._store_map(store)
        if actual_key not in mirror:
            return False
        del mirror[actual_key]
        self._queue(
            "delOnVal",
            db=store,
            key=_normalize_key(key),
            on=on,
            sep=_normalize_sep(sep),
            expected=True,
        )
        return True

    def remOnAll(self, db: Any, key: bytes = b"", on: int = 0, *, sep: bytes = b".") -> bool:
        store = _normalize_store_name(db)
        sep = _normalize_sep(sep)
        if not key:
            return self.delTop(store, b"")
        key = _normalize_key(key)
        doomed = [
            (prefix, ordinal)
            for prefix, ordinal, _val in self._iter_on_items(store, key=key, on=on, sep=sep)
        ]
        if not doomed:
            return False
        for prefix, ordinal in doomed:
            self.remOn(store, key=prefix, on=ordinal, sep=sep)
        return True

    def cntOnAll(self, db: Any, key: bytes = b"", on: int = 0, *, sep: bytes = b".") -> int:
        sep = _normalize_sep(sep)
        if key:
            return len(self._iter_on_items(db, key=_normalize_key(key), on=on, sep=sep))
        return len(self._iter_on_items(db, key=None, on=on, sep=sep))

    def getOnTopItemIter(
        self, db: Any, top: bytes = b"", *, sep: bytes = b"."
    ) -> list[tuple[bytes, int, bytes]]:
        top = _normalize_key(top) if top else b""
        sep = _normalize_sep(sep)
        return [
            (prefix, ordinal, val)
            for prefix, ordinal, val in self._iter_on_items(db, key=None, on=0, sep=sep)
            if not top or prefix.startswith(top)
        ]

    def getOnAllItemIter(
        self, db: Any, key: bytes = b"", on: int = 0, *, sep: bytes = b"."
    ) -> list[tuple[bytes, int, bytes]]:
        sep = _normalize_sep(sep)
        norm_key = _normalize_key(key) if key else None
        return self._iter_on_items(db, key=norm_key, on=on, sep=sep)

    async def flush(self) -> int:
        """
        Persist queued writes in order.

        Once a flush fails, the façade is marked divergent and becomes unusable
        until rebuilt from backend state.
        """
        self._ensure_consistent()
        if not self._pending:
            return 0

        completed = 0
        for op in list(self._pending):
            try:
                result = await getattr(self.backend, op.method)(**op.kwargs)
            except Exception as exc:
                self._diverged = True
                self._divergence_reason = f"Flush failed in {op.method}: {exc}"
                raise MirrorDivergenceError(self._divergence_reason) from exc

            if op.expected is not None and result != op.expected:
                self._diverged = True
                self._divergence_reason = (
                    f"Flush divergence in {op.method}: expected {op.expected!r}, got {result!r}"
                )
                raise MirrorDivergenceError(self._divergence_reason)

            completed += 1
            self._pending.pop(0)

        return completed


class CompatSuber:
    """Small compatibility probe for keripy Suber call shapes."""

    Sep = "."

    def __init__(self, db: SyncIndexedDBFacade, *, subkey: str = "docs.", sep: str | None = None):
        self.db = db
        self.sdb = self.db.env.open_db(key=subkey.encode("utf-8"), dupsort=False)
        self.sep = sep if sep is not None else self.Sep

    def _tokey(
        self,
        keys: str | bytes | memoryview | Iterable[str | bytes | memoryview],
        *,
        topive: bool = False,
    ) -> bytes:
        if hasattr(keys, "encode"):
            return keys.encode()
        if isinstance(keys, memoryview):
            return bytes(keys)
        if hasattr(keys, "decode"):
            return bytes(keys)
        keys = tuple(keys)
        if topive and keys and keys[-1]:
            keys = keys + ("",)
        return self.sep.join(
            key if hasattr(key, "encode") else bytes(key).decode("utf-8") for key in keys
        ).encode("utf-8")

    def _tokeys(self, key: str | bytes | memoryview) -> tuple[str, ...]:
        if isinstance(key, memoryview):
            key = bytes(key)
        if hasattr(key, "decode"):
            key = key.decode("utf-8")
        return tuple(str(key).split(self.sep))

    def _ser(self, val: str | bytes | memoryview) -> bytes:
        return _normalize_text(val)

    def _des(self, val: bytes | memoryview | None) -> str | None:
        if val is None:
            return None
        if isinstance(val, memoryview):
            val = bytes(val)
        return val.decode("utf-8") if hasattr(val, "decode") else val

    def put(self, keys: str | Iterable, val: str | bytes) -> bool:
        return self.db.putVal(db=self.sdb, key=self._tokey(keys), val=self._ser(val))

    def pin(self, keys: str | Iterable, val: str | bytes) -> bool:
        return self.db.setVal(db=self.sdb, key=self._tokey(keys), val=self._ser(val))

    def get(self, keys: str | Iterable) -> str | None:
        return self._des(self.db.getVal(db=self.sdb, key=self._tokey(keys)))

    def rem(self, keys: str | Iterable) -> bool:
        return self.db.delVal(db=self.sdb, key=self._tokey(keys))

    def getItemIter(
        self, keys: str | bytes | memoryview | Iterable = b"", *, topive: bool = False
    ) -> list[tuple[tuple[str, ...], str]]:
        items = self.db.getTopItemIter(db=self.sdb, top=self._tokey(keys, topive=topive))
        return [(self._tokeys(key), self._des(val)) for key, val in items]


class CompatOnSuber(CompatSuber):
    """Small compatibility probe for keripy OnSuber call shapes."""

    def putOn(self, keys: str | Iterable, on: int = 0, val: str | bytes | None = None) -> bool:
        if val is None:
            return False
        return self.db.putOnVal(
            db=self.sdb,
            key=self._tokey(keys),
            on=on,
            val=self._ser(val),
            sep=self.sep.encode("utf-8"),
        )

    def pinOn(self, keys: str | Iterable, on: int = 0, val: str | bytes | None = None) -> bool:
        if val is None:
            return False
        return self.db.pinOnVal(
            db=self.sdb,
            key=self._tokey(keys),
            on=on,
            val=self._ser(val),
            sep=self.sep.encode("utf-8"),
        )

    def appendOn(self, keys: str | Iterable, val: str | bytes) -> int:
        return self.db.appendOnVal(
            db=self.sdb,
            key=self._tokey(keys),
            val=self._ser(val),
            sep=self.sep.encode("utf-8"),
        )

    def getOn(self, keys: str | Iterable, on: int = 0) -> str | None:
        val = self.db.getOnVal(
            db=self.sdb,
            key=self._tokey(keys),
            on=on,
            sep=self.sep.encode("utf-8"),
        )
        return self._des(val)

    def remOn(self, keys: str | Iterable, on: int = 0) -> bool:
        return self.db.remOn(
            db=self.sdb,
            key=self._tokey(keys),
            on=on,
            sep=self.sep.encode("utf-8"),
        )

    def cntOnAll(self, keys: str | Iterable = b"", on: int = 0) -> int:
        return self.db.cntOnAll(
            db=self.sdb,
            key=self._tokey(keys) if keys else b"",
            on=on,
            sep=self.sep.encode("utf-8"),
        )

    def getOnItemIter(
        self, keys: str | Iterable = b"", on: int = 0
    ) -> list[tuple[tuple[str, ...], int, str]]:
        items = self.db.getOnAllItemIter(
            db=self.sdb,
            key=self._tokey(keys) if keys else b"",
            on=on,
            sep=self.sep.encode("utf-8"),
        )
        return [(self._tokeys(key), ordinal, self._des(val)) for key, ordinal, val in items]


class CompatKomer:
    """Small compatibility probe for keripy Komer call shapes."""

    Sep = "."

    def __init__(self, db: SyncIndexedDBFacade, *, subkey: str, schema: type, sep: str | None = None):
        self.db = db
        self.sdb = self.db.env.open_db(key=subkey.encode("utf-8"), dupsort=False)
        self.schema = schema
        self.sep = sep if sep is not None else self.Sep

    def _tokey(self, keys: str | bytes | memoryview | Iterable[str | bytes | memoryview]) -> bytes:
        if hasattr(keys, "encode"):
            return keys.encode("utf-8")
        if isinstance(keys, memoryview):
            return bytes(keys)
        if hasattr(keys, "decode"):
            return bytes(keys)
        return self.sep.join(
            key.decode("utf-8") if hasattr(key, "decode") else str(key) for key in keys
        ).encode("utf-8")

    def _serialize(self, val: Any) -> bytes:
        if not is_dataclass(val):
            raise ValueError(f"Expected dataclass instance, got {type(val)}")
        return json.dumps(asdict(val), separators=(",", ":")).encode("utf-8")

    def _deserialize(self, raw: bytes | None) -> Any | None:
        if raw is None:
            return None
        return self.schema(**json.loads(raw.decode("utf-8")))

    def put(self, keys: str | Iterable, val: Any) -> bool:
        return self.db.putVal(db=self.sdb, key=self._tokey(keys), val=self._serialize(val))

    def pin(self, keys: str | Iterable, val: Any) -> bool:
        return self.db.setVal(db=self.sdb, key=self._tokey(keys), val=self._serialize(val))

    def get(self, keys: str | Iterable) -> Any | None:
        return self._deserialize(self.db.getVal(db=self.sdb, key=self._tokey(keys)))

    def rem(self, keys: str | Iterable) -> bool:
        return self.db.delVal(db=self.sdb, key=self._tokey(keys))


class TaskPollingDoer(Doer):
    """Doer that starts one asyncio task in enter() and polls in recur()."""

    def __init__(self, *, label: str, runner: Callable[[], Any], log: LogFn, **kwa):
        super().__init__(**kwa)
        self.label = label
        self.runner = runner
        self.log = log
        self._task: asyncio.Task | None = None
        self.result: Any = None

    def enter(self):
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self.runner(), name=self.label)
        self.log(f"[doer:{self.label}] started task {self._task.get_name()}", "info")

    def recur(self, tyme):
        if self._task is None:
            raise RuntimeError(f"TaskPollingDoer {self.label} entered without a task")
        if not self._task.done():
            return False
        self.result = self._task.result()
        self.log(f"[doer:{self.label}] completed at tyme={tyme:.4f}", "success")
        return True

    def exit(self):
        if self._task is not None and not self._task.done():
            self._task.cancel()


class TickDoer(Doer):
    """Simple synchronous doer to prove ado() scheduling itself works."""

    def __init__(self, *, stop: int = 3, **kwa):
        super().__init__(**kwa)
        self.stop = stop
        self.seen: list[float] = []

    def recur(self, tyme):
        self.seen.append(round(float(tyme), 4))
        return len(self.seen) >= self.stop


@dataclass(frozen=True)
class WalletRecord:
    aid: str
    state: str


async def _run_doist_with(doers: list[Doer], *, tock: float = 0.02, limit: float = 5.0) -> Doist:
    doist = Doist(real=True, doers=doers, tock=tock, limit=limit)
    await doist.ado()
    return doist


async def _direct_async_backend_tests(log: LogFn) -> None:
    db_name = "keriwasm-worker-hio-direct"
    stores = ["val_store", "ordinal_store"]

    try:
        await deleteDatabase(db_name)
    except Exception:
        pass

    dber = await IndexedDBer.open(db_name, stores, version=1)
    try:
        log("Opened raw async IndexedDB backend", "success")

        _require(await dber.setVal("val_store", b"alpha", b"one") is True, "setVal failed")
        _require(await dber.getVal("val_store", b"alpha") == b"one", "getVal mismatch")
        _require(await dber.putVal("val_store", b"alpha", b"other") is False, "putVal overwrote")
        _require(await dber.setVal("val_store", b"alphabet", b"two") is True, "second setVal failed")

        items = await dber.getTopItemIter("val_store", b"alph")
        _require(items == [(b"alpha", b"one"), (b"alphabet", b"two")], f"prefix iteration mismatch: {items!r}")
        log("Val CRUD + prefix iteration succeeded", "success")

        on0 = await dber.appendOnVal("ordinal_store", b"evt", b"first")
        on1 = await dber.appendOnVal("ordinal_store", b"evt", b"second")
        _require((on0, on1) == (0, 1), f"ordinal append mismatch: {(on0, on1)!r}")
        _require(await dber.getOnVal("ordinal_store", b"evt", 0) == b"first", "ordinal read 0 mismatch")
        _require(await dber.getOnVal("ordinal_store", b"evt", 1) == b"second", "ordinal read 1 mismatch")
        _require(await dber.cntOnVals("ordinal_store", b"evt") == 2, "ordinal count mismatch")
        _require(await dber.delOnVal("ordinal_store", b"evt", 0) is True, "ordinal delete failed")
        _require(await dber.cntOnVals("ordinal_store", b"evt") == 1, "ordinal count after delete mismatch")
        log("Ordinal family CRUD succeeded", "success")

        dber.close()
        dber = await IndexedDBer.open(db_name, stores, version=1)
        _require(await dber.getVal("val_store", b"alpha") == b"one", "persisted readback mismatch")
        log("Write completion survived close/reopen", "success")

        try:
            await dber.getVal("val_store", b"")
        except KeyError as exc:
            log(f"Expected KeyError surfaced: {exc}", "success")
        else:
            raise _RequireError("Empty-key KeyError did not surface")
    finally:
        dber.close()
        try:
            await deleteDatabase(db_name)
        except Exception:
            pass


async def _experiment_send_none_negative(log: LogFn) -> None:
    async def loop_bound():
        await asyncio.sleep(0.01)
        return "done"

    coro = loop_bound()
    try:
        first = coro.send(None)
        log(f"First send(None) yielded {type(first).__name__}", "info")
        try:
            coro.send(None)
        except RuntimeError as exc:
            _require(
                "await wasn't used with future" in str(exc) or "future" in str(exc).lower(),
                f"Unexpected RuntimeError: {exc}",
            )
            log("send(None) on loop-backed coroutine failed as expected", "success")
        else:
            raise _RequireError("send(None) unexpectedly drove loop-backed coroutine")
    finally:
        coro.close()


async def _experiment_real_false_negative(log: LogFn) -> None:
    doer = TickDoer(stop=1, tock=0.0)
    doist = Doist(real=False, doers=[doer], tock=0.01, limit=1.0)
    try:
        await doist.ado()
    except timing.TimerError as exc:
        log(f"ado(real=False) rejected as expected: {exc}", "success")
    else:
        raise _RequireError("ado(real=False) unexpectedly succeeded")


async def _experiment_plain_do_negative(log: LogFn) -> None:
    db_name = "keriwasm-worker-hio-plain-do"
    stores = ["plain_store"]

    try:
        await deleteDatabase(db_name)
    except Exception:
        pass

    dber = await IndexedDBer.open(db_name, stores, version=1)

    async def delayed_write():
        await asyncio.sleep(0.05)
        await dber.setVal("plain_store", b"plain", b"done")
        return True

    doer = TaskPollingDoer(label="plain-do-negative", runner=delayed_write, log=log, tock=0.0)
    doist = Doist(real=True, doers=[doer], tock=0.01, limit=0.03)
    doist.do()

    pending = doer._task is not None and not doer._task.done()
    _require(pending, "Plain do() unexpectedly allowed async task completion")
    doer.exit()
    log("Plain do() left async task pending as expected", "success")

    await asyncio.sleep(0)
    _require(await dber.getVal("plain_store", b"plain") is None, "Plain do() persisted data unexpectedly")
    log("Plain do() produced no false success", "success")

    dber.close()
    try:
        await deleteDatabase(db_name)
    except Exception:
        pass


async def _experiment_timeout_cancel_negative(log: LogFn) -> None:
    db_name = "keriwasm-worker-hio-timeout"
    stores = ["timeout_store"]

    try:
        await deleteDatabase(db_name)
    except Exception:
        pass

    dber = await IndexedDBer.open(db_name, stores, version=1)

    async def delayed_write():
        await asyncio.sleep(0.2)
        await dber.setVal("timeout_store", b"late", b"value")
        return True

    doer = TaskPollingDoer(label="timeout-negative", runner=delayed_write, log=log, tock=0.0)
    await _run_doist_with([doer], tock=0.01, limit=0.05)
    await asyncio.sleep(0)
    _require(doer._task is not None and doer._task.cancelled(), "Timed-out task was not cancelled")
    _require(await dber.getVal("timeout_store", b"late") is None, "Timed-out task reported persistence")
    log("Timeout/cancel produced no persisted write", "success")

    dber.close()
    try:
        await deleteDatabase(db_name)
    except Exception:
        pass


async def _sync_facade_positive_tests(log: LogFn) -> None:
    db_name = "keriwasm-worker-hio-sync"
    stores = ["suber_store", "ordinal_store", "komer_store"]

    try:
        await deleteDatabase(db_name)
    except Exception:
        pass

    facade = await SyncIndexedDBFacade.open(db_name, stores, version=1)
    try:
        suber = CompatSuber(facade, subkey="suber_store")
        _require(suber.put(("alpha",), "one") is True, "Suber.put failed")
        _require(suber.get(("alpha",)) == "one", "Mirror read-your-writes failed")
        _require(facade.pending_count == 1, "Write queue did not record Suber.put")
        await facade.flush()
        facade.close()

        direct = await IndexedDBer.open(db_name, stores, version=1)
        try:
            _require(await direct.getVal("suber_store", b"alpha") == b"one", "Suber flush persistence mismatch")
            log("Suber-compatible val path preserved immediate reads and persisted flush", "success")
        finally:
            direct.close()

        facade = await SyncIndexedDBFacade.open(db_name, stores, version=1)
        onsuber = CompatOnSuber(facade, subkey="ordinal_store")
        _require(onsuber.appendOn(("evt",), "first") == 0, "First appendOn mismatch")
        _require(onsuber.appendOn(("evt",), "second") == 1, "Second appendOn mismatch")
        _require(onsuber.getOn(("evt",), 0) == "first", "Mirror ordinal read 0 mismatch")
        _require(onsuber.getOn(("evt",), 1) == "second", "Mirror ordinal read 1 mismatch")
        _require(onsuber.cntOnAll(("evt",), 0) == 2, "Mirror ordinal count mismatch")
        await facade.flush()
        items = facade.getOnAllItemIter(db="ordinal_store", key=b"evt", on=0, sep=b".")
        _require(
            [(key, ordinal, val) for key, ordinal, val in items]
            == [(b"evt", 0, b"first"), (b"evt", 1, b"second")],
            f"Mirror order mismatch after flush: {items!r}",
        )
        facade.close()

        direct = await IndexedDBer.open(db_name, stores, version=1)
        try:
            persisted = await direct.getOnItemIter("ordinal_store", b"evt")
            _require(
                persisted == [(b"evt", 0, b"first"), (b"evt", 1, b"second")],
                f"Persisted ordinal order mismatch: {persisted!r}",
            )
            log("Ordinal Suber-compatible path preserved deterministic ordered flush", "success")
        finally:
            direct.close()

        facade = await SyncIndexedDBFacade.open(db_name, stores, version=1)
        komer = CompatKomer(facade, subkey="komer_store", schema=WalletRecord)
        record = WalletRecord(aid="EA123", state="active")
        _require(komer.put(("wallet", "EA123"), record) is True, "Komer.put failed")
        _require(komer.get(("wallet", "EA123")) == record, "Komer mirror read mismatch")
        await facade.flush()
        facade.close()

        direct = await IndexedDBer.open(db_name, stores, version=1)
        try:
            raw = await direct.getVal("komer_store", b"wallet.EA123")
            _require(raw is not None, "Komer raw readback missing")
            decoded = WalletRecord(**json.loads(raw.decode("utf-8")))
            _require(decoded == record, f"Komer persisted mismatch: {decoded!r}")
            log("Komer-compatible val path preserved existing sync call shape", "success")
        finally:
            direct.close()
    finally:
        facade.close()
        try:
            await deleteDatabase(db_name)
        except Exception:
            pass


class FaultyIndexedDBBackend:
    """Backend wrapper that fails during flush to test divergence handling."""

    def __init__(self, backend: IndexedDBer):
        self.backend = backend
        self.name = backend.name
        self.db = backend.db
        self.stores = backend.stores
        self._failed = False

    def close(self) -> None:
        self.backend.close()

    async def getTopItemIter(self, *pa, **kwa):
        return await self.backend.getTopItemIter(*pa, **kwa)

    async def _fail_once(self, method: str, *pa, **kwa):
        if not self._failed:
            self._failed = True
            raise RuntimeError(
                f"forced flush failure for divergence test during {method}"
            )
        return await getattr(self.backend, method)(*pa, **kwa)

    async def putVal(self, *pa, **kwa):
        return await self._fail_once("putVal", *pa, **kwa)

    async def setVal(self, *pa, **kwa):
        return await self._fail_once("setVal", *pa, **kwa)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.backend, name)


async def _sync_facade_negative_tests(log: LogFn) -> None:
    db_name = "keriwasm-worker-hio-sync-negative"
    stores = ["suber_store"]

    try:
        await deleteDatabase(db_name)
    except Exception:
        pass

    backend = await IndexedDBer.open(db_name, stores, version=1)
    faulty = FaultyIndexedDBBackend(backend)
    facade = await SyncIndexedDBFacade.from_backend(faulty)
    try:
        suber = CompatSuber(facade, subkey="suber_store")
        _require(suber.put(("beta",), "queued") is True, "Negative probe setup failed")
        _require(facade.pending_count == 1, "Queued write missing before failed flush")
        try:
            await facade.flush()
        except MirrorDivergenceError as exc:
            log(f"Flush failure surfaced explicit divergence: {exc}", "success")
        else:
            raise _RequireError("Divergence flush unexpectedly succeeded")

        _require(facade.diverged is True, "Façade was not marked divergent")
        _require(facade.pending_count == 1, "Queued write was silently dropped after failed flush")

        try:
            suber.get(("beta",))
        except MirrorDivergenceError:
            log("Diverged façade blocks further sync calls", "success")
        else:
            raise _RequireError("Diverged façade still served sync reads")
    finally:
        facade.close()
        try:
            await deleteDatabase(db_name)
        except Exception:
            pass


async def run_all_experiments(log: LogFn) -> None:
    """Run the worker-hosted experiment plan end-to-end."""
    _log_section(log, "Worker Boot Checks")
    try:
        from js import indexedDB  # type: ignore
    except ImportError as exc:  # pragma: no cover - browser-only path
        raise _RequireError(f"Python failed to import worker indexedDB bridge: {exc}") from exc
    _require(indexedDB is not None, "indexedDB missing from worker JS global")
    log("Python imported worker indexedDB bridge", "success")

    tick = TickDoer(stop=3, tock=0.0)
    doist = await _run_doist_with([tick], tock=0.02, limit=1.0)
    _require(doist.done is True, "Doist.ado() did not complete sync doer")
    _require(len(tick.seen) == 3, f"TickDoer recurrence count mismatch: {tick.seen!r}")
    log(f"Doist.ado(real=True) executed sync doer across {tick.seen}", "success")

    _log_section(log, "Phase 1: Direct Async Backend")
    direct_doer = TaskPollingDoer(
        label="direct-async-backend",
        runner=lambda: _direct_async_backend_tests(log),
        log=log,
        tock=0.0,
    )
    await _run_doist_with([direct_doer], tock=0.02, limit=10.0)
    await _experiment_send_none_negative(log)
    await _experiment_real_false_negative(log)
    await _experiment_plain_do_negative(log)
    await _experiment_timeout_cancel_negative(log)

    _log_section(log, "Phase 2: Sync Façade Spike")
    sync_doer = TaskPollingDoer(
        label="sync-facade-positive",
        runner=lambda: _sync_facade_positive_tests(log),
        log=log,
        tock=0.0,
    )
    await _run_doist_with([sync_doer], tock=0.02, limit=10.0)
    await _sync_facade_negative_tests(log)

    _log_section(log, "Promotion Gate")
    log("Phase 1 proved raw async IndexedDB under ado(real=True)", "success")
    log("Phase 2 proved runtime CRUD call-shape compatibility for Suber/Komer probes", "success")
    log("Startup/open-path integration remains deferred by design", "info")
