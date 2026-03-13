# -*- encoding: utf-8 -*-
"""
keri.wasm.bdber module

Browser-safe plain-value DBer backed by PyScript storage.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

try:
    from pyscript import storage
except ImportError:  # pragma: no cover - browser-only runtime
    storage = None

from sortedcontainers import SortedDict  # ty:ignore[unresolved-import]


_RECORDS_KEY = "__records__"
_META_KEY = "__meta__"
_MAX_ON = int("f" * 32, 16)


@dataclass
class SubDb:
    """
    One declared browser-backed subdb.

    Attributes:
        name: Logical store name used by wrappers, for example "bags.".
        namespace: Backing storage namespace, for example "wallet:bags.".
        handle: PyScript storage handle bound to namespace.
        dupsort: Effective dupsort flag for this named store.
        flags_persisted: True once dupsort has been loaded from or flushed to
            backing storage.
        dirty: True when items differs from the last flushed payload.
        opened: True after the first env.open_db(...).
        items: Live ordered ``bytes -> bytes`` map used by sync CRUD methods.
    """

    name: str
    namespace: str
    handle: Any
    dupsort: bool = False
    flags_persisted: bool = False
    dirty: bool = False
    opened: bool = False
    items: SortedDict[bytes, bytes] = field(default_factory=SortedDict)

    def flags(self) -> dict[str, bool]:
        """Return the subdb flags used by upstream wrapper tests."""
        return {"dupsort": self.dupsort}


class BdEnv:
    """Minimal named-subdb opener used by upstream wrappers."""

    def __init__(self, owner: "Bdber"):
        self.owner = owner

    def open_db(self, key: bytes | str, dupsort: bool = False) -> SubDb:
        """
        Open a preconfigured named subdb handle.

        Parameters:
            key: Subdb name as bytes or UTF-8 text.
            dupsort: Requested duplicate flag. Applied only when the named
                store has not yet persisted its dupsort metadata.

        Returns:
            The stable `SubDb` handle for the requested store.

        Raises:
            KeyError: If the store was not declared when the DBer was opened.
        """
        name = self.owner._storify(key)
        if name not in self.owner._stores:
            raise KeyError(f"Store not configured in Bdber: {name}")
        subdb = self.owner._stores[name]
        if not subdb.opened:
            if not subdb.flags_persisted:
                subdb.dupsort = bool(dupsort)
                subdb.flags_persisted = True
                subdb.dirty = True
            subdb.opened = True
        return subdb


class Bdber:
    """
    Storage-backed plain-value DBer.

    Attributes:
        name: Base namespace prefix shared by all declared stores.
        env: Sync open_db(...) adapter used by upstream wrappers.
        _stores: Authoritative mapping of store name to SubDb.
        stores: Declared store names exposed for inspection and tests.
    """

    def __init__(self, *, name: str, stores: dict[str, SubDb]):
        self.name = name
        self.env = BdEnv(self)
        self._stores = stores
        self.stores = list(stores)

    @classmethod
    async def open(
        cls,
        name: str,
        stores: list[str],
        *,
        clear: bool = False,
        storageOpener: Callable[[str], Awaitable[Any]] | None = None,
    ) -> "Bdber":
        """
        Open a storage-backed Bdber instance with a fixed set of stores.

        Parameters:
            name: Base namespace used to derive per-store persistence names.
            stores: Declared subdb names available through `env.open_db`.
            clear: When `True`, reset all persisted store payloads before
                loading them into memory, including per-store metadata.
            storageOpener: Async callable that returns a storage handle for a
                namespace. Defaults to `pyscript.storage`.

        Returns:
            A storage-backed `Bdber` ready for sync CRUD and async `flush()`.

        Raises:
            RuntimeError: If no storage opener is available.
        """
        opener = storageOpener if storageOpener is not None else storage
        if opener is None:
            raise RuntimeError("pyscript.storage is unavailable in this environment")

        opened: dict[str, SubDb] = {}
        for store_name in [cls._storify(store) for store in stores]:
            namespace = f"{name}:{store_name}"
            handle = await opener(namespace)
            if clear:
                handle[_RECORDS_KEY] = "{}"
                handle[_META_KEY] = "{}"
                await handle.sync()
            items = SortedDict(_deserialize_records(handle.get(_RECORDS_KEY)))
            meta = _deserialize_meta(handle.get(_META_KEY))
            flags_persisted = "dupsort" in meta
            if items and not flags_persisted:
                raise ValueError(
                    "Persisted store metadata missing for non-empty store: "
                    f"{namespace}. Clear storage to recreate it."
                )
            opened[store_name] = SubDb(
                name=store_name,
                namespace=namespace,
                handle=handle,
                dupsort=bool(meta.get("dupsort", False)),
                flags_persisted=flags_persisted,
                items=items,
            )

        return cls(name=name, stores=opened)

    @staticmethod
    def _storify(key: bytes | str) -> str:
        if isinstance(key, str):
            return key
        if isinstance(key, bytes):
            return key.decode("utf-8")
        raise TypeError(f"Unsupported store handle type: {type(key)}")

    async def flush(self) -> int:
        """
        Persist dirty stores to their backing storage handles.

        Returns:
            The number of stores whose serialized payload and metadata
            were synced.
        """
        count = 0
        for subdb in self._stores.values():
            if not subdb.dirty:
                continue
            subdb.handle[_RECORDS_KEY] = _serialize_records(subdb.items)
            subdb.handle[_META_KEY] = _serialize_meta({"dupsort": subdb.dupsort})
            await subdb.handle.sync()
            subdb.dirty = False
            count += 1
        return count

    def putVal(self, db: SubDb, key: bytes, val: bytes) -> bool:
        """
        Insert `val` at `key` without overwriting an existing value.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.
            key: Exact bytes key within the subdb keyspace.
            val: Serialized bytes value to store.

        Returns:
            `True` when the value is inserted. `False` when `key` already exists.

        Raises:
            KeyError: If `key` is empty.
        """
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )

        if key in db.items:
            return False

        db.items[key] = val
        db.dirty = True
        return True

    def setVal(self, db: SubDb, key: bytes, val: bytes) -> bool:
        """
        Insert or overwrite `val` at `key`.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.
            key: Exact bytes key within the subdb keyspace.
            val: Serialized bytes value to store.

        Returns:
            `True` after the write succeeds.

        Raises:
            KeyError: If `key` is empty.
        """
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )

        db.items[key] = val
        db.dirty = True
        return True

    def getVal(self, db: SubDb, key: bytes) -> bytes | None:
        """
        Return the stored value at `key`.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.
            key: Exact bytes key within the subdb keyspace.

        Returns:
            Stored bytes value, or `None` when `key` is missing.

        Raises:
            KeyError: If `key` is empty.
        """
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )

        return db.items.get(key)

    def delVal(self, db: SubDb, key: bytes) -> bool:
        """
        Delete the exact entry at `key`.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.
            key: Exact bytes key within the subdb keyspace.

        Returns:
            `True` when an entry existed and was deleted. `False` otherwise.

        Raises:
            KeyError: If `key` is empty.
        """
        if not key:
            raise KeyError(
                f"Key: `{key}` is either empty, too big (for lmdb), "
                "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
            )

        if key not in db.items:
            return False

        del db.items[key]
        db.dirty = True
        return True

    def putOnVal(
        self,
        db: SubDb,
        key: bytes,
        on: int = 0,
        val: bytes | None = None,
        *,
        sep: bytes = b".",
    ) -> bool:
        """
        Write serialized bytes val to location at onkey consisting of
        key + sep + serialized on in db.
        Does not overwrite.

        Returns:
            result (bool): True if successful write, i.e. onkey not already
                in db. False otherwise, including when val is None.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key within the sub db
                keyspace to which the ordinal tail is added.
            on (int): ordinal number at which to write.
            val (bytes | None): serialized value to be
                written at onkey. When None returns False.
            sep (bytes): separator character for join.
        """
        if val is None:
            return False

        return self.putVal(db=db, key=_onKey(key, on, sep=sep), val=val)

    def pinOnVal(
        self,
        db: SubDb,
        key: bytes,
        on: int = 0,
        val: bytes | None = None,
        *,
        sep: bytes = b".",
    ) -> bool:
        """
        Replace value if any at location onkey = key + sep + on with val.
        Replaces pre-existing value at onkey if any or different.
        When key is empty or val is None returns False.

        Returns:
            result (bool): True if successful replacement.
                False if key is empty or val is None.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key within the sub db
                keyspace to which the ordinal tail is added.
            on (int): ordinal number at which to write.
            val (bytes | None): serialized value to be
                written at onkey. When None returns False.
            sep (bytes): separator character for split.
        """
        if val is None or not key:
            return False

        return self.setVal(db=db, key=_onKey(key, on, sep=sep), val=val)

    def appendOnVal(
        self,
        db: SubDb,
        key: bytes,
        val: bytes,
        *,
        sep: bytes = b".",
    ) -> int:
        """
        Append val in order after the last previous onkey = key + sep + on
        as a new entry at a new onkey. New on for the new onkey is one
        greater than the last prior on for the given key in db.
        The onkey of the appended entry is one greater than the last prior
        onkey for key in db.

        Returns:
            on (int): ordinal number of the new onkey for newly appended val.

        Parameters:
            db (SubDb): named browser-backed sub db whose effective keys use
                a hidden ordinal tail for insertion ordering.
            key (bytes): base key within the sub db
                keyspace. If empty raises ValueError.
            val (bytes): serialized value to append.
                If None raises ValueError.
            sep (bytes): separator character for split.

        Raises:
            ValueError: If key is empty, val is None, the next ordinal
                would exceed the maximum 32-hex-width ordinal, or the final
                insert at the computed onkey does not succeed.
        """
        if not key or val is None:
            raise ValueError(f"Bad append parameter: {key=} or {val=}")

        onkey = _onKey(key, _MAX_ON, sep=sep)
        idx = db.items.bisect_right(onkey)
        if idx:
            ponkey, _ = db.items.peekitem(idx - 1)
            ckey, cn = _splitOnKey(ponkey, sep=sep)
            if ckey == key:
                if cn >= _MAX_ON:
                    raise ValueError(f"Number part {cn=} for key part {key=}exceeds maximum size.")
                on = cn + 1
            else:
                on = 0
        else:
            on = 0

        if not self.putVal(db=db, key=_onKey(key, on, sep=sep), val=val):
            raise ValueError(f"Failed appending {val=} at {key=}.")
        return on

    def getOnItem(
        self,
        db: SubDb,
        key: bytes,
        on: int = 0,
        *,
        sep: bytes = b".",
    ) -> tuple[bytes, int, bytes] | None:
        """
        Get item `(key, on, val)` at `onkey = key + sep + on`.
        When `onkey` is missing from `db` or `key` is empty returns `None`.

        Returns:
            item (tuple[bytes, int, bytes] | None): entry item at `onkey`,
                tuple of form `(key, on, val)`. `None` if no entry at `onkey`.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key.
            on (int): ordinal number at which to retrieve.
            sep (bytes): separator character for split.
        """
        if not key:
            return None

        if not (val := self.getVal(db=db, key=_onKey(key, on, sep=sep))):
            return None

        return key, on, val

    def getOnVal(
        self,
        db: SubDb,
        key: bytes,
        on: int = 0,
        *,
        sep: bytes = b".",
    ) -> bytes | None:
        """
        Get value at `onkey = key + sep + on`.
        When `onkey` is missing from `db` or `key` is empty returns `None`.

        Returns:
            val (bytes | None): entry at `onkey = key + sep + on`.
                `None` if `onkey` is missing from `db` or `key` is empty.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key within the sub db
                keyspace to which the ordinal tail is added.
            on (int): ordinal number at which to retrieve.
            sep (bytes): separator character for split.
        """
        if not key:
            return None

        return self.getVal(db=db, key=_onKey(key, on, sep=sep))

    def remOn(
        self,
        db: SubDb,
        key: bytes,
        on: int = 0,
        *,
        sep: bytes = b".",
    ) -> bool:
        """
        Remove entry if any at `onkey = key + sep + on`.
        When `key` is missing or empty returns `False`.

        Returns:
            result (bool): `True` if entry at `onkey` was removed.
                `False` otherwise if no entry at `onkey` or `key` is empty.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key within the sub db
                keyspace to which the ordinal tail is added.
            on (int): ordinal number at which to delete.
            sep (bytes): separator character for split.
        """
        if not key:
            return False

        return self.delVal(db=db, key=_onKey(key, on, sep=sep))

    def remOnAll(
        self,
        db: SubDb,
        key: bytes = b"",
        on: int = 0,
        *,
        sep: bytes = b".",
    ) -> bool:
        """
        Remove entry at each `onkey` for all `on >= on` where for each `on`,
        `onkey = key + sep + on`.
        When `on` is `0`, default, then deletes all `on` at `key`.
        When `key` is empty then deletes whole `db`.

        Returns:
            result (bool): `True` if any entries were deleted.
                `False` otherwise.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key. When empty removes all
                entries in the whole sub db.
            on (int): ordinal number at which to add to `key` to form
                effective key. `0` means to delete all `on` at `key`.
            sep (bytes): separator character for split.

        Uses hidden ordinal key suffix for insertion ordering which is
        transparently suffixed and unsuffixed.
        """
        if not key:
            return self.delTop(db=db, top=b"")

        doomed = []
        for onkey in db.items.irange(minimum=_onKey(key, on, sep=sep)):
            ckey, _ = _splitOnKey(onkey, sep=sep)
            if ckey != key:
                break
            doomed.append(onkey)

        if not doomed:
            return False

        for onkey in doomed:
            del db.items[onkey]
        db.dirty = True
        return True

    def cntOnAll(
        self,
        db: SubDb,
        key: bytes = b"",
        on: int = 0,
        *,
        sep: bytes = b".",
    ) -> int:
        """
        Count all entries one for each `onkey` for all `on >= on` where for
        each `on`, `onkey = key + sep + on`.
        When `key` is empty then count whole sub db.

        Returns:
            count (int): count of all ordinal-keyed values with base `key` but
                different ordinal tail in `db`, starting at ordinal number `on`
                for `on >= on`. Full key is composed of `key + sep + on`.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key within the sub db
                keyspace. When empty counts whole sub db.
            on (int): ordinal number at which to initiate count.
            sep (bytes): separator character for split.
        """
        start = _onKey(key, on, sep=sep) if key else b""
        count = 0

        for onkey in db.items.irange(minimum=start):
            try:
                ckey, _ = _splitOnKey(onkey, sep=sep)
            except ValueError:
                break

            if key and ckey != key:
                break
            count += 1

        return count

    def getOnTopItemIter(
        self,
        db: SubDb,
        top: bytes = b"",
        *,
        sep: bytes = b".",
    ) -> Iterator[tuple[bytes, int, bytes]]:
        """
        Iterate over top branch of all entries where each top key startswith
        `top`.
        Assumes every effective key in `db` has trailing `on` element,
        `onkey = key + sep + on`, so can return `on` in each item.
        When top key is empty, gets all items in database.

        Returns:
            items (Iterator[tuple[bytes, int, bytes]]): iterator of triples
                `(key, on, val)`.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            top (bytes): truncated top key, a keyspace
                prefix to get all items from multiple branches of the keyspace.
                If empty gets all items in database.
            sep (bytes): separator character for split.
        """
        prefix = top

        for onkey, val in self.getTopItemIter(db=db, top=prefix):
            key, on = _splitOnKey(onkey, sep=sep)
            yield key, on, val

    def getOnAllItemIter(
        self,
        db: SubDb,
        key: bytes = b"",
        on: int = 0,
        *,
        sep: bytes = b".",
    ) -> Iterator[tuple[bytes, int, bytes]]:
        """
        Get iterator of triples `(key, on, val)`, at each key over all ordinal
        numbered keys with same `key` and `on >= on`.
        When `on = 0`, default, then iterates over all `on` at `key`.
        When `key` is empty then iterates over all `on` for all keys, whole
        db. Returned items are triples of `(key, on, val)`.

        Entries are sorted by `_onKey(key, on)` where `on` is ordinal number
        int and `key` is prefix sans `on`.

        Returns:
            items (Iterator[tuple[bytes, int, bytes]]): triples of
                `(key, on, val)` for `onkey = key + sep + on` for `on >= on`
                at `key`.

        Parameters:
            db (SubDb): named browser-backed sub db with one value per
                effective key.
            key (bytes): base key. When empty retrieves the
                whole sub db.
            on (int): ordinal number at which to initiate retrieval.
            sep (bytes): separator character for split.
        """
        yield from _iterOnItems(db=db, key=key if key else None, on=on, sep=sep)

    def getTopItemIter(self, db: SubDb, top: bytes = b"") -> Iterator[tuple[bytes, bytes]]:
        """
        Iterate over `(key, val)` pairs whose keys start with `top`.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.
            top (bytes): prefix bytes used to select a branch of the keyspace. Empty
                prefix yields the entire subdb in lexical order.

        Returns:
            Iterator of `(key, val)` tuples in lexical key order.
        """
        prefix = top

        if not prefix:
            for key, val in db.items.items():
                yield key, val
            return

        for key in db.items.irange(minimum=prefix):
            if not key.startswith(prefix):
                break
            yield key, db.items[key]

    def delTop(self, db: SubDb, top: bytes = b"") -> bool:
        """
        Delete all entries whose keys start with `top`.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.
            top (bytes): prefix bytes used to select the branch to delete. Empty prefix
                deletes the whole subdb.

        Returns:
            `True` when at least one entry is deleted. `False` when nothing
            matched the requested prefix.
        """
        prefix = top

        if not prefix:
            if not db.items:
                return False
            db.items.clear()
            db.dirty = True
            return True

        doomed = [key for key, _ in self.getTopItemIter(db=db, top=prefix)]
        if not doomed:
            return False

        for key in doomed:
            del db.items[key]
        db.dirty = True
        return True

    def cntAll(self, db: SubDb) -> int:
        """
        Count all values stored in `db`.

        Parameters:
            db: Named subdb handle returned by `env.open_db`.

        Returns:
            Total number of stored entries.
        """
        return len(db.items)


def _serialize_records(records: dict[bytes, bytes] | SortedDict[bytes, bytes]) -> str:
    return json.dumps({key.hex(): val.hex() for key, val in records.items()}, sort_keys=True)


def _serialize_meta(meta: dict[str, Any]) -> str:
    return json.dumps(meta, sort_keys=True)


def _onKey(
    key: bytes | str,
    on: int,
    *,
    sep: bytes = b".",
) -> bytes:
    """
    Returns:
        onkey (bytes): key formed by joining base `key` and hex string
            conversion of integer ordinal number `on` with separator `sep`.
            The ordinal is serialized as 32 lowercase hex characters so
            lexical ordering matches ordinal ordering.

    Parameters:
        key (bytes | str): base key prefix to be joined with the
            hex version of `on` using `sep`.
        on (int): ordinal number to be converted to 32 hex bytes.
        sep (bytes): separator character for join.
    """
    if hasattr(key, "encode"):
        key = key.encode("utf-8")
    return b"%s%s%032x" % (key, sep, on)


def _splitOnKey(
    onkey: bytes | str | memoryview,
    *,
    sep: bytes | str = b".",
) -> tuple[bytes | str, int]:
    """
    Return base `key` and integer ordinal `on` from `onkey`.
    Accepts either bytes, str, or memoryview ordinal key.
    Ordinal number appears in `onkey` in 32-char lowercase hex format.

    Parameters:
        onkey (bytes | str | memoryview): ordinal key to split.
        sep (bytes | str): separator character for split.

    Returns:
        result (tuple[bytes | str, int]): pair of base key and integer ordinal.

    Raises:
        ValueError: If `onkey` cannot be split into base key and ordinal tail
            at the provided separator.
    """
    if isinstance(onkey, memoryview):
        onkey = bytes(onkey)
    if hasattr(onkey, "encode"):
        if hasattr(sep, "decode"):
            sep = sep.decode("utf-8")
    else:
        if hasattr(sep, "encode"):
            sep = sep.encode("utf-8")
    splits = onkey.rsplit(sep, 1)
    if len(splits) != 2:
        raise ValueError(f"Unsplittable {onkey=} at {sep=}.")

    key, on = splits
    return key, int(on, 16)


def _iterOnItems(
    *,
    db: SubDb,
    key: bytes | None = None,
    on: int = 0,
    sep: bytes = b".",
) -> Iterator[tuple[bytes, int, bytes]]:
    """
    Get iterator of triples `(key, on, val)` at each base key over all ordinal
    numbered keys with same `key` and `on >= on`.
    When `on = 0`, default, then iterates over all `on` at `key`.
    When `key` is `None` then iterates over all `on` for all keys in whole db.
    Returned items are triples of `(key, on, val)`.

    Entries are sorted by `_onKey(key, on)` where `on` is ordinal number int
    and `key` is prefix sans `on`.

    Returns:
        items (Iterator[tuple[bytes, int, bytes]]): triples of `(key, on, val)`
            for `onkey = key + sep + on` for `on >= on` at `key`. When `key`
            is `None` iterates over the whole sub db.

    Parameters:
        db (SubDb): named browser-backed sub db whose effective keys are
            ordinal keys.
        key (bytes | None): base key. `None` means whole sub db.
        on (int): ordinal number at which to initiate retrieval.
        sep (bytes): separator character for split.

    Raises:
        ValueError: If an encountered effective key in `db` is not splittable
            as an ordinal key with the provided separator.
    """
    start = _onKey(key, on, sep=sep) if key else b""

    # Fixed-width ordinal suffixes make lexical order match ordinal order.
    for onkey in db.items.irange(minimum=start):
        ckey, cn = _splitOnKey(onkey, sep=sep)
        if key and ckey != key:
            break
        yield ckey, cn, db.items[onkey]


def _deserialize_records(raw: Any) -> dict[bytes, bytes]:
    if raw in (None, ""):
        return {}
    if isinstance(raw, (bytes, memoryview)):
        raw = bytes(raw).decode("utf-8")
    if isinstance(raw, str):
        payload = json.loads(raw)
    elif isinstance(raw, dict):
        payload = raw
    else:
        raise TypeError(f"Unsupported persisted record payload type: {type(raw)}")

    return {
        bytes.fromhex(str(key_hex)): bytes.fromhex(str(val_hex))
        for key_hex, val_hex in payload.items()
    }


def _deserialize_meta(raw: Any) -> dict[str, Any]:
    if raw in (None, ""):
        return {}
    if isinstance(raw, (bytes, memoryview)):
        raw = bytes(raw).decode("utf-8")
    if isinstance(raw, str):
        payload = json.loads(raw)
    elif isinstance(raw, dict):
        payload = raw
    else:
        raise TypeError(f"Unsupported persisted metadata payload type: {type(raw)}")

    if not isinstance(payload, dict):
        raise TypeError(f"Unsupported persisted metadata payload type: {type(payload)}")

    return dict(payload)
