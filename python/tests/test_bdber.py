# -*- encoding: utf-8 -*-
"""
test_bdber.py - Targeted current-surface tests for the browser-safe Bdber shim.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import logging
import re
import sys
import time
import types
from collections.abc import Awaitable, Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import bdber as bdber_module
from hio.base.doing import Doer, Doist

from bdber import (
    Bdber,
    _META_KEY,
    _RECORDS_KEY,
    _deserialize_records,
    _deserialize_meta,
    _onKey,
    _serialize_records,
    _serialize_meta,
    _splitOnKey,
    storage as pyscript_storage,
)


BAD_KEY_MESSAGE = (
    "Key: `b''` is either empty, too big (for lmdb), "
    "or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
)

TestCase = tuple[str, Callable[[], Any]]


class TestResults:
    """Track pass and failure counts for the Bdber test module."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = 0
        self.failures: list[tuple[str, str]] = []
        self.error_list: list[tuple[str, str]] = []

    def record_pass(self, name: str):
        self.passed += 1
        print(f"  PASS: {name}")

    def record_fail(self, name: str, msg: str):
        self.failed += 1
        self.failures.append((name, msg))
        print(f"  FAIL: {name}")
        print(f"    AssertionError: {msg}")

    def record_error(self, name: str, msg: str):
        self.errors += 1
        self.error_list.append((name, msg))
        print(f"  ERROR: {name}")
        print(f"    {msg}")

    def print_summary(self):
        total = self.passed + self.failed + self.errors
        print("=" * 64)
        print("TEST SUMMARY")
        print("=" * 64)
        print(f"Total:  {total}")
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        print(f"Errors: {self.errors}")

        if self.failed == 0 and self.errors == 0:
            print("-" * 64)
            print("ALL TESTS PASSED!")
            return

        if self.failures:
            print("-" * 64)
            print("FAILURES:")
            for name, msg in self.failures:
                print(f"  {name}: {msg}")

        if self.error_list:
            print("-" * 64)
            print("ERRORS:")
            for name, msg in self.error_list:
                print(f"  {name}: {msg}")


class FakeStorageHandle:
    """Async storage handle with local writes and explicit sync commit."""

    def __init__(self, backend: "FakeStorageBackend", namespace: str):
        self.backend = backend
        self.namespace = namespace
        self._local = dict(self.backend.persisted.get(namespace, {}))

    def get(self, key: str, default: Any = None) -> Any:
        return self._local.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._local[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._local[key] = value

    async def sync(self) -> None:
        self.backend.persisted[self.namespace] = dict(self._local)


class FakeStorageBackend:
    """Minimal async opener that mimics PyScript storage commit semantics."""

    def __init__(self):
        self.persisted: dict[str, dict[str, Any]] = {}

    async def open(self, namespace: str) -> FakeStorageHandle:
        return FakeStorageHandle(self, namespace)


class FlushDoer(Doer):
    """Doer that starts one flush task and polls until it completes."""

    def __init__(self, *, runner: Callable[[], Awaitable[Any]], **kwa):
        super().__init__(**kwa)
        self.runner = runner
        self.result: Any = None
        self._task: asyncio.Task | None = None

    def enter(self):
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self.runner(), name="bdber-flush")

    def recur(self, tyme):
        if self._task is None:
            raise RuntimeError("FlushDoer entered without a task")
        if not self._task.done():
            return False
        self.result = self._task.result()
        return True

    def exit(self):
        if self._task is not None and not self._task.done():
            self._task.cancel()


def _subing_source_path() -> Path:
    candidates = [
        Path(__file__).resolve().parents[1] / "keri_db_subing_source.py",
        Path(__file__).resolve().parents[3] / "keripy" / "src" / "keri" / "db" / "subing.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Unable to locate upstream subing.py from {__file__}")


def _load_upstream_subing_module():
    """Load upstream subing.py under a minimal fake package context."""
    module_name = "keri.db.subing_bdber"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing

    source_path = _subing_source_path()

    keri_mod = sys.modules.get("keri")
    if keri_mod is None:
        keri_mod = types.ModuleType("keri")
        keri_mod.__path__ = []
        sys.modules["keri"] = keri_mod

    db_pkg = types.ModuleType("keri.db")
    db_pkg.__path__ = []
    sys.modules["keri.db"] = db_pkg
    sys.modules["keri"].db = db_pkg

    help_mod = types.ModuleType("keri.help")

    class _Ogler:
        def getLogger(self, name: str | None = None):
            return logging.getLogger(name or "keri")

    help_mod.ogler = _Ogler()
    help_mod.__path__ = []
    sys.modules["keri.help"] = help_mod
    sys.modules["keri"].help = help_mod

    helping_mod = types.ModuleType("keri.help.helping")
    helping_mod.isNonStringIterable = (
        lambda obj: not isinstance(obj, (str, bytes)) and isinstance(obj, Iterable)
    )
    helping_mod.Reb64 = re.compile(br"^[A-Za-z0-9_-]*$")
    sys.modules["keri.help.helping"] = helping_mod
    sys.modules["keri.help"].helping = helping_mod

    dbing_mod = types.ModuleType("keri.db.dbing")
    dbing_mod.LMDBer = Bdber
    sys.modules["keri.db.dbing"] = dbing_mod
    sys.modules["keri.db"].dbing = dbing_mod

    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to build import spec for {source_path}")

    module = importlib.util.module_from_spec(spec)
    module.__package__ = "keri.db"
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


async def _open_fake_dber(
    *,
    name: str = "test-bdber",
    stores: list[str] | None = None,
    clear: bool = False,
    backend: FakeStorageBackend | None = None,
) -> tuple[Bdber, FakeStorageBackend]:
    if backend is None:
        backend = FakeStorageBackend()
    dber = await Bdber.open(
        name=name,
        stores=stores or ["bags.", "docs.", "beep.", "pugs."],
        clear=clear,
        storageOpener=backend.open,
    )
    return dber, backend


def _assert_type_error(fn: Callable[[], Any], expected: str) -> None:
    try:
        fn()
    except TypeError as ex:
        assert ex.args[0] == expected
    else:
        raise AssertionError(f"Expected TypeError: {expected}")


def _print_section(title: str) -> None:
    print(title)
    print("-" * len(title))


async def _run_named_tests(
    title: str,
    tests: Sequence[TestCase],
    results: TestResults,
) -> None:
    print()
    _print_section(title)
    for name, fn in tests:
        try:
            result = fn()
            if inspect.isawaitable(result):
                await result
        except AssertionError as ex:
            results.record_fail(name, str(ex))
        except Exception as ex:
            results.record_error(name, f"{type(ex).__name__}: {ex}")
        else:
            results.record_pass(name)


async def test_open_declares_stores_and_clear_resets_persisted_state():
    """Test open() store declaration, persisted reload, and clear reset."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(
        name="open-clear",
        stores=["bags.", "docs."],
        clear=True,
        backend=backend,
    )
    assert dber.name == "open-clear"
    assert dber.stores == ["bags.", "docs."]

    bags = dber.env.open_db(b"bags.")
    docs = dber.env.open_db("docs.")
    assert bags.namespace == "open-clear:bags."
    assert docs.namespace == "open-clear:docs."
    assert dber.cntAll(bags) == 0
    assert dber.cntAll(docs) == 0
    assert await dber.flush() == 2

    assert dber.setVal(docs, b"alpha", b"one") is True
    assert await dber.flush() == 1

    reopened, _ = await _open_fake_dber(
        name="open-clear",
        stores=["bags.", "docs."],
        backend=backend,
    )
    docs_reopened = reopened.env.open_db("docs.")
    assert reopened.getVal(docs_reopened, b"alpha") == b"one"

    cleared, _ = await _open_fake_dber(
        name="open-clear",
        stores=["bags.", "docs."],
        clear=True,
        backend=backend,
    )
    docs_cleared = cleared.env.open_db("docs.")
    assert cleared.getVal(docs_cleared, b"alpha") is None
    assert cleared.cntAll(docs_cleared) == 0


async def test_open_requires_storage_backend():
    """Test open() fails without an explicit or ambient storage backend."""
    original = bdber_module.storage
    bdber_module.storage = None
    try:
        try:
            await Bdber.open(name="missing-storage", stores=["docs."])
        except RuntimeError as ex:
            assert ex.args[0] == "pyscript.storage is unavailable in this environment"
        else:
            raise AssertionError("Bdber.open should fail without a storage backend")
    finally:
        bdber_module.storage = original


async def test_open_db_flag_persistence():
    """Test dupsort latching is stable in-process and persists across reopen."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(stores=["bags.", "docs."], clear=True, backend=backend)

    bags = dber.env.open_db(b"bags.", dupsort=False)
    assert bags.flags()["dupsort"] is False
    assert bags.dirty is True

    same = dber.env.open_db("bags.", dupsort=True)
    assert same is bags
    assert same.flags()["dupsort"] is False

    docs = dber.env.open_db("docs.", dupsort=True)
    assert docs.flags()["dupsort"] is True
    assert await dber.flush() == 2

    reopened, _ = await _open_fake_dber(stores=["bags.", "docs."], backend=backend)
    bags_reopened = reopened.env.open_db("bags.", dupsort=True)
    docs_reopened = reopened.env.open_db("docs.", dupsort=False)
    assert bags_reopened.flags()["dupsort"] is False
    assert docs_reopened.flags()["dupsort"] is True


async def test_open_db_metadata_only_flush_persists_dupsort():
    """Test first-open dupsort metadata flushes even before any record writes."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(name="meta-only", stores=["docs."], clear=True, backend=backend)

    docs = dber.env.open_db("docs.", dupsort=True)
    assert docs.flags()["dupsort"] is True
    assert docs.dirty is True
    assert await dber.flush() == 1
    assert docs.dirty is False
    assert _deserialize_meta(backend.persisted["meta-only:docs."][_META_KEY]) == {"dupsort": True}
    assert _deserialize_records(backend.persisted["meta-only:docs."][_RECORDS_KEY]) == {}

    reopened, _ = await _open_fake_dber(name="meta-only", stores=["docs."], backend=backend)
    docs_reopened = reopened.env.open_db("docs.", dupsort=False)
    assert docs_reopened.flags()["dupsort"] is True


async def test_clear_resets_persisted_store_metadata():
    """Test clear=True drops persisted dupsort metadata and allows relatching."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(name="clear-meta", stores=["docs."], clear=True, backend=backend)

    docs = dber.env.open_db("docs.", dupsort=True)
    assert await dber.flush() == 1
    assert _deserialize_meta(backend.persisted["clear-meta:docs."][_META_KEY]) == {"dupsort": True}

    cleared, _ = await _open_fake_dber(name="clear-meta", stores=["docs."], clear=True, backend=backend)
    docs_cleared = cleared.env.open_db("docs.", dupsort=False)
    assert docs_cleared.flags()["dupsort"] is False
    assert await cleared.flush() == 1

    reopened, _ = await _open_fake_dber(name="clear-meta", stores=["docs."], backend=backend)
    docs_reopened = reopened.env.open_db("docs.", dupsort=True)
    assert docs_reopened.flags()["dupsort"] is False


async def test_open_rejects_non_empty_records_without_store_metadata():
    """Test open() fails closed on legacy non-empty stores missing flag metadata."""
    backend = FakeStorageBackend()
    backend.persisted["legacy:docs."] = {
        _RECORDS_KEY: _serialize_records({b"alpha": b"one"}),
    }

    try:
        await Bdber.open(
            name="legacy",
            stores=["docs."],
            storageOpener=backend.open,
        )
    except ValueError as ex:
        assert ex.args[0] == (
            "Persisted store metadata missing for non-empty store: "
            "legacy:docs.. Clear storage to recreate it."
        )
    else:
        raise AssertionError("Bdber.open should reject non-empty stores missing metadata")


async def test_open_db_rejects_unconfigured_store():
    """Test open_db rejects stores not declared at open time."""
    dber, _ = await _open_fake_dber(stores=["bags."], clear=True)

    try:
        dber.env.open_db("docs.")
    except KeyError as ex:
        assert ex.args[0] == "Store not configured in Bdber: docs."
    else:
        raise AssertionError("open_db should reject undeclared stores")


async def test_storify_supported_inputs():
    """Test Bdber store-handle normalization accepts configured input types."""
    assert Bdber._storify("docs.") == "docs."
    assert Bdber._storify(b"bags.") == "bags."


async def test_storify_reject_invalid_types():
    """Test Bdber store-handle normalization rejects unsupported inputs."""
    _assert_type_error(lambda: Bdber._storify(lambda: None), "Unsupported store handle type: <class 'function'>")


async def test_serialize_deserialize_edge_cases():
    """Test record and metadata serialization helpers handle supported payload forms."""
    records = {
        b"b": b"\x02",
        b"a": b"\x01",
    }
    serialized = _serialize_records(records)
    assert serialized == '{"61": "01", "62": "02"}'
    meta = {"dupsort": True}
    serialized_meta = _serialize_meta(meta)
    assert serialized_meta == '{"dupsort": true}'

    assert _deserialize_records(None) == {}
    assert _deserialize_records("") == {}
    assert _deserialize_records(serialized) == {b"a": b"\x01", b"b": b"\x02"}
    assert _deserialize_records(serialized.encode("utf-8")) == {b"a": b"\x01", b"b": b"\x02"}
    assert _deserialize_records(memoryview(serialized.encode("utf-8"))) == {
        b"a": b"\x01",
        b"b": b"\x02",
    }
    assert _deserialize_records({"61": "31"}) == {b"a": b"1"}
    assert _deserialize_meta(None) == {}
    assert _deserialize_meta("") == {}
    assert _deserialize_meta(serialized_meta) == meta
    assert _deserialize_meta(serialized_meta.encode("utf-8")) == meta
    assert _deserialize_meta(memoryview(serialized_meta.encode("utf-8"))) == meta
    assert _deserialize_meta(meta) == meta

    try:
        _deserialize_records(42)
    except TypeError as ex:
        assert ex.args[0] == "Unsupported persisted record payload type: <class 'int'>"
    else:
        raise AssertionError("_deserialize_records should reject unsupported payload types")

    try:
        _deserialize_meta(42)
    except TypeError as ex:
        assert ex.args[0] == "Unsupported persisted metadata payload type: <class 'int'>"
    else:
        raise AssertionError("_deserialize_meta should reject unsupported payload types")


async def test_val_crud_and_dirty_noop_semantics():
    """Test current Val CRUD semantics and dirty flags."""
    dber, _ = await _open_fake_dber(stores=["docs."], clear=True)
    docs = dber.env.open_db("docs.")

    assert docs.dirty is True
    assert await dber.flush() == 1
    assert docs.dirty is False
    assert dber.getVal(docs, b"alpha") is None
    assert docs.dirty is False
    assert dber.delVal(docs, b"alpha") is False
    assert docs.dirty is False

    assert dber.putVal(docs, b"alpha", b"one") is True
    assert docs.dirty is True
    assert dber.getVal(docs, b"alpha") == b"one"
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.putVal(docs, b"alpha", b"shadow") is False
    assert docs.dirty is False

    assert dber.setVal(docs, b"\xff\x00", b"\x01\x02") is True
    assert docs.dirty is True
    assert dber.getVal(docs, b"\xff\x00") == b"\x01\x02"
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.setVal(docs, b"alpha", b"two") is True
    assert docs.dirty is True
    assert dber.getVal(docs, b"alpha") == b"two"
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.delVal(docs, b"alpha") is True
    assert docs.dirty is True
    assert dber.getVal(docs, b"alpha") is None
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.delVal(docs, b"alpha") is False
    assert docs.dirty is False


async def test_empty_key_errors():
    """Test LMDB-compatible empty-key validation."""
    dber, _ = await _open_fake_dber(stores=["docs."], clear=True)
    docs = dber.env.open_db("docs.")

    for opname, fn in (
        ("putVal", lambda: dber.putVal(docs, b"", b"val")),
        ("setVal", lambda: dber.setVal(docs, b"", b"val")),
        ("getVal", lambda: dber.getVal(docs, b"")),
        ("delVal", lambda: dber.delVal(docs, b"")),
    ):
        try:
            fn()
        except KeyError as ex:
            assert ex.args[0] == BAD_KEY_MESSAGE, f"{opname} raised {ex.args[0]!r}"
        else:
            raise AssertionError(f"{opname} should raise KeyError for empty key")


async def test_prefix_iteration_and_count():
    """Test lexical prefix iteration and whole-store count."""
    dber, _ = await _open_fake_dber(stores=["docs."], clear=True)
    docs = dber.env.open_db("docs.")

    assert dber.setVal(docs, b"a.1", b"blue") is True
    assert dber.setVal(docs, b"a.2", b"green") is True
    assert dber.setVal(docs, b"ac.4", b"white") is True
    assert dber.setVal(docs, b"b.1", b"red") is True
    assert dber.setVal(docs, b"bc.3", b"black") is True

    assert list(dber.getTopItemIter(docs)) == [
        (b"a.1", b"blue"),
        (b"a.2", b"green"),
        (b"ac.4", b"white"),
        (b"b.1", b"red"),
        (b"bc.3", b"black"),
    ]
    assert list(dber.getTopItemIter(docs, b"a.")) == [
        (b"a.1", b"blue"),
        (b"a.2", b"green"),
    ]
    assert list(dber.getTopItemIter(docs, b"ac")) == [
        (b"ac.4", b"white"),
    ]
    assert list(dber.getTopItemIter(docs, b"z")) == []
    assert dber.cntAll(docs) == 5


async def test_delTop_prefix_and_whole_store_behavior():
    """Test prefix and whole-store deletion semantics and dirty transitions."""
    dber, _ = await _open_fake_dber(stores=["docs."], clear=True)
    docs = dber.env.open_db("docs.")

    assert dber.setVal(docs, b"a.1", b"blue") is True
    assert dber.setVal(docs, b"a.2", b"green") is True
    assert dber.setVal(docs, b"b.1", b"red") is True
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.delTop(docs, b"z.") is False
    assert docs.dirty is False

    assert dber.delTop(docs, b"a.") is True
    assert docs.dirty is True
    assert list(dber.getTopItemIter(docs)) == [(b"b.1", b"red")]
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.delTop(docs) is True
    assert docs.dirty is True
    assert list(dber.getTopItemIter(docs)) == []
    assert await dber.flush() == 1
    assert docs.dirty is False

    assert dber.delTop(docs) is False
    assert docs.dirty is False


async def test_flush_persistence_semantics():
    """Test unsynced vs synced reopen visibility across fake storage handles."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(name="flush-semantics", stores=["docs."], clear=True, backend=backend)
    docs = dber.env.open_db("docs.")

    assert await dber.flush() == 1
    assert await dber.flush() == 0

    assert dber.setVal(docs, b"alpha", b"one") is True

    reopened_before, _ = await _open_fake_dber(
        name="flush-semantics",
        stores=["docs."],
        backend=backend,
    )
    docs_before = reopened_before.env.open_db("docs.")
    assert reopened_before.getVal(docs_before, b"alpha") is None

    assert await dber.flush() == 1
    assert await dber.flush() == 0

    assert dber.setVal(docs, b"alpha", b"two") is True
    reopened_unsynced, _ = await _open_fake_dber(
        name="flush-semantics",
        stores=["docs."],
        backend=backend,
    )
    docs_unsynced = reopened_unsynced.env.open_db("docs.")
    assert reopened_unsynced.getVal(docs_unsynced, b"alpha") == b"one"

    assert await dber.flush() == 1
    reopened_after, _ = await _open_fake_dber(
        name="flush-semantics",
        stores=["docs."],
        backend=backend,
    )
    docs_after = reopened_after.env.open_db("docs.")
    assert reopened_after.getVal(docs_after, b"alpha") == b"two"


async def test_flush_counts_only_dirty_stores():
    """Test flush counts only dirty stores and ignores no-op operations."""
    dber, _ = await _open_fake_dber(stores=["bags.", "docs.", "pugs."], clear=True)
    bags = dber.env.open_db("bags.")
    docs = dber.env.open_db("docs.")
    pugs = dber.env.open_db("pugs.")
    assert await dber.flush() == 3

    assert dber.setVal(bags, b"bag.1", b"blue") is True
    assert dber.setVal(docs, b"doc.1", b"green") is True
    assert await dber.flush() == 2

    assert dber.putVal(docs, b"doc.1", b"shadow") is False
    assert dber.delVal(pugs, b"missing") is False
    assert dber.getVal(bags, b"bag.1") == b"blue"
    assert await dber.flush() == 0

    assert dber.setVal(docs, b"doc.2", b"white") is True
    assert await dber.flush() == 1
    assert await dber.flush() == 0


async def test_flush_with_hio_ado():
    """Test flush completion under hio Doist.ado scheduling when dirty."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(name="ado-flush", stores=["docs."], clear=True, backend=backend)
    docs = dber.env.open_db("docs.")
    assert dber.setVal(docs, b"alpha", b"one") is True

    doer = FlushDoer(runner=dber.flush, tock=0.0)
    doist = Doist(real=True, doers=[doer], tock=0.01, limit=1.0)
    await doist.ado()

    assert doist.done is True
    assert doer.result == 1

    reopened = await Bdber.open(name="ado-flush", stores=["docs."], storageOpener=backend.open)
    docs_reopened = reopened.env.open_db("docs.")
    assert reopened.getVal(docs_reopened, b"alpha") == b"one"


async def test_flush_with_hio_ado_when_clean():
    """Test hio Doist.ado flush completion when no stores are dirty."""
    dber, _ = await _open_fake_dber(name="ado-flush-clean", stores=["docs."], clear=True)

    doer = FlushDoer(runner=dber.flush, tock=0.0)
    doist = Doist(real=True, doers=[doer], tock=0.01, limit=1.0)
    await doist.ado()

    assert doist.done is True
    assert doer.result == 0


async def test_browser_storage_flush_integration():
    """Test flush persistence against real PyScript storage when available."""
    if pyscript_storage is None:
        raise RuntimeError("pyscript.storage is unavailable")

    name = f"test-bdber-browser-{time.time_ns()}"
    stores = ["bags.", "docs."]
    dber = await Bdber.open(name=name, stores=stores, clear=True)

    bags = dber.env.open_db("bags.")
    docs = dber.env.open_db("docs.")
    assert dber.setVal(bags, b"bag.1", b"blue") is True
    assert dber.setVal(docs, b"doc.1", b"green") is True

    assert await dber.flush() == 2

    reopened = await Bdber.open(name=name, stores=stores)
    bags_reopened = reopened.env.open_db("bags.")
    docs_reopened = reopened.env.open_db("docs.")
    assert reopened.getVal(bags_reopened, b"bag.1") == b"blue"
    assert reopened.getVal(docs_reopened, b"doc.1") == b"green"


async def test_keripy_ordinal_key_helpers():
    """Adapted from keripy/tests/db/test_dbing.py::test_key_funcs for ordinal helpers."""
    pre = b"BAzwEHHzq7K0gzQPYGGwTmuupUhPx5_yZ-Wk1x4ejhcc"

    assert _onKey(pre, 0) == pre + b"." + b"%032x" % 0
    assert _onKey(pre, 1) == pre + b"." + b"%032x" % 1
    assert _onKey(pre, 15) == pre + b"." + b"%032x" % 15

    assert _onKey(pre, 0, sep=b"|") == pre + b"|" + b"%032x" % 0
    assert _onKey(pre, 4, sep=b"|") == pre + b"|" + b"%032x" % 4

    onkey = _onKey(pre, 0)
    assert _splitOnKey(onkey) == (pre, 0)
    onkey = _onKey(pre, 1)
    assert _splitOnKey(onkey) == (pre, 1)
    onkey = _onKey(pre, 15)
    assert _splitOnKey(onkey) == (pre, 15)

    onkey = _onKey(pre, 0, sep=b"|")
    assert _splitOnKey(onkey, sep=b"|") == (pre, 0)
    onkey = _onKey(pre, 15, sep=b"|")
    assert _splitOnKey(onkey, sep=b"|") == (pre, 15)

    pre = "BAzwEHHzq7K0gzQPYGGwTmuupUhPx5_yZ-Wk1x4ejhcc"

    assert _onKey(pre, 0) == b"BAzwEHHzq7K0gzQPYGGwTmuupUhPx5_yZ-Wk1x4ejhcc.00000000000000000000000000000000"
    assert _onKey(pre, 15, sep=b"|") == b"BAzwEHHzq7K0gzQPYGGwTmuupUhPx5_yZ-Wk1x4ejhcc|0000000000000000000000000000000f"

    onkey = _onKey(pre, 0).decode("utf-8")
    assert _splitOnKey(onkey) == (pre, 0)
    onkey = _onKey(pre, 15).decode("utf-8")
    assert _splitOnKey(onkey) == (pre, 15)
    onkey = _onKey(pre, 0, sep=b"|").decode("utf-8")
    assert _splitOnKey(onkey, sep=b"|") == (pre, 0)
    onkey = _onKey(pre, 15, sep=b"|").decode("utf-8")
    assert _splitOnKey(onkey, sep=b"|") == (pre, 15)

    pre = b"BAzwEHHzq7K0gzQPYGGwTmuupUhPx5_yZ-Wk1x4ejhcc"
    onkey = memoryview(_onKey(pre, 15))
    assert _splitOnKey(onkey) == (pre, 15)
    onkey = memoryview(_onKey(pre, 15, sep=b"|"))
    assert _splitOnKey(onkey, sep=b"|") == (pre, 15)


async def test_keripy_lmdber_get_on_item_empty_value_contract():
    """Adapted from keripy LMDBer truthy-empty getOnItem behavior."""
    dber, _ = await _open_fake_dber(stores=["seen."], clear=True)
    sdb = dber.env.open_db(key=b"seen.")

    pre = b"BBKY1sKmgyjAiUDdUBPNPyrSz_ad_Qf9yzhDNZlEKiMc"
    assert dber.putOnVal(sdb, pre, 0, val=b"") is True
    assert dber.getOnVal(sdb, pre, 0) == b""
    assert dber.getOnItem(sdb, pre, 0) is None


async def test_keripy_lmdber_on_val_contract():
    """Adapted from keripy/tests/db/test_dbing.py ordinal CRUD and remove block."""
    dber, _ = await _open_fake_dber(stores=["seen."], clear=True)
    sdb = dber.env.open_db(key=b"seen.")

    preA = b"BBKY1sKmgyjAiUDdUBPNPyrSz_ad_Qf9yzhDNZlEKiMc"
    preB = b"EH7Oq9oxCgYa-nnNLvwhp9sFZpALILlRYyB-6n4WDi7w"

    keyA0 = _onKey(preA, 0)

    digA = b"EA73b7reENuBahMJsMTLbeyyNPsfTRzKRWtJ3ytmInvw"
    digU = b"EB73b7reENuBahMJsMTLbeyyNPsfTRzKRWtJ3ytmInvw"
    digV = b"EC4vCeJswIBJlO3RqE-wsE72Vt3wAceJ_LzqKvbDtBSY"
    digW = b"EDAyl33W9ja_wLX85UrzRnL4KNzlsIKIA7CrD04nVX1w"
    digX = b"EEnwxEm5Bg5s5aTLsgQCNpubIYzwlvMwZIzdOM0Z3u7o"
    digY = b"EFrq74_Q11S2vHx1gpK_46Ik5Q7Yy9K1zZ5BavqGDKnk"
    digC = b"EG5RimdY_OWoreR-Z-Q5G81-I4tjASJCaP_MqkBbtM2w"

    assert dber.getVal(sdb, keyA0) is None
    assert dber.delVal(sdb, keyA0) is False
    assert dber.putVal(sdb, keyA0, val=digA) is True
    assert dber.getVal(sdb, keyA0) == digA
    assert dber.putVal(sdb, keyA0, val=digA) is False
    assert dber.setVal(sdb, keyA0, val=digA) is True
    assert dber.getVal(sdb, keyA0) == digA
    assert dber.getOnVal(sdb, preA, 0) == digA
    assert dber.delVal(sdb, keyA0) is True
    assert dber.getVal(sdb, keyA0) is None
    assert dber.getOnVal(sdb, preA, 0) is None

    assert dber.putOnVal(sdb, preA, 0, val=digA) is True
    assert dber.getOnVal(sdb, preA, 0) == digA
    assert dber.getOnItem(sdb, preA, 0) == (preA, 0, digA)
    assert dber.putOnVal(sdb, preA, 0, val=digA) is False
    assert dber.pinOnVal(sdb, preA, 0, val=digA) is True
    assert dber.getOnVal(sdb, preA, 0) == digA
    assert dber.getOnItem(sdb, preA, 0) == (preA, 0, digA)
    assert dber.remOn(sdb, preA, 0) is True
    assert dber.getOnVal(sdb, preA, 0) is None
    assert dber.getOnItem(sdb, preA, 0) is None

    assert dber.putOnVal(sdb, preA, 0, val=digA) is True
    assert dber.putOnVal(sdb, preA, 1, val=digC) is True
    assert dber.putOnVal(sdb, preA, 2, val=digU) is True
    assert dber.putOnVal(sdb, preA, 3, val=digV) is True
    assert dber.putOnVal(sdb, preA, 4, val=digW) is True
    assert dber.putOnVal(sdb, preB, 0, val=digX) is True
    assert dber.putOnVal(sdb, preB, 1, val=digY) is True

    assert dber.cntOnAll(sdb, preA) == 5
    assert dber.cntOnAll(sdb, preB) == 2
    assert dber.cntOnAll(sdb) == 7

    assert dber.remOnAll(sdb, preA, on=3) is True
    assert dber.cntOnAll(sdb, preA) == 3
    assert dber.cntOnAll(sdb, preB) == 2

    assert dber.remOnAll(sdb, preA) is True
    assert dber.cntOnAll(sdb, preA) == 0
    assert dber.cntOnAll(sdb, preB) == 2

    assert dber.remOnAll(sdb) is True
    assert dber.cntOnAll(sdb) == 0


async def test_keripy_lmdber_append_on_iter_contract():
    """Adapted from keripy/tests/db/test_dbing.py append and iterator block."""
    dber, _ = await _open_fake_dber(stores=["seen."], clear=True)
    sdb = dber.env.open_db(key=b"seen.")

    preA = b"BBKY1sKmgyjAiUDdUBPNPyrSz_ad_Qf9yzhDNZlEKiMc"
    preB = b"EH7Oq9oxCgYa-nnNLvwhp9sFZpALILlRYyB-6n4WDi7w"
    preC = b"EIDA1n-WiBA0A8YOqnKrB-wWQYYC49i5zY_qrIZIicQg"
    preD = b"EAYC49i5zY_qrIZIicQgIDA1n-WiBA0A8YOqnKrB-wWQ"

    keyA0 = _onKey(preA, 0)
    keyB0 = _onKey(preB, 0)
    keyB1 = _onKey(preB, 1)
    keyB2 = _onKey(preB, 2)
    keyB3 = _onKey(preB, 3)
    keyB4 = _onKey(preB, 4)
    keyC0 = _onKey(preC, 0)

    digA = b"EA73b7reENuBahMJsMTLbeyyNPsfTRzKRWtJ3ytmInvw"
    digU = b"EB73b7reENuBahMJsMTLbeyyNPsfTRzKRWtJ3ytmInvw"
    digV = b"EC4vCeJswIBJlO3RqE-wsE72Vt3wAceJ_LzqKvbDtBSY"
    digW = b"EDAyl33W9ja_wLX85UrzRnL4KNzlsIKIA7CrD04nVX1w"
    digX = b"EEnwxEm5Bg5s5aTLsgQCNpubIYzwlvMwZIzdOM0Z3u7o"
    digY = b"EFrq74_Q11S2vHx1gpK_46Ik5Q7Yy9K1zZ5BavqGDKnk"
    digC = b"EG5RimdY_OWoreR-Z-Q5G81-I4tjASJCaP_MqkBbtM2w"

    assert dber.getVal(sdb, keyB0) is None
    assert dber.appendOnVal(sdb, preB, digU) == 0
    assert dber.getVal(sdb, keyB0) == digU
    assert dber.delVal(sdb, keyB0) is True
    assert dber.getVal(sdb, keyB0) is None

    assert dber.putVal(sdb, keyA0, val=digA) is True
    assert dber.appendOnVal(sdb, preB, digU) == 0
    assert dber.getVal(sdb, keyB0) == digU
    assert dber.delVal(sdb, keyB0) is True
    assert dber.getVal(sdb, keyB0) is None

    assert dber.getVal(sdb, keyA0) == digA
    assert dber.putVal(sdb, keyC0, val=digC) is True
    assert dber.appendOnVal(sdb, preB, digU) == 0
    assert dber.getVal(sdb, keyB0) == digU
    assert dber.delVal(sdb, keyB0) is True
    assert dber.getVal(sdb, keyB0) is None

    assert dber.delVal(sdb, keyA0) is True
    assert dber.getVal(sdb, keyA0) is None
    assert dber.getVal(sdb, keyC0) == digC
    assert dber.appendOnVal(sdb, preB, digU) == 0
    assert dber.getVal(sdb, keyB0) == digU

    assert dber.putVal(sdb, keyA0, val=digA) is True
    assert dber.appendOnVal(sdb, preB, digV) == 1
    assert dber.getVal(sdb, keyB1) == digV

    assert dber.delVal(sdb, keyA0) is True
    assert dber.getVal(sdb, keyA0) is None
    assert dber.delVal(sdb, keyC0) is True
    assert dber.getVal(sdb, keyC0) is None
    assert dber.appendOnVal(sdb, preB, digW) == 2
    assert dber.getVal(sdb, keyB2) == digW
    assert dber.appendOnVal(sdb, preB, digX) == 3
    assert dber.getVal(sdb, keyB3) == digX
    assert dber.appendOnVal(sdb, preB, digY) == 4
    assert dber.getVal(sdb, keyB4) == digY

    assert dber.appendOnVal(sdb, preD, digY) == 0

    assert dber.cntOnAll(sdb, key=preB) == 5
    assert dber.cntOnAll(sdb, key=b"") == 6
    assert dber.cntOnAll(sdb) == 6
    assert dber.cntAll(sdb) == 6

    assert list(dber.getOnAllItemIter(sdb, preB)) == [
        (preB, 0, digU),
        (preB, 1, digV),
        (preB, 2, digW),
        (preB, 3, digX),
        (preB, 4, digY),
    ]
    assert list(dber.getOnAllItemIter(sdb, preB, on=3)) == [
        (preB, 3, digX),
        (preB, 4, digY),
    ]
    assert list(dber.getOnAllItemIter(sdb, preB, on=5)) == []

    assert dber.putVal(sdb, keyA0, val=digA) is True
    assert dber.putVal(sdb, keyC0, val=digC) is True

    assert list(dber.getOnTopItemIter(sdb, top=preB)) == [
        (preB, 0, digU),
        (preB, 1, digV),
        (preB, 2, digW),
        (preB, 3, digX),
        (preB, 4, digY),
    ]
    assert list(dber.getOnTopItemIter(sdb)) == [
        (preA, 0, digA),
        (preD, 0, digY),
        (preB, 0, digU),
        (preB, 1, digV),
        (preB, 2, digW),
        (preB, 3, digX),
        (preB, 4, digY),
        (preC, 0, digC),
    ]
    assert list(dber.getOnAllItemIter(sdb, key=b"")) == [
        (preA, 0, digA),
        (preD, 0, digY),
        (preB, 0, digU),
        (preB, 1, digV),
        (preB, 2, digW),
        (preB, 3, digX),
        (preB, 4, digY),
        (preC, 0, digC),
    ]
    assert list(dber.getOnAllItemIter(sdb)) == [
        (preA, 0, digA),
        (preD, 0, digY),
        (preB, 0, digU),
        (preB, 1, digV),
        (preB, 2, digW),
        (preB, 3, digX),
        (preB, 4, digY),
        (preC, 0, digC),
    ]

    top, on = _splitOnKey(keyB2)
    assert list(dber.getOnAllItemIter(sdb, key=top, on=on)) == [
        (top, 2, digW),
        (top, 3, digX),
        (top, 4, digY),
    ]
    assert list(dber.getOnAllItemIter(sdb, key=preC, on=1)) == []

    assert dber.remOn(sdb, key=preB) is True
    assert dber.remOn(sdb, key=preB, on=0) is False
    assert dber.remOn(sdb, key=preB, on=1) is True
    assert dber.remOn(sdb, key=preB, on=1) is False
    assert list(dber.getOnAllItemIter(sdb, key=preB)) == [
        (top, 2, digW),
        (top, 3, digX),
        (top, 4, digY),
    ]
    assert dber.remOn(sdb, key=b"") is False


async def test_on_val_dirty_flush_and_persistence_semantics():
    """Test dirty flags, no-op writes, and reopen persistence for ordinal methods."""
    backend = FakeStorageBackend()
    dber, _ = await _open_fake_dber(
        name="on-flush",
        stores=["ords."],
        clear=True,
        backend=backend,
    )
    ords = dber.env.open_db("ords.")

    assert await dber.flush() == 1
    assert await dber.flush() == 0

    assert dber.putOnVal(ords, b"evt", 0, b"icp") is True
    assert ords.dirty is True
    assert await dber.flush() == 1
    assert ords.dirty is False

    assert dber.putOnVal(ords, b"evt", 0, b"dup") is False
    assert ords.dirty is False

    assert dber.pinOnVal(ords, b"evt", 0, b"rot") is True
    assert ords.dirty is True
    assert await dber.flush() == 1
    assert ords.dirty is False

    assert dber.remOn(ords, b"evt", 1) is False
    assert ords.dirty is False

    assert dber.appendOnVal(ords, b"evt", b"ixn") == 1
    assert ords.dirty is True
    assert await dber.flush() == 1
    assert ords.dirty is False

    reopened, _ = await _open_fake_dber(
        name="on-flush",
        stores=["ords."],
        backend=backend,
    )
    ords_reopened = reopened.env.open_db("ords.")
    assert reopened.getOnVal(ords_reopened, b"evt", 0) == b"rot"
    assert reopened.getOnVal(ords_reopened, b"evt", 1) == b"ixn"
    assert list(reopened.getOnAllItemIter(ords_reopened, b"evt")) == [
        (b"evt", 0, b"rot"),
        (b"evt", 1, b"ixn"),
    ]


async def test_on_val_and_empty_key_behavior():
    """Test ON semantics and exact empty-key ordinal behavior."""
    dber, _ = await _open_fake_dber(stores=["ords."], clear=True)
    ords = dber.env.open_db("ords.")

    assert dber.putOnVal(ords, b"evt", 0, b"icp") is True
    assert dber.putOnVal(ords, b"\xff", 1, b"\x01\x02") is True
    assert dber.getOnVal(ords, b"evt", 0) == b"icp"
    assert dber.getOnVal(ords, b"\xff", 1) == b"\x01\x02"
    assert dber.getOnItem(ords, b"\xff", 1) == (b"\xff", 1, b"\x01\x02")
    assert list(dber.getOnTopItemIter(ords, b"\xff")) == [
        (b"\xff", 1, b"\x01\x02"),
    ]

    assert dber.putOnVal(ords, b"", 0, b"root") is True
    assert dber.getVal(ords, _onKey(b"", 0)) == b"root"
    assert dber.getOnVal(ords, b"", 0) is None
    assert dber.getOnItem(ords, b"", 0) is None
    assert dber.pinOnVal(ords, b"", 0, b"shadow") is False
    assert dber.remOn(ords, b"", 0) is False

    try:
        dber.appendOnVal(ords, b"", b"bad")
    except ValueError as ex:
        assert ex.args[0] == "Bad append parameter: key=b'' or val=b'bad'"
    else:
        raise AssertionError("appendOnVal should reject empty ordinal key")

    assert dber.remOnAll(ords, b"", 0) is True
    assert dber.cntAll(ords) == 0
    assert dber.remOnAll(ords, b"", 0) is False


async def test_append_on_val_guard_raises_when_final_put_fails():
    """Test appendOnVal fails closed when final insert does not succeed."""
    dber, _ = await _open_fake_dber(stores=["ords."], clear=True)
    ords = dber.env.open_db("ords.")

    assert dber.putOnVal(ords, b"evt", 0, b"icp") is True

    putVal = dber.putVal

    def reject_next_on(*, db: Any, key: bytes, val: bytes) -> bool:
        if key == _onKey(b"evt", 1):
            return False
        return putVal(db=db, key=key, val=val)

    dber.putVal = reject_next_on  # type: ignore[method-assign]
    try:
        try:
            dber.appendOnVal(ords, b"evt", b"rot")
        except ValueError as ex:
            assert ex.args[0] == "Failed appending val=b'rot' at key=b'evt'."
        else:
            raise AssertionError("appendOnVal should raise when final insert fails")
    finally:
        dber.putVal = putVal  # type: ignore[method-assign]


async def test_append_on_val_rejects_max_ordinal_overflow():
    """Test appendOnVal rejects append after MaxON for one prefix."""
    dber, _ = await _open_fake_dber(stores=["ords."], clear=True)
    ords = dber.env.open_db("ords.")
    maxon = int("f" * 32, 16)

    assert dber.putOnVal(ords, b"evt", maxon, b"last") is True
    try:
        dber.appendOnVal(ords, b"evt", b"overflow")
    except ValueError as ex:
        assert "exceeds maximum size." in ex.args[0]
        assert "key=b'evt'" in ex.args[0]
    else:
        raise AssertionError("appendOnVal should reject append after MaxON")


async def test_keripy_lmdber_core_contract():
    """Adapted from keripy/tests/db/test_dbing.py::test_lmdber for current Bdber methods."""
    dber, _ = await _open_fake_dber(stores=["beep."], clear=True)
    sdb = dber.env.open_db(key=b"beep.")

    key = b"A"
    val = b"whatever"
    assert dber.getVal(sdb, key) is None
    assert dber.delVal(sdb, key) is False
    assert dber.putVal(sdb, key, val) is True
    assert dber.putVal(sdb, key, val) is False
    assert dber.setVal(sdb, key, val) is True
    assert dber.getVal(sdb, key) == val
    assert dber.delVal(sdb, key) is True
    assert dber.getVal(sdb, key) is None

    assert dber.putVal(sdb, b"a.1", b"wow") is True
    assert dber.putVal(sdb, b"a.2", b"wee") is True
    assert dber.putVal(sdb, b"b.1", b"woo") is True

    assert list(dber.getTopItemIter(sdb)) == [
        (b"a.1", b"wow"),
        (b"a.2", b"wee"),
        (b"b.1", b"woo"),
    ]
    assert list(dber.getTopItemIter(sdb, b"a.")) == [
        (b"a.1", b"wow"),
        (b"a.2", b"wee"),
    ]
    assert dber.cntAll(sdb) == 3
    assert dber.delTop(sdb, b"a.") is True
    assert list(dber.getTopItemIter(sdb)) == [(b"b.1", b"woo")]


async def test_keripy_suber_contract():
    """Adapted from keripy/tests/db/test_subing.py::test_suber for current Bdber methods."""
    subing = _load_upstream_subing_module()
    dber, backend = await _open_fake_dber(
        name="suber-contract",
        stores=["bags.", "pugs."],
        clear=True,
    )

    bags = subing.Suber(db=dber, subkey="bags.")
    assert bags.sdb.flags()["dupsort"] is False

    assert bags.put(("test_key", "0001"), "Hello sailer!") is True
    assert bags.get(("test_key", "0001")) == "Hello sailer!"
    assert bags.put(("test_key", "0001"), "shadow") is False
    assert bags.pin(("test_key", "0001"), "Hey gorgeous!") is True
    assert bags.get(("test_key", "0001")) == "Hey gorgeous!"
    assert bags.rem(("test_key", "0001")) is True
    assert bags.get(("test_key", "0001")) is None

    assert bags.put((b"test_key", b"0002"), "Hello sailer!") is True
    assert bags.get((b"test_key", b"0002")) == "Hello sailer!"
    assert bags.put((b"test_key", "0003"), "Hello sailer!") is True
    assert bags.get((b"test_key", "0003")) == "Hello sailer!"

    assert bags.put("keystr", "Shove off!") is True
    assert bags.get("keystr") == "Shove off!"
    assert bags.pin("keystr", "Go away.") is True
    assert bags.get("keystr") == "Go away."

    pugs = subing.Suber(db=dber, subkey="pugs.")
    assert pugs.put(("a", "1"), "Blue dog") is True
    assert pugs.put(("a", "2"), "Green tree") is True
    assert pugs.put(("a", "3"), "Red apple") is True
    assert pugs.put(("a", "4"), "White snow") is True
    assert pugs.put(("b", "1"), "Blue dog") is True
    assert pugs.put(("b", "2"), "Green tree") is True
    assert pugs.put(("bc", "3"), "Red apple") is True
    assert pugs.put(("ac", "4"), "White snow") is True
    assert pugs.cnt() == 8

    assert list(pugs.getItemIter()) == [
        (("a", "1"), "Blue dog"),
        (("a", "2"), "Green tree"),
        (("a", "3"), "Red apple"),
        (("a", "4"), "White snow"),
        (("ac", "4"), "White snow"),
        (("b", "1"), "Blue dog"),
        (("b", "2"), "Green tree"),
        (("bc", "3"), "Red apple"),
    ]
    assert list(pugs.getItemIter(keys=("b", ""))) == [
        (("b", "1"), "Blue dog"),
        (("b", "2"), "Green tree"),
    ]
    assert list(pugs.getItemIter(keys=("a",), topive=True)) == [
        (("a", "1"), "Blue dog"),
        (("a", "2"), "Green tree"),
        (("a", "3"), "Red apple"),
        (("a", "4"), "White snow"),
    ]

    assert pugs.trim(keys=("b", "")) is True
    assert list(pugs.getItemIter()) == [
        (("a", "1"), "Blue dog"),
        (("a", "2"), "Green tree"),
        (("a", "3"), "Red apple"),
        (("a", "4"), "White snow"),
        (("ac", "4"), "White snow"),
        (("bc", "3"), "Red apple"),
    ]
    assert pugs.trim(keys=("a",), topive=True) is True
    assert list(pugs.getItemIter()) == [
        (("ac", "4"), "White snow"),
        (("bc", "3"), "Red apple"),
    ]

    assert bags.pin(("persist", "bag"), "kept") is True
    assert pugs.pin(("persist", "leaf"), "saved") is True
    assert await dber.flush() == 2

    reopened = await Bdber.open(
        name="suber-contract",
        stores=["bags.", "pugs."],
        storageOpener=backend.open,
    )
    bags_reloaded = subing.Suber(db=reopened, subkey="bags.")
    pugs_reloaded = subing.Suber(db=reopened, subkey="pugs.")
    assert bags_reloaded.get(("persist", "bag")) == "kept"
    assert pugs_reloaded.get(("persist", "leaf")) == "saved"
    assert list(pugs_reloaded.getItemIter()) == [
        (("ac", "4"), "White snow"),
        (("bc", "3"), "Red apple"),
        (("persist", "leaf"), "saved"),
    ]


async def test_keripy_on_suber_contract():
    """Adapted from keripy/tests/db/test_subing.py::test_on_suber for current Bdber methods."""
    subing = _load_upstream_subing_module()
    dber, _ = await _open_fake_dber(
        name="onsuber-contract",
        stores=["bags."],
        clear=True,
    )

    onsuber = subing.OnSuber(db=dber, subkey="bags.")
    assert onsuber.sdb.flags()["dupsort"] is False

    w = "Blue dog"
    x = "Green tree"
    y = "Red apple"
    z = "White snow"

    assert onsuber.appendOn(keys=("a",), val=w) == 0
    assert onsuber.appendOn(keys=("a",), val=x) == 1
    assert onsuber.appendOn(keys=("a",), val=y) == 2
    assert onsuber.appendOn(keys=("a",), val=z) == 3

    assert onsuber.cntOnAll(keys=("a",)) == 4
    assert onsuber.cntOnAll(keys=("a",), on=2) == 2
    assert onsuber.cntOnAll(keys=("a",), on=4) == 0
    assert onsuber.cntOnAll() == 4
    assert onsuber.cntOn() == 0
    assert onsuber.cntOn(keys="a") == 4
    assert onsuber.cntOn(keys=("a",), on=2) == 2
    assert onsuber.cntOn(keys=("a",), on=4) == 0

    assert list(onsuber.getItemIter()) == [
        (("a", "00000000000000000000000000000000"), "Blue dog"),
        (("a", "00000000000000000000000000000001"), "Green tree"),
        (("a", "00000000000000000000000000000002"), "Red apple"),
        (("a", "00000000000000000000000000000003"), "White snow"),
    ]
    assert list(onsuber.getOnTopItemIter()) == [
        (("a",), 0, "Blue dog"),
        (("a",), 1, "Green tree"),
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
    ]
    assert list(onsuber.getOnAllItemIter()) == [
        (("a",), 0, "Blue dog"),
        (("a",), 1, "Green tree"),
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
    ]
    assert list(onsuber.getOnAllItemIter(keys="a")) == [
        (("a",), 0, "Blue dog"),
        (("a",), 1, "Green tree"),
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
    ]
    assert list(onsuber.getOnAllItemIter(keys="a", on=2)) == [
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
    ]
    assert list(onsuber.getOnIterAll()) == [
        "Blue dog",
        "Green tree",
        "Red apple",
        "White snow",
    ]
    assert list(onsuber.getOnIterAll(keys="a")) == [
        "Blue dog",
        "Green tree",
        "Red apple",
        "White snow",
    ]
    assert list(onsuber.getOnIterAll(keys="a", on=2)) == [
        "Red apple",
        "White snow",
    ]

    assert onsuber.appendOn(keys=("b",), val=w) == 0
    assert onsuber.appendOn(keys=("b",), val=x) == 1
    assert onsuber.appendOn(keys=("bc",), val=y) == 0
    assert onsuber.appendOn(keys=("ac",), val=z) == 0

    assert onsuber.cntOnAll(keys=("b",)) == 2
    assert onsuber.cntOnAll(keys=("ac",), on=2) == 0
    assert onsuber.cntOnAll(keys="") == 8

    assert list(onsuber.getItemIter()) == [
        (("a", "00000000000000000000000000000000"), "Blue dog"),
        (("a", "00000000000000000000000000000001"), "Green tree"),
        (("a", "00000000000000000000000000000002"), "Red apple"),
        (("a", "00000000000000000000000000000003"), "White snow"),
        (("ac", "00000000000000000000000000000000"), "White snow"),
        (("b", "00000000000000000000000000000000"), "Blue dog"),
        (("b", "00000000000000000000000000000001"), "Green tree"),
        (("bc", "00000000000000000000000000000000"), "Red apple"),
    ]
    assert list(onsuber.getOnTopItemIter()) == [
        (("a",), 0, "Blue dog"),
        (("a",), 1, "Green tree"),
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
        (("ac",), 0, "White snow"),
        (("b",), 0, "Blue dog"),
        (("b",), 1, "Green tree"),
        (("bc",), 0, "Red apple"),
    ]
    assert list(onsuber.getOnTopItemIter(keys="b")) == [
        (("b",), 0, "Blue dog"),
        (("b",), 1, "Green tree"),
        (("bc",), 0, "Red apple"),
    ]
    assert list(onsuber.getOnAllItemIter(keys="b")) == [
        (("b",), 0, "Blue dog"),
        (("b",), 1, "Green tree"),
    ]
    assert list(onsuber.getOnAllItemIter(keys=("b",))) == [
        (("b",), 0, "Blue dog"),
        (("b",), 1, "Green tree"),
    ]
    assert list(onsuber.getOnAllItemIter(keys=("b", ""))) == []
    assert list(onsuber.getOnAllItemIter(keys="")) == [
        (("a",), 0, "Blue dog"),
        (("a",), 1, "Green tree"),
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
        (("ac",), 0, "White snow"),
        (("b",), 0, "Blue dog"),
        (("b",), 1, "Green tree"),
        (("bc",), 0, "Red apple"),
    ]
    assert list(onsuber.getOnAllItemIter()) == [
        (("a",), 0, "Blue dog"),
        (("a",), 1, "Green tree"),
        (("a",), 2, "Red apple"),
        (("a",), 3, "White snow"),
        (("ac",), 0, "White snow"),
        (("b",), 0, "Blue dog"),
        (("b",), 1, "Green tree"),
        (("bc",), 0, "Red apple"),
    ]

    assert onsuber.remOn(keys="a", on=1) is True
    assert onsuber.remOn(keys="a", on=1) is False
    assert onsuber.remOn(keys="a", on=3) is True
    assert onsuber.remOn(keys="a", on=3) is False

    assert onsuber.cntOnAll(keys=("a",)) == 2
    assert onsuber.cntOnAll() == 6
    assert onsuber.cntOn() == 0
    assert onsuber.cntOn(keys="a") == 2
    assert onsuber.cntOn(keys=("a",), on=2) == 1
    assert onsuber.cntOn(keys=("a",), on=4) == 0
    assert list(onsuber.getOnAllItemIter(keys="a")) == [
        (("a",), 0, "Blue dog"),
        (("a",), 2, "Red apple"),
    ]

    assert onsuber.putOn(keys="d", on=0, val="moon") is True
    assert onsuber.getOn(keys="d", on=0) == "moon"
    assert onsuber.getOnItem(keys="d", on=0) == (("d",), 0, "moon")
    assert onsuber.putOn(keys="d", on=0, val="moon") is False
    assert onsuber.pinOn(keys="d", on=0, val="sun") is True
    assert onsuber.getOn(keys="d", on=0) == "sun"
    assert onsuber.getOnItem(keys="d", on=0) == (("d",), 0, "sun")
    assert onsuber.remOn(keys="d", on=0) is True
    assert onsuber.getOn(keys="d", on=0) is None
    assert onsuber.getOnItem(keys="d", on=0) is None

    assert onsuber.putOn(keys="d", on=0, val="moon") is True
    assert onsuber.putOn(keys="d", on=1, val="sun") is True
    assert onsuber.putOn(keys="d", on=2, val="stars") is True
    assert onsuber.putOn(keys="e", on=0, val="stars") is True
    assert onsuber.putOn(keys="e", on=1, val="moon") is True
    assert onsuber.putOn(keys="e", on=2, val="sun") is True

    assert onsuber.cntOnAll(keys="d") == 3
    assert onsuber.cntOnAll(keys="e") == 3
    assert onsuber.cntOnAll() == 12

    assert onsuber.remOnAll(keys="d", on=1) is True
    assert onsuber.cntOnAll(keys="d") == 1
    assert onsuber.cntOnAll(keys="e") == 3
    assert onsuber.cntOnAll() == 10

    assert onsuber.remOnAll(keys="d") is True
    assert onsuber.cntOnAll(keys="d") == 0
    assert onsuber.cntOnAll(keys="e") == 3
    assert onsuber.cntOnAll() == 9

    assert onsuber.remOnAll() is True
    assert onsuber.cntOnAll(keys="d") == 0
    assert onsuber.cntOnAll(keys="e") == 0
    assert onsuber.cntOnAll() == 0


LOCAL_BACKEND_TESTS: list[TestCase] = [
    ("test_open_declares_stores_and_clear_resets_persisted_state", test_open_declares_stores_and_clear_resets_persisted_state),
    ("test_open_requires_storage_backend", test_open_requires_storage_backend),
    ("test_open_db_flag_persistence", test_open_db_flag_persistence),
    ("test_open_db_metadata_only_flush_persists_dupsort", test_open_db_metadata_only_flush_persists_dupsort),
    ("test_clear_resets_persisted_store_metadata", test_clear_resets_persisted_store_metadata),
    ("test_open_rejects_non_empty_records_without_store_metadata", test_open_rejects_non_empty_records_without_store_metadata),
    ("test_open_db_rejects_unconfigured_store", test_open_db_rejects_unconfigured_store),
    ("test_storify_supported_inputs", test_storify_supported_inputs),
    ("test_storify_reject_invalid_types", test_storify_reject_invalid_types),
    ("test_serialize_deserialize_edge_cases", test_serialize_deserialize_edge_cases),
    ("test_val_crud_and_dirty_noop_semantics", test_val_crud_and_dirty_noop_semantics),
    ("test_empty_key_errors", test_empty_key_errors),
    ("test_prefix_iteration_and_count", test_prefix_iteration_and_count),
    ("test_delTop_prefix_and_whole_store_behavior", test_delTop_prefix_and_whole_store_behavior),
    ("test_flush_persistence_semantics", test_flush_persistence_semantics),
    ("test_flush_counts_only_dirty_stores", test_flush_counts_only_dirty_stores),
    ("test_flush_with_hio_ado", test_flush_with_hio_ado),
    ("test_flush_with_hio_ado_when_clean", test_flush_with_hio_ado_when_clean),
    ("test_on_val_dirty_flush_and_persistence_semantics", test_on_val_dirty_flush_and_persistence_semantics),
    ("test_on_val_and_empty_key_behavior", test_on_val_and_empty_key_behavior),
    ("test_append_on_val_guard_raises_when_final_put_fails", test_append_on_val_guard_raises_when_final_put_fails),
    ("test_append_on_val_rejects_max_ordinal_overflow", test_append_on_val_rejects_max_ordinal_overflow),
]

KERIPY_CONTRACT_TESTS: list[TestCase] = [
    ("test_keripy_ordinal_key_helpers", test_keripy_ordinal_key_helpers),
    ("test_keripy_lmdber_core_contract", test_keripy_lmdber_core_contract),
    ("test_keripy_lmdber_get_on_item_empty_value_contract", test_keripy_lmdber_get_on_item_empty_value_contract),
    ("test_keripy_lmdber_on_val_contract", test_keripy_lmdber_on_val_contract),
    ("test_keripy_lmdber_append_on_iter_contract", test_keripy_lmdber_append_on_iter_contract),
    ("test_keripy_suber_contract", test_keripy_suber_contract),
    ("test_keripy_on_suber_contract", test_keripy_on_suber_contract),
]


async def run_all_tests():
    """Run the current-surface Bdber test suite."""
    results = TestResults()

    print("=" * 64)
    print("Bdber Storage-Backed Tests")
    print("=" * 64)

    local_backend_tests = list(LOCAL_BACKEND_TESTS)
    if pyscript_storage is not None:
        local_backend_tests.append(
            ("test_browser_storage_flush_integration", test_browser_storage_flush_integration)
        )
    else:
        print()
        print("Browser storage unavailable; skipping PyScript integration test.")

    await _run_named_tests("local_backend", local_backend_tests, results)
    await _run_named_tests("keripy_contracts", KERIPY_CONTRACT_TESTS, results)

    print()
    results.print_summary()
    return results


if __name__ == "__main__":
    asyncio.run(run_all_tests())
