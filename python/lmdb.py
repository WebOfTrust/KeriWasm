"""lmdb stub for Pyodide/WASM environments.

keripy uses ``lmdb`` (C-extension) for persistent key-value storage.
There is no WASM wheel for lmdb — in the browser we use IndexedDB instead
via ``indexeddb_python.IndexedDBer``.

This stub allows ``import lmdb`` to succeed so that keripy module-level
imports do not crash.  Any actual construction of an ``lmdb.Environment``
will raise ``NotImplementedError`` directing the caller to use IndexedDBer.
"""


class _StubEnvironment:
    """Placeholder for ``lmdb.Environment``."""

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "lmdb is not available in Pyodide/WASM. "
            "Use indexeddb_python.IndexedDBer for browser persistence."
        )


def open(*args, **kwargs):  # noqa: A001 — matches lmdb.open signature
    """Drop-in for ``lmdb.open()`` that immediately raises."""
    raise NotImplementedError(
        "lmdb.open() is not available in Pyodide/WASM. "
        "Use indexeddb_python.IndexedDBer for browser persistence."
    )


Environment = _StubEnvironment

# Constants that keripy may reference at import time
NOTLS = 0x200000
NORDAHEAD = 0x100000
NOSYNC = 0x10000
WRITEMAP = 0x80000
