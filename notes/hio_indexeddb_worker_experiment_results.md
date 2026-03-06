Hio `ado()` + IndexedDB Worker Results
--------------------------------------

Date: 2026-03-06

Question
--------

This round of testing was meant to answer three direct questions:

- does raw async IndexedDB work under `Doist.ado(real=True)` in a Pyodide
  worker?
- does the older `.send(None)` coroutine-driving pattern still help once the
  coroutine actually touches the event loop?
- can a sync-looking DB surface still support runtime `Suber` / `Komer` style
  calls while the real backend is async IndexedDB?


What I Ran
----------

I ran the browser-side experiments in a Pyodide worker.

Files used:

- `KeriWasm/workers/indexeddb_hio_worker.js`
- `KeriWasm/python/run_indexeddb_hio_worker.py`
- `KeriWasm/python/indexeddb_hio_experiments.py`

The worker entrypoint calls `run_all_experiments()`:

`KeriWasm/python/indexeddb_hio_experiments.py`

```python
async def run_all_experiments(log: LogFn) -> None:
    ...
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

    sync_doer = TaskPollingDoer(
        label="sync-facade-positive",
        runner=lambda: _sync_facade_positive_tests(log),
        log=log,
        tock=0.0,
    )
    await _run_doist_with([sync_doer], tock=0.02, limit=10.0)
    await _sync_facade_negative_tests(log)
```


1. `_direct_async_backend_tests`
--------------------------------

What it tests:

- raw async IndexedDB open
- `Val` CRUD
- prefix iteration
- ordinal CRUD
- close/reopen persistence
- expected error propagation

Code:

`KeriWasm/python/indexeddb_hio_experiments.py`

```python
async def _direct_async_backend_tests(log: LogFn) -> None:
    dber = await IndexedDBer.open(db_name, stores, version=1)
    try:
        log("Opened raw async IndexedDB backend", "success")

        _require(await dber.setVal("val_store", b"alpha", b"one") is True, "setVal failed")
        _require(await dber.getVal("val_store", b"alpha") == b"one", "getVal mismatch")
        _require(await dber.putVal("val_store", b"alpha", b"other") is False, "putVal overwrote")

        items = await dber.getTopItemIter("val_store", b"alph")
        _require(items == [(b"alpha", b"one"), (b"alphabet", b"two")], ...)
        log("Val CRUD + prefix iteration succeeded", "success")

        on0 = await dber.appendOnVal("ordinal_store", b"evt", b"first")
        on1 = await dber.appendOnVal("ordinal_store", b"evt", b"second")
        _require((on0, on1) == (0, 1), ...)
        log("Ordinal family CRUD succeeded", "success")

        dber.close()
        dber = await IndexedDBer.open(db_name, stores, version=1)
        _require(await dber.getVal("val_store", b"alpha") == b"one", ...)
        log("Write completion survived close/reopen", "success")
```

Output:

```text
[doer:direct-async-backend] started task direct-async-backend
Opened raw async IndexedDB backend
Val CRUD + prefix iteration succeeded
Ordinal family CRUD succeeded
Write completion survived close/reopen
Expected KeyError surfaced: "Key: `b''` is either empty, too big (for lmdb), or wrong DUPFIXED size. ref) lmdb.BadValsizeError"
[doer:direct-async-backend] completed at tyme=0.0200
```

What it proved:

Raw async IndexedDB operations worked under `Doist.ado(real=True)` in the
worker, including persistence across close/reopen.


2. `_experiment_send_none_negative`
-----------------------------------

What it tests:

- whether manually driving a loop-backed coroutine with `.send(None)` is a
  reliable bridge

Code:

`KeriWasm/python/indexeddb_hio_experiments.py`

```python
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
```

Output:

```text
First send(None) yielded PyodideFuture
send(None) on loop-backed coroutine failed as expected
```

What it proved:

The `.send(None)` pattern is not a reliable way to drive coroutines once they
await real event-loop-backed work.


3. `_experiment_timeout_cancel_negative`
----------------------------------------

What it tests:

- whether timeout/cancel can leave a false impression that a write persisted

Code:

`KeriWasm/python/indexeddb_hio_experiments.py`

```python
async def _experiment_timeout_cancel_negative(log: LogFn) -> None:
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
```

Output:

```text
[doer:timeout-negative] started task timeout-negative
Timeout/cancel produced no persisted write
```

What it proved:

Timed-out or cancelled async work did not leave behind a false persisted write.


4. `_sync_facade_positive_tests`
--------------------------------

What it tests:

- sync-looking `Suber` style val operations
- sync-looking ordinal operations
- sync-looking `Komer` style operations
- immediate read-your-writes behavior from the mirror
- persisted flush into the async backend

Code:

`KeriWasm/python/indexeddb_hio_experiments.py`

```python
async def _sync_facade_positive_tests(log: LogFn) -> None:
    facade = await SyncIndexedDBFacade.open(db_name, stores, version=1)
    try:
        suber = CompatSuber(facade, subkey="suber_store")
        _require(suber.put(("alpha",), "one") is True, "Suber.put failed")
        _require(suber.get(("alpha",)) == "one", "Mirror read-your-writes failed")
        await facade.flush()
        log("Suber-compatible val path preserved immediate reads and persisted flush", "success")

        onsuber = CompatOnSuber(facade, subkey="ordinal_store")
        _require(onsuber.appendOn(("evt",), "first") == 0, "First appendOn mismatch")
        _require(onsuber.appendOn(("evt",), "second") == 1, "Second appendOn mismatch")
        await facade.flush()
        log("Ordinal Suber-compatible path preserved deterministic ordered flush", "success")

        komer = CompatKomer(facade, subkey="komer_store", schema=WalletRecord)
        _require(komer.put(("wallet", "EA123"), record) is True, "Komer.put failed")
        _require(komer.get(("wallet", "EA123")) == record, "Komer mirror read mismatch")
        await facade.flush()
        log("Komer-compatible val path preserved existing sync call shape", "success")
```

Output:

```text
[doer:sync-facade-positive] started task sync-facade-positive
Suber-compatible val path preserved immediate reads and persisted flush
Ordinal Suber-compatible path preserved deterministic ordered flush
Komer-compatible val path preserved existing sync call shape
[doer:sync-facade-positive] completed at tyme=0.0200
```

What it proved:

For the tested runtime subset, a sync-looking surface can preserve the existing
call shape while flushing real async IndexedDB writes underneath.


5. `_sync_facade_negative_tests`
--------------------------------

What it tests:

- whether the sync-looking facade fails closed if backend flush diverges

Code:

`KeriWasm/python/indexeddb_hio_experiments.py`

```python
class FaultyIndexedDBBackend:
    async def _fail_once(self, method: str, *pa, **kwa):
        if not self._failed:
            self._failed = True
            raise RuntimeError(
                f"forced flush failure for divergence test during {method}"
            )

async def _sync_facade_negative_tests(log: LogFn) -> None:
    ...
    try:
        await facade.flush()
    except MirrorDivergenceError as exc:
        log(f"Flush failure surfaced explicit divergence: {exc}", "success")

    ...

    try:
        suber.get(("beta",))
    except MirrorDivergenceError:
        log("Diverged facade blocks further sync calls", "success")
```

Output:

```text
Flush failure surfaced explicit divergence: Flush failed in putVal: forced flush failure for divergence test during putVal
Diverged facade blocks further sync calls
```

What it proved:

If backend flush fails, the facade can mark itself divergent and stop serving
sync-style calls instead of hiding the failure.


Final Output Summary
--------------------

The worker run ended with:

```text
Promotion Gate
--------------
Phase 1 proved raw async IndexedDB under ado(real=True)
Phase 2 proved runtime CRUD call-shape compatibility for Suber/Komer probes
Startup/open-path integration remains deferred by design
```


Conclusions
-----------

- Raw async IndexedDB worked under `Doist.ado(real=True)` in the Pyodide
  worker.
- The `.send(None)` approach is not a reliable bridge for loop-backed async
  work.
- The task-create / poll pattern worked for the IndexedDB case.
- The sync-looking runtime surface worked for the tested `Suber`, ordinal, and
  `Komer` style cases.
- This test stage covered worker-hosted runtime CRUD only. It did not cover
  startup/open-path behavior such as `LMDBer.reopen`, `Habery.setup`, or
  constructor-time DB access.
