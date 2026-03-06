"""
Worker-side runner for the hio/IndexedDB experiment plan.
"""

from __future__ import annotations

import datetime
import json
import traceback

import js  # type: ignore

from indexeddb_hio_experiments import run_all_experiments


def _escape(msg: str) -> str:
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _emit(msg: str, css_class: str = "info") -> None:
    payload = json.dumps(
        {
            "time": datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "msg": _escape(msg),
            "css": css_class,
        }
    )
    js.enqueueIndexedDbHioLogJson(payload)


async def run() -> None:
    """Execute the worker-hosted experiment plan."""
    _emit("Starting worker-hosted hio/IndexedDB experiments...", "info")
    try:
        await run_all_experiments(_emit)
    except Exception as exc:
        _emit(f"Experiment run failed: {exc}", "fail")
        _emit(traceback.format_exc(), "fail")
        raise
    finally:
        js.flushIndexedDbHioLogs()
