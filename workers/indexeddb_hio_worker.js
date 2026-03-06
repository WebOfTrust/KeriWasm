/*
 * Worker-hosted hio/IndexedDB experiment runner.
 * Boots Pyodide off the main thread and runs the KeriWasm async-doist probes.
 */

importScripts("/workers/worker_contract.js");

const CONTRACT = self.KERIWasmWorkerContract;
if (!CONTRACT) {
    throw new Error("Missing KERIWasmWorkerContract");
}

let pyodide = null;
let pyodideReady = null;
let running = false;

const PYODIDE_INDEX_URL = "https://cdn.jsdelivr.net/pyodide/v0.29.1/full/";
const PYODIDE_JS = `${PYODIDE_INDEX_URL}pyodide.js`;

const LOG_FLUSH_MS = 150;
const LOG_BATCH_MAX = 40;
const logBuffer = [];

function nowStamp() {
    return new Date().toISOString().split("T")[1].slice(0, 12);
}

function flushLogs() {
    if (logBuffer.length === 0) return;
    const batch = logBuffer.splice(0, logBuffer.length);
    self.postMessage({ type: CONTRACT.MESSAGE_TYPE.LOG_BATCH, entries: batch });
}

function enqueueLog(entry) {
    logBuffer.push(entry);
    if (logBuffer.length >= LOG_BATCH_MAX) {
        flushLogs();
    }
}

function logMsg(msg, css = "info") {
    enqueueLog({ time: nowStamp(), css, msg });
}

setInterval(() => {
    if (logBuffer.length > 0) {
        flushLogs();
    }
}, LOG_FLUSH_MS);

self.enqueueIndexedDbHioLogJson = (payload) => {
    try {
        enqueueLog(JSON.parse(payload));
    } catch (err) {
        enqueueLog({
            time: nowStamp(),
            css: "fail",
            msg: `Log parse error: ${err && err.message ? err.message : String(err)}`,
        });
    }
};

self.flushIndexedDbHioLogs = flushLogs;

const PY_FILES = [
    { url: "/python/run_indexeddb_hio_worker.py", path: "/run_indexeddb_hio_worker.py" },
    { url: "/python/indexeddb_hio_experiments.py", path: "/indexeddb_hio_experiments.py" },
    { url: "/python/indexeddb_python.py", path: "/indexeddb_python.py" },
    { url: "/python/hio/__init__.py", path: "/hio/__init__.py" },
    { url: "/python/hio/hioing.py", path: "/hio/hioing.py" },
    { url: "/python/hio/base/__init__.py", path: "/hio/base/__init__.py" },
    { url: "/python/hio/base/basing.py", path: "/hio/base/basing.py" },
    { url: "/python/hio/base/doing.py", path: "/hio/base/doing.py" },
    { url: "/python/hio/base/tyming.py", path: "/hio/base/tyming.py" },
    { url: "/python/hio/help/__init__.py", path: "/hio/help/__init__.py" },
    { url: "/python/hio/help/helping.py", path: "/hio/help/helping.py" },
    { url: "/python/hio/help/hicting.py", path: "/hio/help/hicting.py" },
    { url: "/python/hio/help/timing.py", path: "/hio/help/timing.py" },
];

function resolveUrl(path) {
    if (path.startsWith("http")) return path;
    return `${self.location.origin}${path}`;
}

async function fetchToFS(url, path) {
    const res = await fetch(resolveUrl(url), { cache: "no-store" });
    if (!res.ok) {
        throw new Error(`Failed to fetch ${url}: ${res.status} ${res.statusText}`);
    }
    const data = new Uint8Array(await res.arrayBuffer());
    const dir = path.substring(0, path.lastIndexOf("/"));
    if (dir) {
        pyodide.FS.mkdirTree(dir);
    }
    pyodide.FS.writeFile(path, data);
}

async function loadPyFiles() {
    for (const file of PY_FILES) {
        await fetchToFS(file.url, file.path);
    }
}

async function initPyodide() {
    if (pyodideReady) return pyodideReady;

    pyodideReady = (async () => {
        if (typeof indexedDB === "undefined") {
            throw new Error("Worker JS global indexedDB is unavailable");
        }

        logMsg("Worker JS detected indexedDB before Pyodide boot", "success");
        logMsg("Loading Pyodide runtime...");

        importScripts(PYODIDE_JS);
        pyodide = await loadPyodide({ indexURL: PYODIDE_INDEX_URL });
        logMsg("Pyodide runtime loaded", "success");
        logMsg("Loading built-in Pyodide packages...");
        await pyodide.loadPackage(["micropip", "multidict"]);
        logMsg("Built-in Pyodide packages loaded", "success");
        logMsg("Loading local Python files into worker FS...");
        await loadPyFiles();
        logMsg("Local Python files loaded", "success");
        logMsg("Installing Python dependencies with micropip...");
        await pyodide.runPythonAsync(`\
import sys, micropip
if "/" not in sys.path:
    sys.path.insert(0, "/")
await micropip.install(["ordered-set"])
`);
        logMsg("Worker Python dependencies installed", "success");
        return pyodide;
    })();

    return pyodideReady;
}

self.onmessage = async (event) => {
    const { type } = event.data || {};

    if (type !== CONTRACT.REQUEST_TYPE.RUN) return;

    if (running) {
        self.postMessage({
            type: CONTRACT.MESSAGE_TYPE.STATUS,
            state: CONTRACT.STATE.BUSY,
        });
        return;
    }

    running = true;
    self.postMessage({
        type: CONTRACT.MESSAGE_TYPE.STATUS,
        state: CONTRACT.STATE.STARTING,
    });

    try {
        await initPyodide();
        logMsg("Pyodide worker bootstrap complete", "success");
        self.postMessage({
            type: CONTRACT.MESSAGE_TYPE.STATUS,
            state: CONTRACT.STATE.RUNNING,
        });
        logMsg("Starting Python worker runner...");
        await pyodide.runPythonAsync("import run_indexeddb_hio_worker; await run_indexeddb_hio_worker.run()");
        flushLogs();
        self.postMessage({
            type: CONTRACT.MESSAGE_TYPE.STATUS,
            state: CONTRACT.STATE.DONE,
        });
    } catch (err) {
        flushLogs();
        self.postMessage({
            type: CONTRACT.MESSAGE_TYPE.STATUS,
            state: CONTRACT.STATE.ERROR,
            error: err && err.message ? err.message : String(err),
        });
    } finally {
        running = false;
    }
};
