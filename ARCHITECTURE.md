# Architecture

filemig is a single-process, single-user async web app. One SQLite file is the
source of truth; an in-process asyncio worker pool moves files; a FastAPI +
HTMX dashboard drives and monitors it.

## Module layout

```
src/filemig/
├── config.py            # pydantic-settings: ports, paths, concurrency, caps
├── cli.py               # `filemig run|status|reconcile`
├── app.py               # FastAPI factory + lifespan (recovery → workers)
├── service.py           # MigrationService: owns singletons, high-level actions
├── domain/
│   ├── states.py        # FileState / BatchState enums + transition tables
│   ├── models.py        # dataclasses (FileRecord, Batch, FolderNode, …)
│   └── errors.py        # RetryableError / FatalError / VerificationMismatch
├── hashing/
│   ├── dropbox_content_hash.py  # the safety-critical hasher (4 MiB blocks)
│   └── multihash.py             # single-pass dbx_hash + md5 + sha256 + size
├── clients/
│   ├── http.py          # shared retry/backoff/jitter + Retry-After handling
│   ├── auth.py          # AuthManager: live access tokens, refresh, 401 retry
│   ├── tokens.py        # 0600 refresh-token store
│   ├── oauth_dropbox.py # auth-code offline flow
│   ├── oauth_google.py  # loopback PKCE (S256), prompt=consent
│   ├── dropbox_client.py# list_folder, download, export, delete (disabled)
│   └── drive_client.py  # folders, resumable upload + resume probe, checksums
├── pipeline/
│   ├── discovery.py     # full recursive scan + lazy per-level fetch
│   ├── folders.py       # depth-first Drive folder tree creation + cache
│   ├── scheduler.py     # quota status, plan-all / per-folder batch building
│   ├── worker.py        # per-file state machine + both verification legs
│   ├── pool.py          # asyncio worker pool, leasing, pause/resume
│   ├── recovery.py      # startup reset of interrupted rows
│   └── deleter.py       # source deletion (DISABLED in copy-only mode)
├── web/
│   ├── routes.py        # all HTTP endpoints
│   ├── events.py        # in-process pub/sub event bus
│   └── sse.py           # Server-Sent Events stream
├── templates/           # Jinja2 + HTMX fragments
└── util/                # clock, identity (appProperties hashing)
```

`db/store.py` is the single validated writer: every state transition flows
through it, is checked against the transition tables, writes an append-only
`events` row, and publishes to the in-process bus for live UI updates.

## Data model (SQLite, WAL)

* **runs** — one migration job; holds the Drive root folder id + `run_uuid`
  (stamped into Drive `appProperties` for idempotency).
* **folders** — Dropbox folder tree → Drive folder id mapping.
* **files** — the core work table; one row per file walking the file state
  machine, with Dropbox metadata, locally-computed hashes, Drive ids/checksums,
  resumable-upload offset, lease fields, and timestamps.
* **batches** — the unit of approval/work; counters + lifecycle state.
* **upload_ledger** — rolling-24h byte accounting for the quota gate.
* **events** — append-only audit trail + SSE feed.

All writes go through one connection serialized by an `asyncio.Lock` (SQLite is
single-writer); blocking calls run in a worker thread so the event loop never
stalls.

## State machines

**File:** `discovered → queued → downloading → downloaded → uploading →
uploaded → verifying → verified` (terminal in copy-only mode). Error states:
`failed` (retryable, re-queued) and `dead` (fatal / hash mismatch — never
auto-deleted). Transient states (downloading/uploading/verifying) are rewound to
`queued` by startup recovery; progress fields are preserved so the worker
resumes (skips a completed download, resumes a partial upload via the persisted
Drive session URI + offset).

**Batch:** `proposed → copy_approved → running → awaiting_delete_approval`
(terminal in copy-only mode = "copied & verified"). `running →
awaiting_delete_approval` happens automatically once every file settles
(verified or dead). A finished batch can be put back to `running` by **Resume**
to retry incomplete files.

## Per-file pipeline (worker.py)

1. **Claim** a queued file (lease) — atomic, only from a `running` batch.
2. **Idempotency probe** — if a Drive file tagged with this source's identity
   already exists and its md5 matches our local md5, short-circuit to verified.
3. **Download** to a staging temp file while computing all hashes in one pass.
4. **Leg 1** — recomputed Dropbox `content_hash` must match Dropbox's metadata.
5. **Upload** — resumable session; chunks are 256 KiB multiples; the confirmed
   offset is persisted each chunk so a crash resumes mid-file. Zero-byte files
   are finalized with an empty `bytes */0` chunk.
6. **Leg 2** — Drive's `md5Checksum`/`sha256Checksum` + size must match our
   local values. A real content hash is always compared (never size-only).
7. **Verified** — record bytes in the quota ledger, delete the staged temp file.

Any hash/size mismatch escalates the file to `dead` — it is never marked
verified and (with deletion enabled) never deletable.

## Verification protocol

Dropbox `content_hash` (block double-SHA-256) and Drive `md5Checksum` are
*different algorithms* and are never compared to each other. Each leg compares
against the bytes we held locally, which transitively chains
Dropbox → local → Drive. Both legs must pass.

## Quotas & batching

Google caps uploads at ~750 GB/day/user (unraisable). The scheduler tracks a
rolling-24h ledger and enforces an effective cap (~712 GiB, 5% margin). Batch
*planning* is not quota-bound (batches are just plans); the worker's in-flight
gate throttles actual uploads, pausing near the cap and resuming when the window
rolls. Idempotency uses Drive `appProperties` (the source path is hashed to fit
the 124-byte key+value limit), and probes are batched per folder to respect
Drive's quota-unit budget.
