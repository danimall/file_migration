# filemig — safe Dropbox → Google Drive migration

A local, single-user web app that migrates files from a personal Dropbox to a
personal Google Drive **safely and verifiably**. Every file is downloaded,
checked against Dropbox's own hash, uploaded to Drive with resumable chunks, and
checked again against Drive's checksum before it's marked done.

**This build is copy-and-verify only — it never deletes anything from Dropbox.**
You delete the originals yourself, on your own schedule, once you're satisfied
the copies are verified. (Deletion support exists in the code but is disabled
behind a kill switch — see [Copy-only mode](#copy-only-mode).)

## How it works

1. **Browse & select** — the dashboard shows your Dropbox folder tree with file
   counts and sizes. Check the folders you want to migrate (selecting a folder
   includes its whole subtree).
2. **Plan** — "Plan all batches" splits your selection into as many batches as
   needed (bounded by a per-batch file/size cap). Nothing copies yet.
3. **Start** — "Start all proposed" runs every planned batch. Workers copy and
   verify continuously, self-throttling under Google's daily upload cap.
4. **Monitor** — a live activity bar shows progress (%, throughput, ETA) and the
   folder tree marks folders "✓ copied & verified" as they complete.

Files land in a single app-created folder, **`My Drive / Dropbox Migration /`**,
mirroring your Dropbox folder structure.

## Safety model

* **Two-leg verification.** A file is only marked `verified` after BOTH:
  * **Leg 1** — bytes downloaded from Dropbox match Dropbox's `content_hash`
    (4 MiB block double-SHA-256; verified byte-identical to Dropbox's reference
    implementation).
  * **Leg 2** — bytes that landed in Drive match Drive's `md5Checksum` (and
    `sha256Checksum` when present) plus size. A real content hash is *always*
    compared — never size alone.
* **No deletion.** The app cannot remove anything from Dropbox (enforced at the
  client, service, and route layers).
* **Crash-safe & resumable.** All state lives in one SQLite file (WAL). On
  restart, interrupted work resumes from durable checkpoints, including mid-file
  resumable uploads (the Drive session URI + confirmed byte offset are
  persisted). A **Resume** button re-tries any failed files per batch.
* **Quota-aware.** Google enforces a hard ~750 GB/day per-user upload cap. The
  scheduler tracks a rolling-24h ledger and throttles uploads below an
  ~712 GiB/day effective cap, so a 1 TB+ migration safely spans multiple days.

## Prerequisites

* Python 3.12+ and [`uv`](https://docs.astral.sh/uv/).
* A **Dropbox** scoped app (App Console → *Create app* → *Scoped access* →
  *Full Dropbox*). On the **Permissions** tab enable `files.metadata.read`,
  `files.content.read`, `files.content.write`, then **Submit**. On **Settings**,
  add the redirect URI `http://127.0.0.1:8766/auth/dropbox/callback`.
* A **Google Cloud** project with the **Drive API** enabled and an OAuth client
  of type **Desktop app**.
  * Consent-screen scope: `https://www.googleapis.com/auth/drive.file`
    (non-sensitive — no Google verification needed).
  * **Publish the consent screen to "Production"** (Google Auth Platform →
    Audience → Publish). Otherwise refresh tokens expire after 7 days and a
    multi-day migration breaks.

## Setup

```sh
uv venv --python 3.12
uv pip install -e ".[dev]"
cp .env.example .env        # then fill in the four app keys/secrets
```

`.env` holds your OAuth credentials and is git-ignored — never commit it.

## Run

```sh
uv run filemig run          # serves http://127.0.0.1:8766/
```

Open the dashboard, connect both accounts, then browse → select → plan → start.

### Keep the Mac awake for multi-day transfers

```sh
caffeinate -dimsu uv run filemig run
```

For unattended runs, install the launchd agent (see
`launchd/com.user.filemigration.plist.example`), which restarts the process on
crash. Keep the staging directory on the internal SSD.

## CLI

```sh
uv run filemig run          # start the web dashboard + workers
uv run filemig status       # one-shot migration status summary
uv run filemig reconcile    # re-verify destination vs source, print a report
```

## Copy-only mode

Source deletion is disabled by the constant `DROPBOX_DELETION_ENABLED = False`
in `src/filemig/clients/dropbox_client.py`. While off:

* the Dropbox client's delete methods raise before any API call,
* `service.approve_delete` refuses, and
* the `/batches/{id}/approve-delete` route returns 403.

To re-enable deletion, flip that constant and restore the deletion flow in the
service/routes/UI. The (verified, manual-approval, soft-delete) deleter and its
tests are retained but unused.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the module layout, data model, state
machines, and verification protocol.

## Tests

```sh
uv run pytest
```
