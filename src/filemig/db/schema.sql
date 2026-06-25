-- filemig schema. Single SQLite file is the source of truth.
-- All writes funnel through one writer connection (db/connection.py).

-- One row per migration job.
CREATE TABLE IF NOT EXISTS runs (
    id            INTEGER PRIMARY KEY,
    run_uuid      TEXT NOT NULL UNIQUE,   -- = migrationRunId stamped into Drive appProperties
    dst_root_id   TEXT,                   -- app-created Drive root folder id
    dst_root_name TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',  -- active | paused | done
    created_at    TEXT NOT NULL
);

-- Dropbox folder tree -> Drive folder id mapping.
CREATE TABLE IF NOT EXISTS folders (
    id                INTEGER PRIMARY KEY,
    run_id            INTEGER NOT NULL REFERENCES runs(id),
    src_path_lower    TEXT NOT NULL,            -- "" = root
    src_path_display  TEXT NOT NULL,
    parent_path_lower TEXT,                     -- NULL for root
    dst_folder_id     TEXT,                     -- NULL until created on Drive
    depth             INTEGER NOT NULL,         -- guard vs Drive 100-level cap
    created_at        TEXT NOT NULL,
    UNIQUE(run_id, src_path_lower)
);
CREATE INDEX IF NOT EXISTS idx_folders_parent  ON folders(run_id, parent_path_lower);
CREATE INDEX IF NOT EXISTS idx_folders_pending ON folders(run_id) WHERE dst_folder_id IS NULL;

-- Core work table: one row per file, walking the file state machine.
CREATE TABLE IF NOT EXISTS files (
    id                 INTEGER PRIMARY KEY,
    run_id             INTEGER NOT NULL REFERENCES runs(id),
    batch_id           INTEGER REFERENCES batches(id),

    -- Dropbox identity / metadata (captured at discovery)
    dbx_id             TEXT NOT NULL,
    src_path_lower     TEXT NOT NULL,
    src_path_display   TEXT NOT NULL,
    parent_path_lower  TEXT NOT NULL,
    dbx_rev            TEXT NOT NULL,            -- parent_rev guard on delete
    size               INTEGER NOT NULL,
    is_downloadable    INTEGER NOT NULL DEFAULT 1,
    export_format      TEXT,
    src_content_hash   TEXT,                     -- NULL for cloud docs
    src_path_sha256    TEXT NOT NULL,            -- appProperties probe key
    is_shared          INTEGER NOT NULL DEFAULT 0,

    -- Locally computed during download (single streaming pass)
    local_dbx_hash     TEXT,
    local_md5          TEXT,
    local_sha256       TEXT,
    local_size         INTEGER,
    export_hash        TEXT,

    -- Drive side
    dst_file_id        TEXT,
    dst_md5            TEXT,
    dst_sha256         TEXT,
    dst_size           INTEGER,
    upload_session_uri TEXT,
    upload_offset      INTEGER NOT NULL DEFAULT 0,

    -- Verification
    leg1_ok            INTEGER NOT NULL DEFAULT 0,
    leg2_ok            INTEGER NOT NULL DEFAULT 0,

    -- State machine
    state              TEXT NOT NULL DEFAULT 'discovered',
    prev_durable_state TEXT,
    attempts           INTEGER NOT NULL DEFAULT 0,
    last_error         TEXT,
    staged_path        TEXT,
    leased_by          TEXT,
    leased_until       TEXT,

    discovered_at      TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    verified_at        TEXT,
    deleted_at         TEXT,
    UNIQUE(run_id, dbx_id)
);
CREATE INDEX IF NOT EXISTS idx_files_state ON files(run_id, state);
CREATE INDEX IF NOT EXISTS idx_files_batch ON files(batch_id, state);
CREATE INDEX IF NOT EXISTS idx_files_lease ON files(state, leased_until);
CREATE INDEX IF NOT EXISTS idx_files_probe ON files(run_id, src_path_sha256);

-- The approve-each-batch unit.
CREATE TABLE IF NOT EXISTS batches (
    id                 INTEGER PRIMARY KEY,
    run_id             INTEGER NOT NULL REFERENCES runs(id),
    seq                INTEGER NOT NULL,
    state              TEXT NOT NULL DEFAULT 'proposed',
    file_count         INTEGER NOT NULL DEFAULT 0,
    total_bytes        INTEGER NOT NULL DEFAULT 0,
    copied_count       INTEGER NOT NULL DEFAULT 0,
    verified_count     INTEGER NOT NULL DEFAULT 0,
    failed_count       INTEGER NOT NULL DEFAULT 0,
    deleted_count      INTEGER NOT NULL DEFAULT 0,
    proposed_at        TEXT NOT NULL,
    copy_approved_at   TEXT,
    delete_approved_at TEXT,
    completed_at       TEXT,
    UNIQUE(run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(run_id, state);

-- Rolling-24h upload accounting (bytes counted at Leg 2 success).
CREATE TABLE IF NOT EXISTS upload_ledger (
    id             INTEGER PRIMARY KEY,
    run_id         INTEGER NOT NULL REFERENCES runs(id),
    file_id        INTEGER NOT NULL REFERENCES files(id),
    bytes_uploaded INTEGER NOT NULL,
    uploaded_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_time ON upload_ledger(run_id, uploaded_at);

-- Append-only audit + SSE feed.
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY,
    run_id     INTEGER,
    file_id    INTEGER,
    batch_id   INTEGER,
    kind       TEXT NOT NULL,          -- state_change | error | quota_pause | delete_approved | ...
    from_state TEXT,
    to_state   TEXT,
    detail     TEXT,                   -- JSON blob
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_file ON events(file_id, id);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(created_at);
