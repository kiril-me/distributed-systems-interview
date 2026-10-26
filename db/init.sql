-- Lab automation: workflow execution.
--
-- The record only moves forward: rows are never deleted, terminal statuses
-- are final, attempts are append-only. See docs/plan.md.

CREATE DATABASE executor_db;

\c executor_db

-- One execution of a workflow.
CREATE TABLE runs (
    id               TEXT        PRIMARY KEY,
    workflow_name    TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending',
    -- Set by cancel. Running steps finish; nothing new is dispatched.
    cancel_requested BOOLEAN     NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    -- Set by a client that may retry createRun: the same key, the same run.
    idempotency_key  TEXT        UNIQUE
);

-- One node of the workflow DAG, for one run. started_at, finished_at, error
-- and data_ref are copied from the step's last attempt.
CREATE TABLE steps (
    run_id      TEXT        NOT NULL REFERENCES runs(id),
    name        TEXT        NOT NULL,
    position    INT         NOT NULL,
    device_id   TEXT        NOT NULL,
    depends_on  TEXT[]      NOT NULL DEFAULT '{}',
    status      TEXT        NOT NULL DEFAULT 'pending',
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error       TEXT,
    data_ref    TEXT,
    PRIMARY KEY (run_id, name)
);

-- One command for one step: dispatched -> completed | failed.
--
-- It is also the outbox: a row is inserted in the same transaction that moves
-- its step to running, and published_at stays NULL until the command is in
-- JetStream. The instrument's columns are filled from the driver's result.
CREATE TABLE attempts (
    job_id       TEXT        PRIMARY KEY,            -- <run_id>/<step>/<n>
    run_id       TEXT        NOT NULL,
    step         TEXT        NOT NULL,
    n            INT         NOT NULL,
    device_id    TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'dispatched',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    execution_id TEXT,
    outcome      TEXT,
    error        TEXT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    data_ref     TEXT,
    data_sha256  TEXT,
    -- When the job id went into the REVOKED bucket, because the run was
    -- cancelled while the command was queued.
    revoked_at   TIMESTAMPTZ,
    UNIQUE (run_id, step, n),
    FOREIGN KEY (run_id, step) REFERENCES steps(run_id, name)
);

CREATE INDEX attempts_unpublished ON attempts (created_at) WHERE published_at IS NULL;
