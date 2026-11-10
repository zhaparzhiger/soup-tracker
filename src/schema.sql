-- Idempotent: runs at every start under an advisory lock.

CREATE TABLE IF NOT EXISTS projects (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Only the SHA-256 of a key is stored. Keys are 256-bit random, so a plain hash is enough.
CREATE TABLE IF NOT EXISTS api_keys (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  key_hash    bytea NOT NULL UNIQUE,
  prefix      text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at  timestamptz
);

-- last_seq is the head of the run's event log. Invariant: last_seq = count(points of this run).
CREATE TABLE IF NOT EXISTS runs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  last_seq    bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  conflicts   bigint NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS runs_project_created ON runs (project_id, created_at DESC, id);

-- seq: delivery order (server, gapless per run). step: display order (client).
-- UNIQUE (run_id, key, step) is the idempotency key and the last line of defence against duplicates.
CREATE TABLE IF NOT EXISTS points (
  run_id      uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  seq         bigint NOT NULL CHECK (seq >= 1),
  key         text NOT NULL,
  step        bigint NOT NULL CHECK (step >= 0),
  value       double precision NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq),
  UNIQUE (run_id, key, step)
);
