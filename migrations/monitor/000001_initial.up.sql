CREATE TABLE users (
 id uuid PRIMARY KEY, email text UNIQUE NOT NULL, password_hash text NOT NULL,
 email_verified_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE sessions (
 id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 token_hash text UNIQUE NOT NULL, expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sessions_expiry ON sessions(expires_at);
CREATE TABLE email_verification_tokens (
 id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 token_hash text UNIQUE NOT NULL, expires_at timestamptz NOT NULL, used_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX verification_user ON email_verification_tokens(user_id, created_at DESC);
CREATE TABLE monitors (
 id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id), name text NOT NULL, url text NOT NULL,
 interval_seconds integer NOT NULL CHECK (interval_seconds IN (60,300,600)),
 timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 15),
 expected_status integer NOT NULL CHECK (expected_status BETWEEN 100 AND 599),
 status text NOT NULL DEFAULT 'unknown' CHECK (status IN ('unknown','up','down')),
 is_paused boolean NOT NULL DEFAULT false, next_check_at timestamptz NOT NULL DEFAULT now(),
 last_checked_at timestamptz, last_response_ms bigint,
 consecutive_failures integer NOT NULL DEFAULT 0, consecutive_successes integer NOT NULL DEFAULT 0,
 lease_token uuid, lease_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz
);
CREATE INDEX monitors_due ON monitors(next_check_at) WHERE NOT is_paused AND deleted_at IS NULL;
CREATE INDEX monitors_owner ON monitors(user_id) WHERE deleted_at IS NULL;
CREATE TABLE checks (
 id uuid PRIMARY KEY, monitor_id uuid NOT NULL REFERENCES monitors(id), checked_at timestamptz NOT NULL,
 response_time_ms bigint NOT NULL, status_code integer, success boolean NOT NULL, error_type text
);
CREATE INDEX checks_history ON checks(monitor_id,checked_at DESC);
CREATE INDEX checks_retention ON checks(checked_at);
CREATE TABLE incidents (
 id uuid PRIMARY KEY, monitor_id uuid NOT NULL REFERENCES monitors(id), started_at timestamptz NOT NULL,
 resolved_at timestamptz, cause text NOT NULL
);
CREATE UNIQUE INDEX one_open_incident ON incidents(monitor_id) WHERE resolved_at IS NULL;
CREATE INDEX incidents_history ON incidents(monitor_id,started_at DESC);
CREATE TABLE outbox_events (
 id uuid PRIMARY KEY, sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 topic text NOT NULL, message_key text NOT NULL, payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz,
 attempts integer NOT NULL DEFAULT 0, last_error text
);
CREATE INDEX outbox_pending ON outbox_events(sequence) WHERE published_at IS NULL;
