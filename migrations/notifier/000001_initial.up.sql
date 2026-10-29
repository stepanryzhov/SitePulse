CREATE TABLE processed_events (
 event_id uuid PRIMARY KEY, event_type text NOT NULL, processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE notifications (
 id uuid PRIMARY KEY, event_id uuid UNIQUE NOT NULL REFERENCES processed_events(event_id),
 recipient_email text NOT NULL, subject text NOT NULL, body text NOT NULL,
 status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sending','retry_wait','sent','failed')),
 attempts integer NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL DEFAULT now(),
 lease_token uuid, lease_until timestamptz, last_error text,
 created_at timestamptz NOT NULL DEFAULT now(), sent_at timestamptz
);
CREATE INDEX notifications_due ON notifications(next_attempt_at) WHERE status IN ('pending','retry_wait','sending');
CREATE TABLE notification_attempts (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
 attempt integer NOT NULL, attempted_at timestamptz NOT NULL DEFAULT now(), error text, success boolean NOT NULL
);
CREATE TABLE rejected_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, event_id uuid, topic text NOT NULL,
 partition integer NOT NULL, "offset" bigint NOT NULL, reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(topic,partition,"offset")
);
