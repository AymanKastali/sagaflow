CREATE TABLE timers (
    id       BIGSERIAL PRIMARY KEY,
    fire_at  TIMESTAMPTZ NOT NULL,
    subject  TEXT NOT NULL,
    token    TEXT NOT NULL,
    fired_at TIMESTAMPTZ
);

-- Partial index: the scheduler only ever asks for unfired rows, so the index
-- stays the size of the pending set rather than the size of history.
CREATE INDEX timers_due ON timers (fire_at) WHERE fired_at IS NULL;

COMMENT ON COLUMN timers.subject IS
    'The stream this timer is about — a seat id here, a saga id in booking.';
COMMENT ON COLUMN timers.token IS
    'What the subject looked like when the timer was set. A handler compares it against the stream it just loaded; a mismatch means the world moved on and this timer is stale.';
COMMENT ON COLUMN timers.fire_at IS
    'Application-supplied, never DEFAULT now(), so a test controls a deadline with a value instead of by waiting.';

---- create above / drop below ----

DROP TABLE timers;
