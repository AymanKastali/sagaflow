CREATE TABLE seat_availability (
    seat_id    TEXT        PRIMARY KEY,
    status     TEXT        NOT NULL,
    hold_id    TEXT        NOT NULL,
    booking_id TEXT        NOT NULL,
    expires_at TIMESTAMPTZ,
    version    INT         NOT NULL
);

CREATE INDEX seat_availability_browse ON seat_availability (status, seat_id);

COMMENT ON TABLE seat_availability IS
    'Derived from the seat streams and safe to drop: every row can be folded again from events. Deliberately stale. A hold decided from this table instead of from the seat stream would be an oversell.';

COMMENT ON COLUMN seat_availability.status IS
    'Stored as text rather than an integer so the table reads in psql, and so renumbering a Go constant cannot silently rewrite what a row says.';

COMMENT ON COLUMN seat_availability.version IS
    'The stream version this row was folded to. Not a lock: it is what stops a re-derivation that read an older stream from overwriting a newer row.';

---- create above / drop below ----

DROP TABLE seat_availability;
