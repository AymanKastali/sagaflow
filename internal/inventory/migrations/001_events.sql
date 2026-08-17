CREATE TABLE events (
    global_seq  BIGSERIAL   PRIMARY KEY,
    stream_id   TEXT        NOT NULL,
    version     INT         NOT NULL,
    type        TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    meta        JSONB       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, version)
);

COMMENT ON COLUMN events.global_seq IS
    'Diagnostic and replay tooling only. BIGSERIAL commits out of order, so no consumer may track a cursor over this column.';

---- create above / drop below ----

DROP TABLE events;
