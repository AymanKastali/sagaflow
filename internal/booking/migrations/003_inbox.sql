CREATE TABLE inbox (
    consumer   TEXT NOT NULL,
    source     TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    handled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, source, event_id)
);

COMMENT ON TABLE inbox IS
    'Consume-once deduplication. (source, event_id) is CloudEvents ce_source + ce_id, which the spec guarantees unique. consumer is in the key because several consumers in one service read the same message and must deduplicate independently.';

---- create above / drop below ----

DROP TABLE inbox;
