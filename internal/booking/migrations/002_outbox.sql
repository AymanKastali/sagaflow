CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    headers      JSONB NOT NULL,
    published_at TIMESTAMPTZ
);

-- Partial index: the poller only ever asks for unpublished rows, so the index
-- stays the size of the backlog rather than the size of history.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

COMMENT ON COLUMN outbox.key IS
    'Kafka partition key — always the stream id, which is what preserves per-stream ordering.';

---- create above / drop below ----

DROP TABLE outbox;
