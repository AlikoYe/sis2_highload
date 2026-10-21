CREATE TABLE IF NOT EXISTS deliveries (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID        NOT NULL,
    event_type   TEXT        NOT NULL,
    user_id      BIGINT      NOT NULL,
    channel      TEXT        NOT NULL CHECK (channel IN ('PUSH','EMAIL')),
    status       TEXT        NOT NULL DEFAULT 'QUEUED'
                 CHECK (status IN ('QUEUED','SENDING','RETRY','SENT','FAILED')),
    attempts     INT         NOT NULL DEFAULT 0,
    payload      JSONB       NOT NULL DEFAULT '{}',
    occurred_at  TIMESTAMPTZ NOT NULL,
    accepted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, channel)
);
