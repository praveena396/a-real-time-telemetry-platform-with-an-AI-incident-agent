-- Telemetry Sentinel schema. Idempotent: safe to run on every start.

CREATE TABLE IF NOT EXISTS readings (
    ts         timestamptz      NOT NULL,
    device_id  text             NOT NULL,
    metric     text             NOT NULL,
    value      double precision NOT NULL,
    fault      text             -- simulator ground truth; NULL = normal
);
CREATE INDEX IF NOT EXISTS readings_device_metric_ts ON readings (device_id, metric, ts DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    ts         timestamptz      NOT NULL,
    reading_ts timestamptz      NOT NULL,
    device_id  text             NOT NULL,
    metric     text             NOT NULL,
    value      double precision NOT NULL,
    score      double precision NOT NULL,
    detector   text             NOT NULL,
    fault      text
);
CREATE INDEX IF NOT EXISTS anomalies_device_ts ON anomalies (device_id, ts DESC);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id   text PRIMARY KEY,
    device_id     text             NOT NULL,
    started       timestamptz      NOT NULL,
    ended         timestamptz      NOT NULL,
    anomaly_count integer          NOT NULL,
    metrics       text[]           NOT NULL,
    max_score     double precision NOT NULL,
    truth         text,
    samples       jsonb            NOT NULL DEFAULT '[]',
    created_at    timestamptz      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS proposals (
    proposal_id text PRIMARY KEY,
    incident_id text             NOT NULL REFERENCES incidents (incident_id),
    device_id   text             NOT NULL,
    diagnosis   text             NOT NULL,
    cause       text             NOT NULL,
    action      text             NOT NULL,
    params      jsonb            NOT NULL DEFAULT '{}',
    confidence  double precision NOT NULL,
    agent       text             NOT NULL,
    status      text             NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'approved', 'rejected', 'executed', 'failed')),
    created_at  timestamptz      NOT NULL DEFAULT now(),
    decided_at  timestamptz,
    decided_by  text,
    note        text
);
CREATE INDEX IF NOT EXISTS proposals_status ON proposals (status, created_at DESC);

-- Append-only record of every proposal, validation failure, approval,
-- rejection and execution.
CREATE TABLE IF NOT EXISTS audit_log (
    id          bigserial PRIMARY KEY,
    ts          timestamptz NOT NULL DEFAULT now(),
    event       text        NOT NULL,
    actor       text        NOT NULL,
    proposal_id text,
    details     jsonb       NOT NULL DEFAULT '{}'
);
