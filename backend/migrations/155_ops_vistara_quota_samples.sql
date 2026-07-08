CREATE TABLE IF NOT EXISTS ops_vistara_quota_samples (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    used_quota BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS ops_vistara_quota_samples_created_at_idx
    ON ops_vistara_quota_samples (created_at DESC);
