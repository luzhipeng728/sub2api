ALTER TABLE ops_system_metrics
    ADD COLUMN IF NOT EXISTS heap_alloc_mb BIGINT,
    ADD COLUMN IF NOT EXISTS heap_sys_mb BIGINT,
    ADD COLUMN IF NOT EXISTS gc_count INT,
    ADD COLUMN IF NOT EXISTS ws_active_conns INT,
    ADD COLUMN IF NOT EXISTS ws_handshake_total BIGINT;
