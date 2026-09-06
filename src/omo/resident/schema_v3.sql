-- schema_v3.sql — Agent 认知帧持久化 (BET-Y1Q4-T10-132)
-- WAL 模式: 读写不阻塞, 崩溃安全 (commit 前的帧在 power-loss 后可恢复)
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS hydration_frames (
    agent_id   TEXT PRIMARY KEY,
    frame      TEXT NOT NULL,            -- JSON-serialized cognitive frame
    state      TEXT NOT NULL DEFAULT 'DORMANT'
               CHECK (state IN ('DORMANT','HYDRATING','ACTIVE','DEHYDRATING')),
    updated_at REAL NOT NULL             -- epoch seconds
);

CREATE INDEX IF NOT EXISTS idx_hydration_state
    ON hydration_frames (state);
