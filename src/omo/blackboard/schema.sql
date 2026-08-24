-- projects/omo/src/omo/blackboard/schema.sql
-- 架构因果黑板 (Architecture Causal Blackboard) Schema v1

-- 1. 节点表：记录项目、模块、文件、规则、BOS服务、Cron任务
CREATE TABLE IF NOT EXISTS graph_nodes (
    id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,          -- 'gov_ssot', 'knowledge_mos', 'compute_fabric', 'ingress_lifeos'
    node_type TEXT NOT NULL,       -- 'project', 'rule', 'bos_service', 'cron_job', 'ssot_file'
    path_or_uri TEXT,
    metadata JSON DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 边表：依赖关系、包含关系、监控关系、暴露关系
CREATE TABLE IF NOT EXISTS graph_edges (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,   -- 'depends_on', 'monitored_by', 'owns', 'exposes'
    metadata JSON DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, target_id, relation_type)
);

-- 3. 物理测量事实表：记录由真实 CLI/探针产生的收据事实 (Proof of Execution)
CREATE TABLE IF NOT EXISTS measurement_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,        -- 'devil:gov', 'builder:mos', 'meta-doctor', etc.
    fact_type TEXT NOT NULL,       -- 'health_check', 'drift_scan', 'chaos_probe', 'vitality'
    exit_code INTEGER NOT NULL,
    proof_hash TEXT NOT NULL,      -- sha256 of stdout/evidence
    execution_ms INTEGER NOT NULL,
    verdict TEXT NOT NULL,         -- 'pass', 'fail', 'corroded'
    details JSON DEFAULT '{}',
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(node_id) REFERENCES graph_nodes(id)
);

-- 4. 领域守卫租约与心跳表：记录当前活跃 Agent 的认领与活性
CREATE TABLE IF NOT EXISTS domain_leases (
    domain TEXT NOT NULL,
    role TEXT NOT NULL,            -- 'builder', 'devil', 'sage', 'keeper'
    agent_id TEXT NOT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON DEFAULT '{}',
    PRIMARY KEY (domain, role)
);

-- 索引以加速依赖查询与事实检索
CREATE INDEX IF NOT EXISTS idx_edges_source ON graph_edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON graph_edges(target_id);
CREATE INDEX IF NOT EXISTS idx_facts_node ON measurement_facts(node_id);
CREATE INDEX IF NOT EXISTS idx_facts_verdict ON measurement_facts(verdict);
CREATE INDEX IF NOT EXISTS idx_nodes_domain ON graph_nodes(domain);
