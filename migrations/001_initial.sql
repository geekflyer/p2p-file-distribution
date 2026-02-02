-- Servers health tracking
CREATE TABLE servers (
    server_ip VARCHAR(45) PRIMARY KEY,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'healthy', -- healthy|unhealthy
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Deployment jobs
CREATE TABLE model_deployment_jobs (
    deployment_job_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gcs_file_path TEXT NOT NULL,
    gcs_manifest_path TEXT NOT NULL,
    total_chunks INT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'created', -- created|in_progress|completed|failed
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-server deployment tasks
CREATE TABLE server_model_deployment_tasks (
    server_ip VARCHAR(45) NOT NULL,
    deployment_job_id UUID NOT NULL REFERENCES model_deployment_jobs(deployment_job_id),
    last_chunk_id_completed INT NOT NULL DEFAULT -1,
    status VARCHAR(20) NOT NULL DEFAULT 'created', -- created|in_progress|completed|failed
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (server_ip, deployment_job_id)
);

-- Index for efficient task lookups by server
CREATE INDEX idx_tasks_server_status ON server_model_deployment_tasks(server_ip, status);

-- Index for job status queries
CREATE INDEX idx_jobs_status ON model_deployment_jobs(status);
