CREATE TABLE process_definitions (
    id VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    name VARCHAR(255),
    definition JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, version)
);

CREATE TABLE process_instances (
    id UUID PRIMARY KEY,
    definition_id VARCHAR(255) NOT NULL,
    definition_version INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    current_tokens JSONB NOT NULL DEFAULT '[]',
    variables JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (definition_id, definition_version) 
        REFERENCES process_definitions(id, version)
);

CREATE INDEX idx_instances_status ON process_instances(status);