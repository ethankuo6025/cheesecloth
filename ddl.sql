CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    cik VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS facts (
    id BIGSERIAL PRIMARY KEY,
    fact_hash VARCHAR(32) NOT NULL UNIQUE,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    accession_number VARCHAR(25) NOT NULL,
    qname VARCHAR(300) NOT NULL,
    namespace VARCHAR(512) NOT NULL,
    local_name VARCHAR(256) NOT NULL,
    period_type VARCHAR(10) NOT NULL,
    value TEXT,
    instant_date DATE,
    start_date DATE,
    end_date DATE,
    unit VARCHAR(100),
    decimals INTEGER,
    precision INTEGER,
    dimensions JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_facts_company_id ON facts(company_id);
CREATE INDEX IF NOT EXISTS idx_facts_accession ON facts(accession_number);
CREATE INDEX IF NOT EXISTS idx_facts_local_name ON facts(local_name);
CREATE INDEX IF NOT EXISTS idx_facts_qname ON facts(qname);
CREATE INDEX IF NOT EXISTS idx_facts_end_date ON facts(end_date DESC) WHERE end_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_facts_instant_date ON facts(instant_date DESC) WHERE instant_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_facts_dimensions ON facts USING GIN(dimensions);