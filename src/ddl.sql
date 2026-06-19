CREATE TABLE IF NOT EXISTS companies (
    cik VARCHAR(10) PRIMARY KEY
      CHECK (cik ~ '^[0-9]{10}$'),
    ticker VARCHAR(10) NOT NULL UNIQUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS filings (
    cik VARCHAR(10) NOT NULL REFERENCES companies(cik) ON DELETE CASCADE,
    accession_number VARCHAR(20) NOT NULL
      CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    CONSTRAINT filing_key PRIMARY KEY (cik, accession_number)
);

CREATE TABLE IF NOT EXISTS facts (
    id BIGSERIAL PRIMARY KEY,
    fact_hash VARCHAR(64) NOT NULL UNIQUE,
    cik VARCHAR(10) NOT NULL,
    accession_number VARCHAR(20) NOT NULL,
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
    dimensions JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT fk_filing 
      FOREIGN KEY (cik, accession_number) 
      REFERENCES filings(cik, accession_number) 
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS metrics (
    key VARCHAR(64) PRIMARY KEY,
    display_name VARCHAR(128) NOT NULL UNIQUE,
    format_type VARCHAR(16) NOT NULL DEFAULT 'text'
      CHECK (format_type IN ('percentage','ratio','currency',
                             'number','text'))
);

CREATE TABLE IF NOT EXISTS metric_mappings (
    cik VARCHAR(10) NOT NULL REFERENCES companies(cik) ON DELETE CASCADE,
    metric_key VARCHAR(64) NOT NULL REFERENCES metrics(key)   ON DELETE CASCADE,
    qname VARCHAR(300) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,   -- lower = tried first
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT metric_mapping_key PRIMARY KEY (cik, metric_key, qname)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_facts_by_company ON facts(cik);
CREATE INDEX IF NOT EXISTS idx_facts_by_accession ON facts(accession_number);
CREATE INDEX IF NOT EXISTS idx_facts_qname ON facts(qname);
CREATE INDEX IF NOT EXISTS idx_facts_end_date ON facts(end_date DESC) WHERE end_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_facts_instant_date ON facts(instant_date DESC) WHERE instant_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_facts_filing ON facts(cik, accession_number);
CREATE INDEX IF NOT EXISTS idx_metric_mappings_lookup
  ON metric_mappings(cik, metric_key, priority);
