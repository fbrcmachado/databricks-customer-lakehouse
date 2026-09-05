-- Phase 4 only. Execute after catalog creation has been validated.

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.landing
COMMENT 'Landing zone for immutable source files';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.bronze
COMMENT 'Raw ingestion layer with lineage metadata';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.silver
COMMENT 'Canonical, typed and validated business data';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.quarantine
COMMENT 'Rejected rows and data quality evidence';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.gold_bi
COMMENT 'Curated analytical serving layer for BI';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.gold_ml
COMMENT 'Feature and machine learning result layer';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.observability
COMMENT 'Operational metadata, pipeline history and quality metrics';

CREATE SCHEMA IF NOT EXISTS customer_lakehouse.governance
COMMENT 'Governance metadata, controls, glossary and lineage declarations';