-- Phase 4 only. Execute after catalog and schema creation have been validated.

CREATE VOLUME IF NOT EXISTS customer_lakehouse.landing.source_files
COMMENT 'Immutable landing volume for original customer sales CSV files';