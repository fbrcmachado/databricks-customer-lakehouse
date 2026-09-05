"""Project-level names used by provisioning and pipeline code."""

CATALOG = "customer_lakehouse"

SCHEMAS = (
    "landing",
    "bronze",
    "silver",
    "quarantine",
    "gold_bi",
    "gold_ml",
    "observability",
    "governance",
)

SOURCE_VOLUME = "source_files"