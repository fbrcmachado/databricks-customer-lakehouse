from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "databricks.yml",
    "pyproject.toml",
    "uv.lock",
    "config/data_contract.yaml",
    "config/observability_contract.yaml",
    "config/gold_contract.yaml",
    "config/ml_contract.yaml",
    "config/governance_contract.yaml",
    "config/orchestration_contract.yaml",
    "config/serving_contract.yaml",
    "config/hardening_contract.yaml",
    "notebooks/01_bronze_ingestion.py",
    "notebooks/02_silver_transformation.py",
    "notebooks/03_observability_metrics.py",
    "notebooks/04_gold_bi.py",
    "notebooks/05_gold_ml.py",
    "notebooks/06_governance_as_code.py",
    "notebooks/07_e2e_validation.py",
    "resources/bronze_ingestion.job.yml",
    "resources/silver_transformation.job.yml",
    "resources/observability_metrics.job.yml",
    "resources/gold_bi.job.yml",
    "resources/gold_ml.job.yml",
    "resources/governance.job.yml",
    "resources/e2e_pipeline.job.yml",
    "scripts/configure_sql_serving.ps1",
    "sql/01_create_powerbi_serving_views.sql",
    "sql/02_validate_powerbi_serving.sql",
    ".github/workflows/ci.yml",
    ".github/workflows/deploy-dev.yml",
    ".gitattributes",
]

FORBIDDEN_SUFFIXES = {
    ".pbix",
    ".pfx",
    ".p12",
    ".key",
}

SECRET_PATTERNS = [
    (
        "Databricks PAT",
        re.compile(r"\bdapi[a-zA-Z0-9]{20,}\b"),
    ),
    (
        "Private key",
        re.compile(
            r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
        ),
    ),
]

TEXT_SUFFIXES = {
    ".py",
    ".ps1",
    ".sql",
    ".yml",
    ".yaml",
    ".md",
    ".toml",
    ".txt",
    ".dax",
    ".json",
}


def git_tracked_files() -> list[Path]:
    """
    Return all files currently tracked by Git.

    This list is used for policies that specifically apply to versioned
    artifacts, such as blocking PBIX and private-key files from source control.
    """
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )

    values = [
        value
        for value in result.stdout.decode("utf-8").split("\0")
        if value
    ]

    return [ROOT / value for value in values]


def repository_source_files() -> list[Path]:
    """
    Return text-based source files from the current working tree.

    Unlike git_tracked_files(), this intentionally includes untracked files.
    This prevents newly created source files from bypassing secret scanning
    before they are added to Git.
    """
    excluded_parts = {
        ".git",
        ".venv",
        ".databricks",
        ".pytest_cache",
        "__pycache__",
    }

    files: list[Path] = []

    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue

        if any(part in excluded_parts for part in path.parts):
            continue

        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue

        files.append(path)

    return files


def fail(messages: list[str]) -> None:
    """
    Print all repository-integrity failures and terminate with exit code 1.
    """
    print("REPOSITORY INTEGRITY: FAIL")

    for message in messages:
        print(f" - {message}")

    sys.exit(1)


def main() -> None:
    errors: list[str] = []

    # ---------------------------------------------------------
    # Required project structure
    # ---------------------------------------------------------

    for relative_path in REQUIRED_FILES:
        if not (ROOT / relative_path).is_file():
            errors.append(
                f"Missing required file: {relative_path}"
            )

    # ---------------------------------------------------------
    # Git-tracked artifact policies
    # ---------------------------------------------------------

    tracked = git_tracked_files()

    for path in tracked:
        if path.suffix.lower() in FORBIDDEN_SUFFIXES:
            errors.append(
                "Forbidden tracked artifact: "
                f"{path.relative_to(ROOT)}"
            )

    # ---------------------------------------------------------
    # Working-tree secret scanning
    # ---------------------------------------------------------

    source_files = repository_source_files()

    for path in source_files:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        for label, pattern in SECRET_PATTERNS:
            if pattern.search(text):
                errors.append(
                    f"Potential literal {label}: "
                    f"{path.relative_to(ROOT)}"
                )

    # ---------------------------------------------------------
    # Final gate
    # ---------------------------------------------------------

    if errors:
        fail(errors)

    print("REPOSITORY INTEGRITY: PASS")
    print(f"Required files: {len(REQUIRED_FILES)}")
    print(f"Tracked files inspected: {len(tracked)}")
    print(
        "Repository source files inspected: "
        f"{len(source_files)}"
    )
    print(
        "Forbidden tracked PBIX/private-key artifacts: 0"
    )
    print(
        "Literal high-confidence secrets detected: 0"
    )


if __name__ == "__main__":
    main()