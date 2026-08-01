"""
Phase 13A — Verified Research Dataset Builder

Builds a research-ready dataset using only live scanner cycles that passed
Phase 12E/12E.1 and are marked:

    scanner_cycle_validation.research_eligible = 1

Included cycle statuses:
    VERIFIED
    VERIFIED_WITH_WARNING

Excluded:
    WARNING
    INVALID
    LEGACY
    VALIDATION_ERROR
    demo_paper_trades
    unvalidated cycles

Outputs:
    research/verified_dataset/verified_live_features.csv
    research/verified_dataset/verified_live_features.jsonl
    research/verified_dataset/verified_cycles.csv
    research/verified_dataset/verified_dataset_manifest.json
    research/verified_dataset/verified_dataset_validation.json

This module is read-only with respect to scanner and trading tables.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "13A.1.0"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE_PATH = PROJECT_ROOT / "database" / "trades.db"
DEFAULT_OUTPUT_DIRECTORY = PROJECT_ROOT / "research" / "verified_dataset"

FEATURES_CSV = "verified_live_features.csv"
FEATURES_JSONL = "verified_live_features.jsonl"
CYCLES_CSV = "verified_cycles.csv"
MANIFEST_JSON = "verified_dataset_manifest.json"
VALIDATION_JSON = "verified_dataset_validation.json"


class VerifiedDatasetError(RuntimeError):
    """Base exception for Phase 13A failures."""


@dataclass(frozen=True, slots=True)
class BuilderConfiguration:
    database_path: Path = DEFAULT_DATABASE_PATH
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True
    minimum_verified_cycles: int = 1
    minimum_verified_rows: int = 1
    include_statuses: tuple[str, ...] = (
        "VERIFIED",
        "VERIFIED_WITH_WARNING",
    )

    def validate(self) -> None:
        if not str(self.database_path).strip():
            raise VerifiedDatasetError(
                "database_path cannot be empty."
            )

        if not str(self.output_directory).strip():
            raise VerifiedDatasetError(
                "output_directory cannot be empty."
            )

        if self.minimum_verified_cycles <= 0:
            raise VerifiedDatasetError(
                "minimum_verified_cycles must be positive."
            )

        if self.minimum_verified_rows <= 0:
            raise VerifiedDatasetError(
                "minimum_verified_rows must be positive."
            )

        if not self.include_statuses:
            raise VerifiedDatasetError(
                "include_statuses cannot be empty."
            )


@dataclass(frozen=True, slots=True)
class ValidationCheck:
    name: str
    passed: bool
    observed: Any
    expected: Any
    details: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    generated_at: str
    schema_version: str
    database_path: str

    verified_rows: int
    verified_cycles: int
    unique_assets: int
    unique_tokens: int

    verified_status_rows: int
    verified_with_warning_status_rows: int

    successful_quotes: int
    quote_errors: int
    eligible_observations: int
    profitable_observations: int

    average_net_profit_usd: float
    best_net_profit_usd: float
    worst_net_profit_usd: float
    average_total_cost_bps: float
    average_quote_latency_ms: float
    average_quality_score: float

    excluded_unvalidated_cycles: int
    excluded_noneligible_cycles: int
    excluded_legacy_cycles: int
    excluded_invalid_cycles: int
    excluded_warning_cycles: int
    excluded_validation_error_cycles: int

    first_scan_time: str | None
    last_scan_time: str | None

    valid: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        path,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute(
        "PRAGMA busy_timeout=30000"
    )
    return connection


def _table_exists(
    connection: sqlite3.Connection,
    table_name: str,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()

    return row is not None


def _safe_float(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0

    return numeric if math.isfinite(numeric) else 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _row_to_dict(
    row: sqlite3.Row,
) -> dict[str, Any]:
    return {
        key: row[key]
        for key in row.keys()
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


class VerifiedResearchDatasetBuilder:
    def __init__(
        self,
        configuration: BuilderConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or BuilderConfiguration()
        )
        self.configuration.validate()

    def build(
        self,
    ) -> tuple[
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
        DatasetSummary,
        tuple[ValidationCheck, ...],
    ]:
        connection = _connect(
            self.configuration.database_path
        )

        try:
            self._validate_source_tables(
                connection
            )

            feature_rows = self._load_verified_features(
                connection
            )

            cycle_rows = self._load_verified_cycles(
                connection
            )

            summary = self._build_summary(
                connection,
                feature_rows,
                cycle_rows,
            )

            checks = self._validate_dataset(
                feature_rows,
                cycle_rows,
                summary,
            )

            valid = all(
                check.passed
                for check in checks
            )

            summary = DatasetSummary(
                **{
                    **summary.to_dict(),
                    "valid": valid,
                }
            )

            return (
                tuple(feature_rows),
                tuple(cycle_rows),
                summary,
                tuple(checks),
            )

        finally:
            connection.close()

    def _validate_source_tables(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        required_tables = (
            "live_scanner_features",
            "live_scanner_cycles",
            "scanner_cycle_validation",
        )

        missing = [
            table
            for table in required_tables
            if not _table_exists(
                connection,
                table,
            )
        ]

        if missing:
            raise VerifiedDatasetError(
                "Missing required tables: "
                + ", ".join(missing)
            )

    def _load_verified_features(
        self,
        connection: sqlite3.Connection,
    ) -> list[dict[str, Any]]:
        statuses = tuple(
            status.strip().upper()
            for status in self.configuration.include_statuses
        )

        placeholders = ",".join(
            "?"
            for _ in statuses
        )

        rows = connection.execute(
            f"""
            SELECT
                f.*,
                v.status AS validation_status,
                v.quality_score AS validation_quality_score,
                v.errors AS validation_errors,
                v.warnings AS validation_warnings,
                v.validated_at,
                v.validator_version,
                v.research_eligible
            FROM live_scanner_features AS f
            INNER JOIN scanner_cycle_validation AS v
                ON v.cycle_id = f.cycle_id
               AND v.cycle_number = f.cycle_number
            WHERE v.research_eligible = 1
              AND UPPER(v.status) IN ({placeholders})
            ORDER BY
                f.cycle_number,
                f.cycle_id,
                f.cycle_position,
                f.id
            """,
            statuses,
        ).fetchall()

        return [
            _row_to_dict(row)
            for row in rows
        ]

    def _load_verified_cycles(
        self,
        connection: sqlite3.Connection,
    ) -> list[dict[str, Any]]:
        statuses = tuple(
            status.strip().upper()
            for status in self.configuration.include_statuses
        )

        placeholders = ",".join(
            "?"
            for _ in statuses
        )

        rows = connection.execute(
            f"""
            SELECT
                c.*,
                v.status AS validation_status,
                v.quality_score AS validation_quality_score,
                v.errors AS validation_errors,
                v.warnings AS validation_warnings,
                v.validated_at,
                v.validator_version,
                v.research_eligible
            FROM live_scanner_cycles AS c
            INNER JOIN scanner_cycle_validation AS v
                ON v.cycle_id = c.cycle_id
               AND v.cycle_number = c.cycle_number
            WHERE v.research_eligible = 1
              AND UPPER(v.status) IN ({placeholders})
            ORDER BY
                c.cycle_number,
                c.cycle_id,
                c.id
            """,
            statuses,
        ).fetchall()

        return [
            _row_to_dict(row)
            for row in rows
        ]

    def _build_summary(
        self,
        connection: sqlite3.Connection,
        feature_rows: Sequence[Mapping[str, Any]],
        cycle_rows: Sequence[Mapping[str, Any]],
    ) -> DatasetSummary:
        verified_status_rows = sum(
            str(
                row.get(
                    "validation_status",
                    "",
                )
            ).upper()
            == "VERIFIED"
            for row in feature_rows
        )

        verified_with_warning_status_rows = sum(
            str(
                row.get(
                    "validation_status",
                    "",
                )
            ).upper()
            == "VERIFIED_WITH_WARNING"
            for row in feature_rows
        )

        successful_rows = [
            row
            for row in feature_rows
            if bool(
                _safe_int(
                    row.get(
                        "quote_successful"
                    )
                )
            )
        ]

        profits = [
            _safe_float(
                row.get("net_profit_usd")
            )
            for row in successful_rows
        ]

        costs = [
            _safe_float(
                row.get("total_cost_bps")
            )
            for row in successful_rows
        ]

        latencies = [
            _safe_float(
                row.get("quote_latency_ms")
            )
            for row in successful_rows
        ]

        quality_scores = [
            _safe_float(
                row.get(
                    "validation_quality_score"
                )
            )
            for row in feature_rows
        ]

        validation_counts = {
            str(
                row["status"]
            ).upper(): _safe_int(
                row["count"]
            )
            for row in connection.execute(
                """
                SELECT
                    status,
                    COUNT(*) AS count
                FROM scanner_cycle_validation
                GROUP BY status
                """
            ).fetchall()
        }

        all_live_cycles = _safe_int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM live_scanner_cycles
                """
            ).fetchone()[0]
        )

        validated_cycle_ids = {
            (
                str(row["cycle_id"]),
                _safe_int(
                    row["cycle_number"]
                ),
            )
            for row in connection.execute(
                """
                SELECT cycle_id, cycle_number
                FROM scanner_cycle_validation
                """
            ).fetchall()
        }

        all_cycle_ids = {
            (
                str(row["cycle_id"]),
                _safe_int(
                    row["cycle_number"]
                ),
            )
            for row in connection.execute(
                """
                SELECT cycle_id, cycle_number
                FROM live_scanner_cycles
                """
            ).fetchall()
        }

        excluded_unvalidated = len(
            all_cycle_ids
            - validated_cycle_ids
        )

        first_scan_time = (
            min(
                str(row["scan_time"])
                for row in feature_rows
                if row.get("scan_time")
            )
            if feature_rows
            else None
        )

        last_scan_time = (
            max(
                str(row["scan_time"])
                for row in feature_rows
                if row.get("scan_time")
            )
            if feature_rows
            else None
        )

        return DatasetSummary(
            generated_at=utc_now_text(),
            schema_version=SCHEMA_VERSION,
            database_path=str(
                self.configuration.database_path
            ),
            verified_rows=len(feature_rows),
            verified_cycles=len(cycle_rows),
            unique_assets=len(
                {
                    str(
                        row.get("asset_key")
                    )
                    for row in feature_rows
                    if str(
                        row.get("asset_key") or ""
                    ).strip()
                }
            ),
            unique_tokens=len(
                {
                    str(
                        row.get("token")
                    )
                    for row in feature_rows
                    if str(
                        row.get("token") or ""
                    ).strip()
                }
            ),
            verified_status_rows=(
                verified_status_rows
            ),
            verified_with_warning_status_rows=(
                verified_with_warning_status_rows
            ),
            successful_quotes=len(
                successful_rows
            ),
            quote_errors=(
                len(feature_rows)
                - len(successful_rows)
            ),
            eligible_observations=sum(
                bool(
                    _safe_int(
                        row.get("eligible")
                    )
                )
                for row in feature_rows
            ),
            profitable_observations=sum(
                _safe_float(
                    row.get("net_profit_usd")
                ) > 0
                and bool(
                    _safe_int(
                        row.get(
                            "quote_successful"
                        )
                    )
                )
                for row in feature_rows
            ),
            average_net_profit_usd=(
                sum(profits) / len(profits)
                if profits
                else 0.0
            ),
            best_net_profit_usd=(
                max(profits)
                if profits
                else 0.0
            ),
            worst_net_profit_usd=(
                min(profits)
                if profits
                else 0.0
            ),
            average_total_cost_bps=(
                sum(costs) / len(costs)
                if costs
                else 0.0
            ),
            average_quote_latency_ms=(
                sum(latencies) / len(latencies)
                if latencies
                else 0.0
            ),
            average_quality_score=(
                sum(quality_scores)
                / len(quality_scores)
                if quality_scores
                else 0.0
            ),
            excluded_unvalidated_cycles=(
                excluded_unvalidated
            ),
            excluded_noneligible_cycles=max(
                0,
                all_live_cycles
                - len(cycle_rows)
                - excluded_unvalidated,
            ),
            excluded_legacy_cycles=(
                validation_counts.get(
                    "LEGACY",
                    0,
                )
            ),
            excluded_invalid_cycles=(
                validation_counts.get(
                    "INVALID",
                    0,
                )
            ),
            excluded_warning_cycles=(
                validation_counts.get(
                    "WARNING",
                    0,
                )
            ),
            excluded_validation_error_cycles=(
                validation_counts.get(
                    "VALIDATION_ERROR",
                    0,
                )
            ),
            first_scan_time=first_scan_time,
            last_scan_time=last_scan_time,
            valid=False,
        )

    def _validate_dataset(
        self,
        feature_rows: Sequence[Mapping[str, Any]],
        cycle_rows: Sequence[Mapping[str, Any]],
        summary: DatasetSummary,
    ) -> list[ValidationCheck]:
        checks: list[ValidationCheck] = []

        checks.append(
            ValidationCheck(
                name="minimum_verified_rows",
                passed=(
                    len(feature_rows)
                    >= self.configuration.minimum_verified_rows
                ),
                observed=len(feature_rows),
                expected=(
                    f">= {self.configuration.minimum_verified_rows}"
                ),
                details="Verified feature row minimum.",
            )
        )

        checks.append(
            ValidationCheck(
                name="minimum_verified_cycles",
                passed=(
                    len(cycle_rows)
                    >= self.configuration.minimum_verified_cycles
                ),
                observed=len(cycle_rows),
                expected=(
                    f">= {self.configuration.minimum_verified_cycles}"
                ),
                details="Verified cycle minimum.",
            )
        )

        cycle_identity = {
            (
                str(row.get("cycle_id")),
                _safe_int(
                    row.get("cycle_number")
                ),
            )
            for row in cycle_rows
        }

        feature_cycle_identity = {
            (
                str(row.get("cycle_id")),
                _safe_int(
                    row.get("cycle_number")
                ),
            )
            for row in feature_rows
        }

        checks.append(
            ValidationCheck(
                name="cycle_identity_reconciliation",
                passed=(
                    feature_cycle_identity
                    == cycle_identity
                ),
                observed=sorted(
                    feature_cycle_identity
                ),
                expected=sorted(
                    cycle_identity
                ),
                details=(
                    "Feature cycles must exactly match "
                    "verified cycle-summary rows."
                ),
            )
        )

        duplicate_keys: set[
            tuple[Any, ...]
        ] = set()
        duplicate_count = 0

        for row in feature_rows:
            key = (
                row.get("cycle_id"),
                row.get("cycle_number"),
                row.get("source_event_id"),
                row.get("token"),
                row.get("scan_time"),
            )

            if key in duplicate_keys:
                duplicate_count += 1
            else:
                duplicate_keys.add(key)

        checks.append(
            ValidationCheck(
                name="no_duplicate_observations",
                passed=duplicate_count == 0,
                observed=duplicate_count,
                expected=0,
                details=(
                    "Duplicate verified observations "
                    "are not permitted."
                ),
            )
        )

        invalid_status_rows = [
            row
            for row in feature_rows
            if str(
                row.get(
                    "validation_status",
                    "",
                )
            ).upper()
            not in {
                status.upper()
                for status
                in self.configuration.include_statuses
            }
        ]

        checks.append(
            ValidationCheck(
                name="status_allowlist",
                passed=(
                    not invalid_status_rows
                ),
                observed=len(
                    invalid_status_rows
                ),
                expected=0,
                details=(
                    "Every row must originate from an "
                    "allowed validation status."
                ),
            )
        )

        noneligible_rows = [
            row
            for row in feature_rows
            if not bool(
                _safe_int(
                    row.get(
                        "research_eligible"
                    )
                )
            )
        ]

        checks.append(
            ValidationCheck(
                name="research_eligibility",
                passed=not noneligible_rows,
                observed=len(noneligible_rows),
                expected=0,
                details=(
                    "Every exported row must have "
                    "research_eligible=1."
                ),
            )
        )

        demo_fields_present = any(
            str(
                row.get("decision") or ""
            ).upper()
            == "DEMO_EXECUTE"
            or str(
                row.get("token") or ""
            ).upper().startswith("DEMO")
            for row in feature_rows
        )

        checks.append(
            ValidationCheck(
                name="demo_trade_exclusion",
                passed=(
                    not demo_fields_present
                ),
                observed=demo_fields_present,
                expected=False,
                details=(
                    "Synthetic demo trades must never "
                    "enter verified research exports."
                ),
            )
        )

        blank_asset_keys = sum(
            not str(
                row.get("asset_key")
                or ""
            ).strip()
            for row in feature_rows
        )

        checks.append(
            ValidationCheck(
                name="asset_key_completeness",
                passed=blank_asset_keys == 0,
                observed=blank_asset_keys,
                expected=0,
                details=(
                    "Verified rows must have a nonblank asset_key."
                ),
            )
        )

        checks.append(
            ValidationCheck(
                name="summary_row_count",
                passed=(
                    summary.verified_rows
                    == len(feature_rows)
                ),
                observed=summary.verified_rows,
                expected=len(feature_rows),
                details="Summary must reconcile to exported rows.",
            )
        )

        return checks


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    if not rows:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    fieldnames: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    key: (
                        json.dumps(
                            value,
                            ensure_ascii=False,
                        )
                        if isinstance(
                            value,
                            (dict, list, tuple),
                        )
                        else value
                    )
                    for key, value in row.items()
                }
            )


def _write_jsonl(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    with path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    dict(row),
                    ensure_ascii=False,
                    default=str,
                    separators=(",", ":"),
                )
            )
            handle.write("\n")


def export_verified_dataset(
    *,
    feature_rows: Sequence[Mapping[str, Any]],
    cycle_rows: Sequence[Mapping[str, Any]],
    summary: DatasetSummary,
    checks: Sequence[ValidationCheck],
    configuration: BuilderConfiguration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    features_csv = output / FEATURES_CSV
    features_jsonl = output / FEATURES_JSONL
    cycles_csv = output / CYCLES_CSV
    manifest_json = output / MANIFEST_JSON
    validation_json = output / VALIDATION_JSON

    destinations = (
        features_csv,
        features_jsonl,
        cycles_csv,
        manifest_json,
        validation_json,
    )

    if not configuration.overwrite:
        existing = [
            path
            for path in destinations
            if path.exists()
        ]

        if existing:
            raise VerifiedDatasetError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    _write_csv(
        features_csv,
        feature_rows,
    )
    _write_jsonl(
        features_jsonl,
        feature_rows,
    )
    _write_csv(
        cycles_csv,
        cycle_rows,
    )

    validation_json.write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": summary.to_dict(),
                "checks": [
                    check.to_dict()
                    for check in checks
                ],
                "valid": summary.valid,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    file_metadata = {}

    for path, rows in (
        (features_csv, len(feature_rows)),
        (features_jsonl, len(feature_rows)),
        (cycles_csv, len(cycle_rows)),
        (validation_json, len(checks)),
    ):
        file_metadata[path.name] = {
            "path": str(path),
            "rows": rows,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }

    manifest_json.write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": utc_now_text(),
                "summary": summary.to_dict(),
                "source_policy": {
                    "source_table": (
                        "live_scanner_features"
                    ),
                    "validation_table": (
                        "scanner_cycle_validation"
                    ),
                    "required_research_eligible": 1,
                    "allowed_statuses": list(
                        configuration.include_statuses
                    ),
                    "demo_tables_included": False,
                    "legacy_cycles_included": False,
                    "unvalidated_cycles_included": False,
                },
                "files": file_metadata,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return destinations


def run_builder(
    configuration: BuilderConfiguration | None = None,
) -> DatasetSummary:
    active = (
        configuration
        or BuilderConfiguration()
    )

    (
        feature_rows,
        cycle_rows,
        summary,
        checks,
    ) = VerifiedResearchDatasetBuilder(
        active
    ).build()

    export_verified_dataset(
        feature_rows=feature_rows,
        cycle_rows=cycle_rows,
        summary=summary,
        checks=checks,
        configuration=active,
    )

    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Phase 13A verified live research dataset."
        )
    )

    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
    )

    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )

    parser.add_argument(
        "--minimum-cycles",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--minimum-rows",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--no-overwrite",
        action="store_true",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = _parser().parse_args(
        argv
    )

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    configuration = BuilderConfiguration(
        database_path=Path(
            args.database
        ),
        output_directory=Path(
            args.output_directory
        ),
        overwrite=(
            not args.no_overwrite
        ),
        minimum_verified_cycles=(
            args.minimum_cycles
        ),
        minimum_verified_rows=(
            args.minimum_rows
        ),
    )

    try:
        (
            feature_rows,
            cycle_rows,
            summary,
            checks,
        ) = VerifiedResearchDatasetBuilder(
            configuration
        ).build()

        output_paths = export_verified_dataset(
            feature_rows=feature_rows,
            cycle_rows=cycle_rows,
            summary=summary,
            checks=checks,
            configuration=configuration,
        )

    except (
        VerifiedDatasetError,
        sqlite3.Error,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 13A — "
        "Verified Research Dataset Builder"
    )
    print("=" * 80)
    print(
        f"Database: {summary.database_path}"
    )
    print(
        "Allowed statuses: "
        + ", ".join(
            configuration.include_statuses
        )
    )
    print()

    print("Verified Dataset")
    print("-" * 80)
    print(
        f"Rows: {summary.verified_rows}"
    )
    print(
        f"Cycles: {summary.verified_cycles}"
    )
    print(
        f"Unique assets: {summary.unique_assets}"
    )
    print(
        f"Unique tokens: {summary.unique_tokens}"
    )
    print(
        "VERIFIED rows: "
        f"{summary.verified_status_rows}"
    )
    print(
        "VERIFIED_WITH_WARNING rows: "
        f"{summary.verified_with_warning_status_rows}"
    )
    print(
        "Successful quotes / errors: "
        f"{summary.successful_quotes} / "
        f"{summary.quote_errors}"
    )
    print(
        "Eligible / profitable: "
        f"{summary.eligible_observations} / "
        f"{summary.profitable_observations}"
    )
    print(
        "Average net profit: "
        f"${summary.average_net_profit_usd:.6f}"
    )
    print(
        "Average total cost: "
        f"{summary.average_total_cost_bps:.4f} bps"
    )
    print(
        "Average quote latency: "
        f"{summary.average_quote_latency_ms:.2f} ms"
    )
    print(
        "Average validation quality: "
        f"{summary.average_quality_score:.2f}/100"
    )
    print()

    print("Exclusions")
    print("-" * 80)
    print(
        "Unvalidated cycles: "
        f"{summary.excluded_unvalidated_cycles}"
    )
    print(
        "Noneligible validated cycles: "
        f"{summary.excluded_noneligible_cycles}"
    )
    print(
        "Legacy / warning / invalid / validation-error: "
        f"{summary.excluded_legacy_cycles} / "
        f"{summary.excluded_warning_cycles} / "
        f"{summary.excluded_invalid_cycles} / "
        f"{summary.excluded_validation_error_cycles}"
    )
    print()

    print("Validation")
    print("-" * 80)

    for check in checks:
        print(
            f"{'PASS' if check.passed else 'FAIL'} | "
            f"{check.name:32} | "
            f"observed={check.observed} | "
            f"expected={check.expected}"
        )

    print()
    print(
        f"VALID: {summary.valid}"
    )
    print()

    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(path)

    return 0 if summary.valid else 2


if __name__ == "__main__":
    raise SystemExit(main())