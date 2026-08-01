"""
Phase 12D — Enriched-Data Quality Validation

Validates Phase 12A/12B enriched scanner observations stored in:

    live_scanner_features
    live_scanner_cycles
    scanner_enrichment

The validator is read-only with respect to scanner decisions and trade logic.
It may write validation reports to a separate folder and, optionally, to a
dedicated validation-history table.

Checks include:
- Required-field completeness
- Numeric finiteness and range validation
- Asset identity quality
- Duplicate detection
- Timestamp ordering and quote-latency consistency
- Profit/cost arithmetic reconciliation
- Cycle-summary reconciliation
- Quote success/error consistency
- Feature completeness scoring
- Outlier warnings
- Cross-table row reconciliation

Run from the project root:

    python3 -m execution.enriched_data_validator

Optional:

    python3 -m execution.enriched_data_validator --cycle-id "..."
    python3 -m execution.enriched_data_validator --latest-cycle
    python3 -m execution.enriched_data_validator --strict
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sqlite3
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

VALIDATOR_SCHEMA_VERSION = "12D.1.1"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE_PATH = PROJECT_ROOT / "database" / "trades.db"
DEFAULT_OUTPUT_DIRECTORY = PROJECT_ROOT / "research" / "data_quality"

DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIRECTORY / "enriched_data_quality_report.json"
DEFAULT_ISSUES_CSV = DEFAULT_OUTPUT_DIRECTORY / "enriched_data_quality_issues.csv"
DEFAULT_CYCLE_SUMMARY_CSV = DEFAULT_OUTPUT_DIRECTORY / "cycle_quality_summary.csv"
DEFAULT_MANIFEST_JSON = DEFAULT_OUTPUT_DIRECTORY / "data_quality_manifest.json"


class EnrichedDataValidationError(RuntimeError):
    """Base exception for enriched-data validation failures."""


class InvalidValidationConfigurationError(EnrichedDataValidationError):
    """Raised when validator configuration is invalid."""


@dataclass(frozen=True, slots=True)
class ValidationConfiguration:
    database_path: Path = DEFAULT_DATABASE_PATH
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    strict: bool = False
    latest_cycle_only: bool = False
    cycle_id: str | None = None
    write_validation_history: bool = True
    overwrite: bool = True

    arithmetic_tolerance_usd: float = 1e-8
    bps_tolerance: float = 1e-6
    latency_warning_ms: float = 1_000.0
    latency_error_ms: float = 20_000.0
    quote_age_warning_ms: float = 5_000.0
    minimum_quality_score: float = 90.0
    minimum_asset_key_rate: float = 0.99
    minimum_required_field_rate: float = 0.95
    duplicate_rate_error_threshold: float = 0.01

    # Phase 12D.1 legacy classification and clean-data gate.
    classify_legacy_data: bool = True
    legacy_blank_asset_key_rate: float = 0.50
    minimum_current_rows: int = 100
    minimum_current_cycles: int = 1

    def validate(self) -> None:
        if not str(self.database_path).strip():
            raise InvalidValidationConfigurationError(
                "database_path cannot be empty."
            )

        if not str(self.output_directory).strip():
            raise InvalidValidationConfigurationError(
                "output_directory cannot be empty."
            )

        numeric_fields = (
            "arithmetic_tolerance_usd",
            "bps_tolerance",
            "latency_warning_ms",
            "latency_error_ms",
            "quote_age_warning_ms",
            "minimum_quality_score",
            "minimum_asset_key_rate",
            "minimum_required_field_rate",
            "duplicate_rate_error_threshold",
        )

        for name in numeric_fields:
            value = float(getattr(self, name))

            if not math.isfinite(value):
                raise InvalidValidationConfigurationError(
                    f"{name} must be finite."
                )

        if self.arithmetic_tolerance_usd < 0:
            raise InvalidValidationConfigurationError(
                "arithmetic_tolerance_usd cannot be negative."
            )

        if self.bps_tolerance < 0:
            raise InvalidValidationConfigurationError(
                "bps_tolerance cannot be negative."
            )

        if self.latency_warning_ms < 0:
            raise InvalidValidationConfigurationError(
                "latency_warning_ms cannot be negative."
            )

        if self.latency_error_ms < self.latency_warning_ms:
            raise InvalidValidationConfigurationError(
                "latency_error_ms must be >= latency_warning_ms."
            )

        for name in (
            "minimum_asset_key_rate",
            "minimum_required_field_rate",
            "duplicate_rate_error_threshold",
        ):
            value = float(getattr(self, name))

            if not 0.0 <= value <= 1.0:
                raise InvalidValidationConfigurationError(
                    f"{name} must be in [0, 1]."
                )

        if not 0.0 <= self.minimum_quality_score <= 100.0:
            raise InvalidValidationConfigurationError(
                "minimum_quality_score must be in [0, 100]."
            )

        if not 0.0 <= self.legacy_blank_asset_key_rate <= 1.0:
            raise InvalidValidationConfigurationError(
                "legacy_blank_asset_key_rate must be in [0, 1]."
            )

        if self.minimum_current_rows <= 0:
            raise InvalidValidationConfigurationError(
                "minimum_current_rows must be positive."
            )

        if self.minimum_current_cycles <= 0:
            raise InvalidValidationConfigurationError(
                "minimum_current_cycles must be positive."
            )


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    severity: str
    code: str
    message: str
    cycle_id: str | None = None
    row_id: int | None = None
    token: str | None = None
    observed: Any = None
    expected: Any = None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    passed: bool
    examined: int
    errors: int
    warnings: int
    information: int
    details: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CycleQualitySummary:
    cycle_id: str
    cycle_number: int
    rows: int
    quote_successes: int
    quote_errors: int
    eligible_observations: int
    profitable_observations: int
    duplicate_rows: int
    missing_asset_keys: int
    missing_required_fields: int
    arithmetic_errors: int
    timestamp_errors: int
    numeric_errors: int
    warning_count: int
    error_count: int
    average_enrichment_quality_score: float
    average_quote_latency_ms: float
    average_quote_age_ms: float
    average_total_cost_bps: float
    average_net_profit_usd: float
    quality_score: float
    valid: bool

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LegacyClassificationSummary:
    classification_enabled: bool
    all_rows: int
    all_cycles: int
    legacy_rows: int
    legacy_cycles: int
    current_rows: int
    current_cycles: int
    legacy_cycle_ids: tuple[str, ...]
    current_cycle_ids: tuple[str, ...]
    classification_rule: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["legacy_cycle_ids"] = list(
            self.legacy_cycle_ids
        )
        result["current_cycle_ids"] = list(
            self.current_cycle_ids
        )
        return result


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    generated_at: datetime
    schema_version: str
    strict_mode: bool
    database_path: str

    all_rows: int
    all_cycles: int
    legacy_rows: int
    legacy_cycles: int
    current_rows: int
    current_cycles: int

    rows_examined: int
    cycles_examined: int
    checks_run: int
    checks_passed: int
    checks_failed: int
    errors: int
    warnings: int
    information_notices: int
    duplicate_rows: int
    overall_quality_score: float
    clean_data_gate_passed: bool
    valid: bool
    selected_cycle_id: str | None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["generated_at"] = self.generated_at.isoformat()
        return result


REQUIRED_FIELDS: tuple[str, ...] = (
    "cycle_id",
    "cycle_number",
    "scan_time",
    "token",
    "asset_key",
    "decision",
    "quote_successful",
    "starting_amount_usd",
    "ending_amount_usd",
    "gross_profit_usd",
    "estimated_cost_usd",
    "net_profit_usd",
    "gross_edge_bps",
    "net_edge_bps",
    "total_cost_bps",
    "market_score",
    "liquidity_score",
    "volume_score",
    "intelligence_score",
    "quote_latency_ms",
    "enrichment_quality_score",
)

NUMERIC_FIELDS: tuple[str, ...] = (
    "cycle_number",
    "cycle_position",
    "cycle_size",
    "ai_priority",
    "opportunity_probability",
    "expected_profit_usd",
    "combined_confidence",
    "prediction_confidence",
    "downside_risk",
    "trend_score",
    "stability_score",
    "market_score",
    "liquidity_score",
    "volume_score",
    "pair_score",
    "intelligence_score",
    "score_mean",
    "score_std",
    "score_min",
    "score_max",
    "score_range",
    "starting_amount_usd",
    "ending_amount_usd",
    "gross_profit_usd",
    "estimated_cost_usd",
    "net_profit_usd",
    "gross_edge_bps",
    "net_edge_bps",
    "total_cost_bps",
    "slippage_bps",
    "price_impact_bps",
    "network_fee_usd",
    "dex_fee_usd",
    "slippage_cost_usd",
    "liquidity_usd",
    "volume_24h_usd",
    "volume_liquidity_ratio",
    "route_hops",
    "dex_count",
    "quote_latency_ms",
    "quote_age_ms",
    "enrichment_quality_score",
    "cycle_elapsed_seconds",
    "scanner_speed_tokens_per_minute",
)

NON_NEGATIVE_FIELDS: tuple[str, ...] = (
    "cycle_number",
    "cycle_position",
    "cycle_size",
    "starting_amount_usd",
    "ending_amount_usd",
    "estimated_cost_usd",
    "total_cost_bps",
    "slippage_bps",
    "price_impact_bps",
    "network_fee_usd",
    "dex_fee_usd",
    "slippage_cost_usd",
    "liquidity_usd",
    "volume_24h_usd",
    "volume_liquidity_ratio",
    "route_hops",
    "dex_count",
    "quote_latency_ms",
    "quote_age_ms",
    "enrichment_quality_score",
    "cycle_elapsed_seconds",
    "scanner_speed_tokens_per_minute",
)

PERCENTAGE_SCORE_FIELDS: tuple[str, ...] = (
    "ai_priority",
    "opportunity_probability",
    "combined_confidence",
    "prediction_confidence",
    "downside_risk",
    "trend_score",
    "stability_score",
    "market_score",
    "liquidity_score",
    "volume_score",
    "pair_score",
    "intelligence_score",
    "score_mean",
    "score_min",
    "score_max",
    "enrichment_quality_score",
)


class EnrichedDataValidator:
    def __init__(
        self,
        configuration: ValidationConfiguration | None = None,
    ) -> None:
        self.configuration = configuration or ValidationConfiguration()
        self.configuration.validate()
        self.issues: list[ValidationIssue] = []
        self.checks: list[CheckResult] = []

    def run(
        self,
    ) -> tuple[
        ValidationSummary,
        tuple[CheckResult, ...],
        tuple[ValidationIssue, ...],
        tuple[CycleQualitySummary, ...],
        LegacyClassificationSummary,
    ]:
        all_rows, all_cycle_rows = self._load_rows()

        if not all_rows:
            raise EnrichedDataValidationError(
                "No live_scanner_features rows matched the requested scope."
            )

        (
            current_rows,
            current_cycle_rows,
            legacy_summary,
        ) = self._classify_legacy_data(
            all_rows,
            all_cycle_rows,
        )

        if not current_rows:
            raise EnrichedDataValidationError(
                "All matching rows were classified as legacy; "
                "no current-schema rows are available for the clean-data gate."
            )

        # Strict checks apply only to current-schema rows. Legacy rows remain
        # in SQLite and are reported separately for audit history.
        self._check_required_fields(current_rows)
        self._check_numeric_finiteness(current_rows)
        self._check_numeric_ranges(current_rows)
        self._check_asset_identity(current_rows)
        self._check_duplicates(current_rows)
        self._check_timestamp_ordering(current_rows)
        self._check_quote_consistency(current_rows)
        self._check_profit_arithmetic(current_rows)
        self._check_bps_arithmetic(current_rows)
        self._check_score_derivatives(current_rows)
        self._check_quality_scores(current_rows)
        self._check_latency_outliers(current_rows)
        self._check_cycle_positions(current_rows)
        self._check_cycle_summary_reconciliation(
            current_rows,
            current_cycle_rows,
        )
        self._check_cross_table_reconciliation(
            current_rows
        )

        cycle_summaries = self._build_cycle_summaries(
            current_rows
        )

        errors = sum(
            issue.severity == "ERROR"
            for issue in self.issues
        )
        warnings = sum(
            issue.severity == "WARNING"
            for issue in self.issues
        )
        information = sum(
            issue.severity == "INFO"
            for issue in self.issues
        )

        checks_passed = sum(
            check.passed
            for check in self.checks
        )
        checks_failed = len(self.checks) - checks_passed

        overall_quality_score = (
            statistics.fmean(
                cycle.quality_score
                for cycle in cycle_summaries
            )
            if cycle_summaries
            else 0.0
        )

        current_cycle_count = len(
            {
                str(row["cycle_id"])
                for row in current_rows
            }
        )

        minimum_support_passed = (
            len(current_rows)
            >= self.configuration.minimum_current_rows
            and current_cycle_count
            >= self.configuration.minimum_current_cycles
        )

        if not minimum_support_passed:
            self._add_issue(
                "ERROR",
                "INSUFFICIENT_CURRENT_DATA",
                (
                    "Current-schema data is below the clean-data "
                    "gate minimum support."
                ),
                observed={
                    "rows": len(current_rows),
                    "cycles": current_cycle_count,
                },
                expected={
                    "minimum_rows": (
                        self.configuration.minimum_current_rows
                    ),
                    "minimum_cycles": (
                        self.configuration.minimum_current_cycles
                    ),
                },
            )
            errors += 1

        clean_data_gate_passed = (
            errors == 0
            and (
                not self.configuration.strict
                or warnings == 0
            )
            and minimum_support_passed
        )

        summary = ValidationSummary(
            generated_at=datetime.now(timezone.utc),
            schema_version=VALIDATOR_SCHEMA_VERSION,
            strict_mode=self.configuration.strict,
            database_path=str(
                self.configuration.database_path
            ),
            all_rows=legacy_summary.all_rows,
            all_cycles=legacy_summary.all_cycles,
            legacy_rows=legacy_summary.legacy_rows,
            legacy_cycles=legacy_summary.legacy_cycles,
            current_rows=legacy_summary.current_rows,
            current_cycles=legacy_summary.current_cycles,
            rows_examined=len(current_rows),
            cycles_examined=current_cycle_count,
            checks_run=len(self.checks),
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            errors=errors,
            warnings=warnings,
            information_notices=information,
            duplicate_rows=sum(
                issue.code == "DUPLICATE_OBSERVATION"
                for issue in self.issues
            ),
            overall_quality_score=overall_quality_score,
            clean_data_gate_passed=clean_data_gate_passed,
            valid=clean_data_gate_passed,
            selected_cycle_id=self.configuration.cycle_id,
        )

        if self.configuration.write_validation_history:
            self._write_validation_history(
                summary,
                cycle_summaries,
            )

        return (
            summary,
            tuple(self.checks),
            tuple(self.issues),
            tuple(cycle_summaries),
            legacy_summary,
        )

    def _classify_legacy_data(
        self,
        rows: Sequence[sqlite3.Row],
        cycle_rows: Sequence[sqlite3.Row],
    ) -> tuple[
        tuple[sqlite3.Row, ...],
        tuple[sqlite3.Row, ...],
        LegacyClassificationSummary,
    ]:
        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)

        for row in rows:
            grouped[str(row["cycle_id"])].append(row)

        legacy_cycle_ids: set[str] = set()

        if self.configuration.classify_legacy_data:
            for cycle_id, grouped_rows in grouped.items():
                blank_asset_keys = sum(
                    not str(
                        row["asset_key"] or ""
                    ).strip()
                    for row in grouped_rows
                )

                blank_rate = (
                    blank_asset_keys / len(grouped_rows)
                    if grouped_rows
                    else 0.0
                )

                if (
                    blank_rate
                    >= self.configuration
                    .legacy_blank_asset_key_rate
                ):
                    legacy_cycle_ids.add(cycle_id)
                    self._add_issue(
                        "INFO",
                        "LEGACY_CYCLE_CLASSIFIED",
                        (
                            "Cycle classified as legacy because its "
                            "blank asset-key rate predates the current "
                            "field-mapping standard."
                        ),
                        observed={
                            "blank_asset_keys": blank_asset_keys,
                            "rows": len(grouped_rows),
                            "blank_rate": blank_rate,
                        },
                        expected=(
                            "legacy classification threshold >= "
                            f"{self.configuration.legacy_blank_asset_key_rate}"
                        ),
                        cycle_id=cycle_id,
                    )

        all_cycle_ids = set(grouped)
        current_cycle_ids = (
            all_cycle_ids - legacy_cycle_ids
        )

        current_rows = tuple(
            row
            for row in rows
            if str(row["cycle_id"])
            in current_cycle_ids
        )

        current_cycle_rows = tuple(
            row
            for row in cycle_rows
            if str(row["cycle_id"])
            in current_cycle_ids
        )

        legacy_rows = sum(
            len(grouped[cycle_id])
            for cycle_id in legacy_cycle_ids
        )

        classification_rule = (
            "A complete cycle is legacy when its blank asset_key rate "
            f"is >= {self.configuration.legacy_blank_asset_key_rate:.2%}. "
            "Legacy rows remain stored and auditable but are excluded "
            "from current-schema validation and clean-data promotion gates."
            if self.configuration.classify_legacy_data
            else "Legacy classification disabled; all rows are current."
        )

        summary = LegacyClassificationSummary(
            classification_enabled=(
                self.configuration.classify_legacy_data
            ),
            all_rows=len(rows),
            all_cycles=len(all_cycle_ids),
            legacy_rows=legacy_rows,
            legacy_cycles=len(legacy_cycle_ids),
            current_rows=len(current_rows),
            current_cycles=len(current_cycle_ids),
            legacy_cycle_ids=tuple(
                sorted(legacy_cycle_ids)
            ),
            current_cycle_ids=tuple(
                sorted(current_cycle_ids)
            ),
            classification_rule=classification_rule,
        )

        return (
            current_rows,
            current_cycle_rows,
            summary,
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.configuration.database_path
        )
        connection.row_factory = sqlite3.Row
        connection.execute(
            "PRAGMA busy_timeout=30000"
        )
        return connection

    def _load_rows(
        self,
    ) -> tuple[
        tuple[sqlite3.Row, ...],
        tuple[sqlite3.Row, ...],
    ]:
        connection = self._connect()

        try:
            where_parts: list[str] = []
            parameters: list[Any] = []

            selected_cycle_id = self.configuration.cycle_id

            if self.configuration.latest_cycle_only:
                row = connection.execute(
                    """
                    SELECT cycle_id
                    FROM live_scanner_cycles
                    ORDER BY finished_at DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()

                if row is None:
                    raise EnrichedDataValidationError(
                        "No live scanner cycles are available."
                    )

                selected_cycle_id = str(
                    row["cycle_id"]
                )

            if selected_cycle_id:
                where_parts.append(
                    "cycle_id = ?"
                )
                parameters.append(
                    selected_cycle_id
                )

            where_sql = (
                "WHERE " + " AND ".join(where_parts)
                if where_parts
                else ""
            )

            rows = tuple(
                connection.execute(
                    f"""
                    SELECT *
                    FROM live_scanner_features
                    {where_sql}
                    ORDER BY
                        cycle_number,
                        cycle_id,
                        cycle_position,
                        id
                    """,
                    parameters,
                ).fetchall()
            )

            cycle_rows = tuple(
                connection.execute(
                    f"""
                    SELECT *
                    FROM live_scanner_cycles
                    {where_sql}
                    ORDER BY
                        cycle_number,
                        cycle_id,
                        id
                    """,
                    parameters,
                ).fetchall()
            )

            return rows, cycle_rows

        finally:
            connection.close()

    def _add_issue(
        self,
        severity: str,
        code: str,
        message: str,
        row: Mapping[str, Any] | sqlite3.Row | None = None,
        *,
        observed: Any = None,
        expected: Any = None,
        cycle_id: str | None = None,
    ) -> None:
        """
        Add one validation issue.

        sqlite3.Row supports key indexing but does not implement dict.get().
        Read values defensively so both dictionaries and sqlite3.Row objects
        are accepted.
        """

        row_cycle_id = _row_value(
            row,
            "cycle_id",
        )
        row_id_value = _row_value(
            row,
            "id",
        )
        row_token = _row_value(
            row,
            "token",
        )

        self.issues.append(
            ValidationIssue(
                severity=severity,
                code=code,
                message=message,
                cycle_id=(
                    cycle_id
                    if cycle_id is not None
                    else (
                        str(row_cycle_id)
                        if row_cycle_id is not None
                        else None
                    )
                ),
                row_id=(
                    int(row_id_value)
                    if row_id_value is not None
                    else None
                ),
                token=(
                    str(row_token)
                    if row_token is not None
                    else None
                ),
                observed=observed,
                expected=expected,
            )
        )

    def _append_check(
        self,
        name: str,
        *,
        examined: int,
        starting_issue_count: int,
        details: str,
    ) -> None:
        new_issues = self.issues[
            starting_issue_count:
        ]

        errors = sum(
            issue.severity == "ERROR"
            for issue in new_issues
        )
        warnings = sum(
            issue.severity == "WARNING"
            for issue in new_issues
        )
        information = sum(
            issue.severity == "INFO"
            for issue in new_issues
        )

        passed = (
            errors == 0
            and (
                not self.configuration.strict
                or warnings == 0
            )
        )

        self.checks.append(
            CheckResult(
                name=name,
                passed=passed,
                examined=examined,
                errors=errors,
                warnings=warnings,
                information=information,
                details=details,
            )
        )

    def _check_required_fields(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        missing_count = 0

        for row in rows:
            for field in REQUIRED_FIELDS:
                value = row[field]

                if value is None:
                    missing_count += 1
                    self._add_issue(
                        "ERROR",
                        "MISSING_REQUIRED_FIELD",
                        f"Required field {field} is missing.",
                        row,
                        observed=None,
                        expected="non-null",
                    )
                    continue

                if (
                    isinstance(value, str)
                    and not value.strip()
                ):
                    missing_count += 1
                    self._add_issue(
                        "ERROR",
                        "BLANK_REQUIRED_FIELD",
                        f"Required field {field} is blank.",
                        row,
                        observed=value,
                        expected="non-blank",
                    )

        total_required_values = (
            len(rows) * len(REQUIRED_FIELDS)
        )

        completeness_rate = (
            1.0 - (
                missing_count / total_required_values
            )
            if total_required_values
            else 0.0
        )

        if (
            completeness_rate
            < self.configuration.minimum_required_field_rate
        ):
            self._add_issue(
                "ERROR",
                "LOW_REQUIRED_FIELD_COMPLETENESS",
                "Required-field completeness is below the configured minimum.",
                observed=completeness_rate,
                expected=(
                    f">= {self.configuration.minimum_required_field_rate}"
                ),
            )

        self._append_check(
            "required_field_completeness",
            examined=total_required_values,
            starting_issue_count=start,
            details=(
                f"completeness_rate={completeness_rate:.4%}, "
                f"missing_values={missing_count}"
            ),
        )

    def _check_numeric_finiteness(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            for field in NUMERIC_FIELDS:
                value = row[field]

                if value is None:
                    continue

                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    self._add_issue(
                        "ERROR",
                        "NON_NUMERIC_VALUE",
                        f"{field} is not numeric.",
                        row,
                        observed=value,
                        expected="finite number",
                    )
                    continue

                if not math.isfinite(numeric):
                    self._add_issue(
                        "ERROR",
                        "NON_FINITE_VALUE",
                        f"{field} is not finite.",
                        row,
                        observed=value,
                        expected="finite number",
                    )

        self._append_check(
            "numeric_finiteness",
            examined=len(rows) * len(NUMERIC_FIELDS),
            starting_issue_count=start,
            details=(
                f"rows={len(rows)}, "
                f"numeric_fields={len(NUMERIC_FIELDS)}"
            ),
        )

    def _check_numeric_ranges(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            for field in NON_NEGATIVE_FIELDS:
                value = _number(row[field])

                if value < 0:
                    self._add_issue(
                        "ERROR",
                        "NEGATIVE_NON_NEGATIVE_FIELD",
                        f"{field} cannot be negative.",
                        row,
                        observed=value,
                        expected=">= 0",
                    )

            for field in PERCENTAGE_SCORE_FIELDS:
                value = _number(row[field])

                if not 0.0 <= value <= 100.0:
                    self._add_issue(
                        "WARNING",
                        "SCORE_OUT_OF_RANGE",
                        f"{field} is outside [0, 100].",
                        row,
                        observed=value,
                        expected="0 to 100",
                    )

            if _number(row["score_std"]) < 0:
                self._add_issue(
                    "ERROR",
                    "NEGATIVE_SCORE_STD",
                    "score_std cannot be negative.",
                    row,
                    observed=row["score_std"],
                    expected=">= 0",
                )

        self._append_check(
            "numeric_ranges",
            examined=len(rows),
            starting_issue_count=start,
            details="validated non-negative and score-range constraints",
        )

    def _check_asset_identity(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        missing_asset_keys = 0

        for row in rows:
            asset_key = str(
                row["asset_key"] or ""
            ).strip()

            token = str(
                row["token"] or ""
            ).strip()

            if not asset_key:
                missing_asset_keys += 1
                self._add_issue(
                    "ERROR",
                    "MISSING_ASSET_KEY",
                    "asset_key is missing.",
                    row,
                )

            elif asset_key in {
                "UNKNOWN",
                "symbol:",
                "mint:",
            }:
                self._add_issue(
                    "WARNING",
                    "WEAK_ASSET_KEY",
                    "asset_key is not specific enough.",
                    row,
                    observed=asset_key,
                    expected="specific mint or symbol key",
                )

            if not token:
                self._add_issue(
                    "ERROR",
                    "MISSING_TOKEN",
                    "token is missing.",
                    row,
                )

        asset_key_rate = (
            1.0 - (
                missing_asset_keys / len(rows)
            )
            if rows
            else 0.0
        )

        if (
            asset_key_rate
            < self.configuration.minimum_asset_key_rate
        ):
            self._add_issue(
                "ERROR",
                "LOW_ASSET_KEY_RATE",
                "Asset-key completeness is below the configured minimum.",
                observed=asset_key_rate,
                expected=(
                    f">= {self.configuration.minimum_asset_key_rate}"
                ),
            )

        self._append_check(
            "asset_identity",
            examined=len(rows),
            starting_issue_count=start,
            details=(
                f"asset_key_rate={asset_key_rate:.4%}, "
                f"missing_asset_keys={missing_asset_keys}"
            ),
        )

    def _check_duplicates(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        seen: dict[
            tuple[str, Any, str, Any],
            int,
        ] = {}
        duplicate_count = 0

        for row in rows:
            key = (
                str(row["cycle_id"]),
                row["source_event_id"],
                str(row["token"]),
                row["scan_time"],
            )

            if key in seen:
                duplicate_count += 1
                self._add_issue(
                    "ERROR",
                    "DUPLICATE_OBSERVATION",
                    (
                        "Duplicate cycle/event/token/scan-time "
                        f"identity; first row id={seen[key]}."
                    ),
                    row,
                    observed=key,
                    expected="unique observation identity",
                )
            else:
                seen[key] = int(row["id"])

        duplicate_rate = (
            duplicate_count / len(rows)
            if rows
            else 0.0
        )

        if (
            duplicate_rate
            > self.configuration.duplicate_rate_error_threshold
        ):
            self._add_issue(
                "ERROR",
                "HIGH_DUPLICATE_RATE",
                "Duplicate rate exceeds the configured threshold.",
                observed=duplicate_rate,
                expected=(
                    f"<= {self.configuration.duplicate_rate_error_threshold}"
                ),
            )

        self._append_check(
            "duplicate_observations",
            examined=len(rows),
            starting_issue_count=start,
            details=(
                f"duplicate_rows={duplicate_count}, "
                f"duplicate_rate={duplicate_rate:.4%}"
            ),
        )

    def _check_timestamp_ordering(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            scan_time = _parse_timestamp(
                row["scan_time"]
            )
            logged_at = _parse_timestamp(
                row["logged_at"]
            )

            if scan_time and logged_at and logged_at < scan_time:
                self._add_issue(
                    "ERROR",
                    "LOGGED_BEFORE_SCAN",
                    "logged_at is earlier than scan_time.",
                    row,
                    observed=row["logged_at"],
                    expected=f">= {row['scan_time']}",
                )

            if _number(row["quote_latency_ms"]) < 0:
                self._add_issue(
                    "ERROR",
                    "NEGATIVE_QUOTE_LATENCY",
                    "quote_latency_ms cannot be negative.",
                    row,
                )

            if _number(row["quote_age_ms"]) < 0:
                self._add_issue(
                    "ERROR",
                    "NEGATIVE_QUOTE_AGE",
                    "quote_age_ms cannot be negative.",
                    row,
                )

        self._append_check(
            "timestamp_ordering",
            examined=len(rows),
            starting_issue_count=start,
            details="validated scan/log ordering and non-negative timing values",
        )

    def _check_quote_consistency(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            successful = bool(
                int(row["quote_successful"] or 0)
            )
            decision = str(
                row["decision"] or ""
            ).upper()

            if successful and "QUOTE ERROR" in decision:
                self._add_issue(
                    "ERROR",
                    "SUCCESS_WITH_QUOTE_ERROR_DECISION",
                    "Quote is marked successful but decision is QUOTE ERROR.",
                    row,
                )

            if (
                not successful
                and "QUOTE ERROR" not in decision
            ):
                self._add_issue(
                    "WARNING",
                    "FAILED_QUOTE_WITHOUT_ERROR_DECISION",
                    "Quote is marked failed without a QUOTE ERROR decision.",
                    row,
                )

            if (
                successful
                and _number(row["starting_amount_usd"]) <= 0
            ):
                self._add_issue(
                    "ERROR",
                    "SUCCESSFUL_QUOTE_WITH_NON_POSITIVE_START",
                    "Successful quote has non-positive starting amount.",
                    row,
                    observed=row["starting_amount_usd"],
                    expected="> 0",
                )

        self._append_check(
            "quote_success_consistency",
            examined=len(rows),
            starting_issue_count=start,
            details="validated quote_successful against decision and amounts",
        )

    def _check_profit_arithmetic(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        tolerance = (
            self.configuration.arithmetic_tolerance_usd
        )

        for row in rows:
            if not bool(
                int(row["quote_successful"] or 0)
            ):
                continue

            starting = _number(
                row["starting_amount_usd"]
            )
            ending = _number(
                row["ending_amount_usd"]
            )
            gross = _number(
                row["gross_profit_usd"]
            )
            cost = _number(
                row["estimated_cost_usd"]
            )
            net = _number(
                row["net_profit_usd"]
            )

            calculated_gross = ending - starting
            calculated_net = gross - cost

            if abs(
                gross - calculated_gross
            ) > tolerance:
                self._add_issue(
                    "ERROR",
                    "GROSS_PROFIT_MISMATCH",
                    "gross_profit_usd does not reconcile with ending-starting.",
                    row,
                    observed=gross,
                    expected=calculated_gross,
                )

            if abs(
                net - calculated_net
            ) > tolerance:
                self._add_issue(
                    "ERROR",
                    "NET_PROFIT_MISMATCH",
                    "net_profit_usd does not reconcile with gross-cost.",
                    row,
                    observed=net,
                    expected=calculated_net,
                )

        self._append_check(
            "profit_arithmetic",
            examined=len(rows),
            starting_issue_count=start,
            details=f"tolerance_usd={tolerance}",
        )

    def _check_bps_arithmetic(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        tolerance = self.configuration.bps_tolerance

        for row in rows:
            starting = _number(
                row["starting_amount_usd"]
            )

            if starting <= 0:
                continue

            gross_expected = (
                _number(row["gross_profit_usd"])
                / starting
                * 10_000.0
            )
            net_expected = (
                _number(row["net_profit_usd"])
                / starting
                * 10_000.0
            )
            cost_expected = (
                _number(row["estimated_cost_usd"])
                / starting
                * 10_000.0
            )

            for field, expected in (
                ("gross_edge_bps", gross_expected),
                ("net_edge_bps", net_expected),
                ("total_cost_bps", cost_expected),
            ):
                observed = _number(row[field])

                if abs(
                    observed - expected
                ) > tolerance:
                    self._add_issue(
                        "ERROR",
                        "BPS_RECONCILIATION_ERROR",
                        f"{field} does not reconcile.",
                        row,
                        observed=observed,
                        expected=expected,
                    )

        self._append_check(
            "bps_arithmetic",
            examined=len(rows),
            starting_issue_count=start,
            details=f"bps_tolerance={tolerance}",
        )

    def _check_score_derivatives(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            minimum = _number(row["score_min"])
            maximum = _number(row["score_max"])
            score_range = _number(
                row["score_range"]
            )

            if maximum < minimum:
                self._add_issue(
                    "ERROR",
                    "SCORE_MAX_BELOW_MIN",
                    "score_max is below score_min.",
                    row,
                    observed=maximum,
                    expected=f">= {minimum}",
                )

            expected_range = maximum - minimum

            if abs(
                score_range - expected_range
            ) > 1e-8:
                self._add_issue(
                    "ERROR",
                    "SCORE_RANGE_MISMATCH",
                    "score_range does not equal score_max-score_min.",
                    row,
                    observed=score_range,
                    expected=expected_range,
                )

        self._append_check(
            "score_derivatives",
            examined=len(rows),
            starting_issue_count=start,
            details="validated score min/max/range relationships",
        )

    def _check_quality_scores(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            quality = _number(
                row["enrichment_quality_score"]
            )

            if quality < self.configuration.minimum_quality_score:
                self._add_issue(
                    "WARNING",
                    "LOW_ENRICHMENT_QUALITY",
                    "Enrichment quality score is below the configured minimum.",
                    row,
                    observed=quality,
                    expected=(
                        f">= {self.configuration.minimum_quality_score}"
                    ),
                )

        self._append_check(
            "enrichment_quality",
            examined=len(rows),
            starting_issue_count=start,
            details=(
                "minimum_quality_score="
                f"{self.configuration.minimum_quality_score}"
            ),
        )

    def _check_latency_outliers(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)

        for row in rows:
            latency = _number(
                row["quote_latency_ms"]
            )
            quote_age = _number(
                row["quote_age_ms"]
            )

            if latency >= self.configuration.latency_error_ms:
                self._add_issue(
                    "ERROR",
                    "EXTREME_QUOTE_LATENCY",
                    "Quote latency exceeds the error threshold.",
                    row,
                    observed=latency,
                    expected=(
                        f"< {self.configuration.latency_error_ms} ms"
                    ),
                )

            elif latency >= self.configuration.latency_warning_ms:
                self._add_issue(
                    "WARNING",
                    "HIGH_QUOTE_LATENCY",
                    "Quote latency exceeds the warning threshold.",
                    row,
                    observed=latency,
                    expected=(
                        f"< {self.configuration.latency_warning_ms} ms"
                    ),
                )

            if quote_age >= self.configuration.quote_age_warning_ms:
                self._add_issue(
                    "WARNING",
                    "STALE_PROVIDER_QUOTE",
                    "Quote age exceeds the warning threshold.",
                    row,
                    observed=quote_age,
                    expected=(
                        f"< {self.configuration.quote_age_warning_ms} ms"
                    ),
                )

        self._append_check(
            "latency_outliers",
            examined=len(rows),
            starting_issue_count=start,
            details=(
                f"warning_ms={self.configuration.latency_warning_ms}, "
                f"error_ms={self.configuration.latency_error_ms}"
            ),
        )

    def _check_cycle_positions(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        grouped: dict[
            str,
            list[sqlite3.Row],
        ] = defaultdict(list)

        for row in rows:
            grouped[str(row["cycle_id"])].append(
                row
            )

        for cycle_id, cycle_rows in grouped.items():
            positions = [
                int(row["cycle_position"] or 0)
                for row in cycle_rows
            ]

            expected_positions = list(
                range(1, len(cycle_rows) + 1)
            )

            if sorted(positions) != expected_positions:
                self._add_issue(
                    "ERROR",
                    "INVALID_CYCLE_POSITIONS",
                    "Cycle positions are not contiguous from 1..N.",
                    observed=sorted(positions),
                    expected=expected_positions,
                    cycle_id=cycle_id,
                )

            declared_sizes = {
                int(row["cycle_size"] or 0)
                for row in cycle_rows
            }

            if declared_sizes != {len(cycle_rows)}:
                self._add_issue(
                    "ERROR",
                    "CYCLE_SIZE_MISMATCH",
                    "cycle_size does not match actual cycle row count.",
                    observed=sorted(declared_sizes),
                    expected=len(cycle_rows),
                    cycle_id=cycle_id,
                )

        self._append_check(
            "cycle_positions",
            examined=len(grouped),
            starting_issue_count=start,
            details=f"cycles_examined={len(grouped)}",
        )

    def _check_cycle_summary_reconciliation(
        self,
        rows: Sequence[sqlite3.Row],
        cycle_rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        grouped: dict[
            str,
            list[sqlite3.Row],
        ] = defaultdict(list)

        for row in rows:
            grouped[str(row["cycle_id"])].append(
                row
            )

        cycle_lookup = {
            str(row["cycle_id"]): row
            for row in cycle_rows
        }

        for cycle_id, feature_rows in grouped.items():
            cycle_row = cycle_lookup.get(cycle_id)

            if cycle_row is None:
                self._add_issue(
                    "ERROR",
                    "MISSING_CYCLE_SUMMARY",
                    "No live_scanner_cycles row exists for this cycle.",
                    cycle_id=cycle_id,
                )
                continue

            calculated = {
                "observations_received": len(feature_rows),
                "observations_logged": len(feature_rows),
                "quote_successes": sum(
                    bool(int(row["quote_successful"] or 0))
                    for row in feature_rows
                ),
                "quote_errors": sum(
                    not bool(int(row["quote_successful"] or 0))
                    for row in feature_rows
                ),
                "eligible_observations": sum(
                    bool(int(row["eligible"] or 0))
                    for row in feature_rows
                ),
                "profitable_observations": sum(
                    _number(row["net_profit_usd"]) > 0
                    and bool(int(row["quote_successful"] or 0))
                    for row in feature_rows
                ),
            }

            for field, expected in calculated.items():
                observed = int(
                    cycle_row[field] or 0
                )

                if observed != expected:
                    self._add_issue(
                        "ERROR",
                        "CYCLE_SUMMARY_COUNT_MISMATCH",
                        f"Cycle summary field {field} does not reconcile.",
                        observed=observed,
                        expected=expected,
                        cycle_id=cycle_id,
                    )

        self._append_check(
            "cycle_summary_reconciliation",
            examined=len(grouped),
            starting_issue_count=start,
            details=(
                f"feature_cycles={len(grouped)}, "
                f"summary_cycles={len(cycle_rows)}"
            ),
        )

    def _check_cross_table_reconciliation(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> None:
        start = len(self.issues)
        enrichment_ids = [
            int(row["scanner_enrichment_id"])
            for row in rows
            if row["scanner_enrichment_id"] is not None
        ]

        if not enrichment_ids:
            self._add_issue(
                "WARNING",
                "NO_SCANNER_ENRICHMENT_LINKS",
                "No live feature rows link to scanner_enrichment.",
            )
            self._append_check(
                "cross_table_reconciliation",
                examined=len(rows),
                starting_issue_count=start,
                details="linked_scanner_enrichment_rows=0",
            )
            return

        connection = self._connect()

        try:
            placeholders = ",".join(
                "?"
                for _ in enrichment_ids
            )

            found = {
                int(row["id"])
                for row in connection.execute(
                    f"""
                    SELECT id
                    FROM scanner_enrichment
                    WHERE id IN ({placeholders})
                    """,
                    enrichment_ids,
                ).fetchall()
            }

        finally:
            connection.close()

        for row in rows:
            enrichment_id = row["scanner_enrichment_id"]

            if (
                enrichment_id is not None
                and int(enrichment_id) not in found
            ):
                self._add_issue(
                    "ERROR",
                    "MISSING_SCANNER_ENRICHMENT_ROW",
                    "Referenced scanner_enrichment row does not exist.",
                    row,
                    observed=enrichment_id,
                    expected="existing scanner_enrichment.id",
                )

        self._append_check(
            "cross_table_reconciliation",
            examined=len(enrichment_ids),
            starting_issue_count=start,
            details=(
                f"linked={len(enrichment_ids)}, "
                f"found={len(found)}"
            ),
        )

    def _build_cycle_summaries(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> list[CycleQualitySummary]:
        grouped: dict[
            str,
            list[sqlite3.Row],
        ] = defaultdict(list)

        for row in rows:
            grouped[str(row["cycle_id"])].append(
                row
            )

        issues_by_cycle: dict[
            str,
            list[ValidationIssue],
        ] = defaultdict(list)

        for issue in self.issues:
            if issue.cycle_id:
                issues_by_cycle[
                    issue.cycle_id
                ].append(issue)

        summaries: list[CycleQualitySummary] = []

        for cycle_id, cycle_rows in grouped.items():
            issues = issues_by_cycle.get(
                cycle_id,
                [],
            )

            errors = sum(
                issue.severity == "ERROR"
                for issue in issues
            )
            warnings = sum(
                issue.severity == "WARNING"
                for issue in issues
            )

            missing_asset_keys = sum(
                issue.code == "MISSING_ASSET_KEY"
                for issue in issues
            )
            missing_required_fields = sum(
                issue.code in {
                    "MISSING_REQUIRED_FIELD",
                    "BLANK_REQUIRED_FIELD",
                }
                for issue in issues
            )
            arithmetic_errors = sum(
                issue.code in {
                    "GROSS_PROFIT_MISMATCH",
                    "NET_PROFIT_MISMATCH",
                    "BPS_RECONCILIATION_ERROR",
                }
                for issue in issues
            )
            timestamp_errors = sum(
                issue.code in {
                    "LOGGED_BEFORE_SCAN",
                    "NEGATIVE_QUOTE_LATENCY",
                    "NEGATIVE_QUOTE_AGE",
                }
                for issue in issues
            )
            numeric_errors = sum(
                issue.code in {
                    "NON_NUMERIC_VALUE",
                    "NON_FINITE_VALUE",
                    "NEGATIVE_NON_NEGATIVE_FIELD",
                    "NEGATIVE_SCORE_STD",
                }
                for issue in issues
            )
            duplicate_rows = sum(
                issue.code == "DUPLICATE_OBSERVATION"
                for issue in issues
            )

            row_count = len(cycle_rows)
            base_score = 100.0
            base_score -= min(
                60.0,
                errors * 5.0,
            )
            base_score -= min(
                30.0,
                warnings * 1.0,
            )

            quality_score = max(
                0.0,
                min(100.0, base_score),
            )

            summaries.append(
                CycleQualitySummary(
                    cycle_id=cycle_id,
                    cycle_number=int(
                        cycle_rows[0]["cycle_number"]
                        or 0
                    ),
                    rows=row_count,
                    quote_successes=sum(
                        bool(
                            int(
                                row["quote_successful"]
                                or 0
                            )
                        )
                        for row in cycle_rows
                    ),
                    quote_errors=sum(
                        not bool(
                            int(
                                row["quote_successful"]
                                or 0
                            )
                        )
                        for row in cycle_rows
                    ),
                    eligible_observations=sum(
                        bool(
                            int(
                                row["eligible"]
                                or 0
                            )
                        )
                        for row in cycle_rows
                    ),
                    profitable_observations=sum(
                        bool(
                            int(
                                row["quote_successful"]
                                or 0
                            )
                        )
                        and _number(
                            row["net_profit_usd"]
                        ) > 0
                        for row in cycle_rows
                    ),
                    duplicate_rows=duplicate_rows,
                    missing_asset_keys=missing_asset_keys,
                    missing_required_fields=missing_required_fields,
                    arithmetic_errors=arithmetic_errors,
                    timestamp_errors=timestamp_errors,
                    numeric_errors=numeric_errors,
                    warning_count=warnings,
                    error_count=errors,
                    average_enrichment_quality_score=statistics.fmean(
                        _number(
                            row[
                                "enrichment_quality_score"
                            ]
                        )
                        for row in cycle_rows
                    ),
                    average_quote_latency_ms=statistics.fmean(
                        _number(
                            row["quote_latency_ms"]
                        )
                        for row in cycle_rows
                    ),
                    average_quote_age_ms=statistics.fmean(
                        _number(
                            row["quote_age_ms"]
                        )
                        for row in cycle_rows
                    ),
                    average_total_cost_bps=statistics.fmean(
                        _number(
                            row["total_cost_bps"]
                        )
                        for row in cycle_rows
                    ),
                    average_net_profit_usd=statistics.fmean(
                        _number(
                            row["net_profit_usd"]
                        )
                        for row in cycle_rows
                    ),
                    quality_score=quality_score,
                    valid=(
                        errors == 0
                        and (
                            not self.configuration.strict
                            or warnings == 0
                        )
                    ),
                )
            )

        summaries.sort(
            key=lambda item: (
                item.cycle_number,
                item.cycle_id,
            )
        )

        return summaries

    def _write_validation_history(
        self,
        summary: ValidationSummary,
        cycle_summaries: Sequence[CycleQualitySummary],
    ) -> None:
        connection = self._connect()

        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS enriched_data_validation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    validator_schema_version TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    strict_mode INTEGER NOT NULL,
                    rows_examined INTEGER NOT NULL,
                    cycles_examined INTEGER NOT NULL,
                    errors INTEGER NOT NULL,
                    warnings INTEGER NOT NULL,
                    information_notices INTEGER NOT NULL,
                    overall_quality_score REAL NOT NULL,
                    valid INTEGER NOT NULL,
                    selected_cycle_id TEXT,
                    summary_json TEXT NOT NULL,
                    cycle_summaries_json TEXT NOT NULL
                )
                """
            )

            connection.execute(
                """
                INSERT INTO enriched_data_validation_history (
                    validator_schema_version,
                    generated_at,
                    strict_mode,
                    rows_examined,
                    cycles_examined,
                    errors,
                    warnings,
                    information_notices,
                    overall_quality_score,
                    valid,
                    selected_cycle_id,
                    summary_json,
                    cycle_summaries_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    VALIDATOR_SCHEMA_VERSION,
                    summary.generated_at.isoformat(),
                    int(summary.strict_mode),
                    summary.rows_examined,
                    summary.cycles_examined,
                    summary.errors,
                    summary.warnings,
                    summary.information_notices,
                    summary.overall_quality_score,
                    int(summary.valid),
                    summary.selected_cycle_id,
                    json.dumps(
                        summary.to_dict(),
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        [
                            cycle.to_record()
                            for cycle in cycle_summaries
                        ],
                        ensure_ascii=False,
                    ),
                ),
            )

            connection.commit()

        finally:
            connection.close()


def export_validation_results(
    summary: ValidationSummary,
    checks: Sequence[CheckResult],
    issues: Sequence[ValidationIssue],
    cycle_summaries: Sequence[CycleQualitySummary],
    legacy_summary: LegacyClassificationSummary,
    configuration: ValidationConfiguration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = output / DEFAULT_REPORT_JSON.name
    issues_path = output / DEFAULT_ISSUES_CSV.name
    cycles_path = output / DEFAULT_CYCLE_SUMMARY_CSV.name
    manifest_path = output / DEFAULT_MANIFEST_JSON.name

    destinations = (
        report_path,
        issues_path,
        cycles_path,
        manifest_path,
    )

    if not configuration.overwrite:
        existing = [
            path
            for path in destinations
            if path.exists()
        ]

        if existing:
            raise EnrichedDataValidationError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    report_path.write_text(
        json.dumps(
            {
                "summary": summary.to_dict(),
                "configuration": {
                    **asdict(configuration),
                    "database_path": str(
                        configuration.database_path
                    ),
                    "output_directory": str(
                        configuration.output_directory
                    ),
                },
                "checks": [
                    check.to_dict()
                    for check in checks
                ],
                "issues": [
                    issue.to_record()
                    for issue in issues
                ],
                "legacy_classification": (
                    legacy_summary.to_dict()
                ),
                "clean_data_gate": {
                    "passed": (
                        summary.clean_data_gate_passed
                    ),
                    "minimum_current_rows": (
                        configuration.minimum_current_rows
                    ),
                    "minimum_current_cycles": (
                        configuration.minimum_current_cycles
                    ),
                    "current_rows": summary.current_rows,
                    "current_cycles": summary.current_cycles,
                },
                "cycle_summaries": [
                    cycle.to_record()
                    for cycle in cycle_summaries
                ],
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    _write_csv(
        issues_path,
        [
            issue.to_record()
            for issue in issues
        ],
    )

    _write_csv(
        cycles_path,
        [
            cycle.to_record()
            for cycle in cycle_summaries
        ],
    )

    manifest_path.write_text(
        json.dumps(
            {
                "validator_schema_version": (
                    VALIDATOR_SCHEMA_VERSION
                ),
                "generated_at": (
                    datetime.now(timezone.utc)
                    .isoformat()
                ),
                "database_path": str(
                    configuration.database_path
                ),
                "outputs": {
                    "report": str(report_path),
                    "issues": str(issues_path),
                    "cycle_summaries": str(cycles_path),
                },
                "valid": summary.valid,
                "clean_data_gate_passed": (
                    summary.clean_data_gate_passed
                ),
                "legacy_classification": (
                    legacy_summary.to_dict()
                ),
                "overall_quality_score": (
                    summary.overall_quality_score
                ),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return destinations


def _write_csv(
    path: Path,
    records: Sequence[Mapping[str, Any]],
) -> None:
    if not records:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(
                records[0].keys()
            ),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(records)


def _row_value(
    row: Mapping[str, Any] | sqlite3.Row | None,
    key: str,
    default: Any = None,
) -> Any:
    """
    Read a value from either a dictionary-like mapping or sqlite3.Row.
    """

    if row is None:
        return default

    try:
        return row[key]
    except (
        KeyError,
        IndexError,
        TypeError,
    ):
        return default


def _number(
    value: Any,
) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0

    if not math.isfinite(numeric):
        return 0.0

    return numeric


def _parse_timestamp(
    value: Any,
) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()

        if not text:
            return None

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            for pattern in (
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    parsed = datetime.strptime(
                        text,
                        pattern,
                    )
                    break
                except ValueError:
                    continue
            else:
                return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        timezone.utc
    )


def run_validation(
    *,
    configuration: ValidationConfiguration | None = None,
) -> ValidationSummary:
    active_configuration = (
        configuration
        or ValidationConfiguration()
    )

    (
        summary,
        checks,
        issues,
        cycle_summaries,
        legacy_summary,
    ) = EnrichedDataValidator(
        active_configuration
    ).run()

    export_validation_results(
        summary,
        checks,
        issues,
        cycle_summaries,
        legacy_summary,
        active_configuration,
    )

    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 12D enriched-data quality validation."
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
        "--cycle-id",
        default=None,
    )

    parser.add_argument(
        "--latest-cycle",
        action="store_true",
    )

    parser.add_argument(
        "--strict",
        action="store_true",
    )

    parser.add_argument(
        "--disable-legacy-classification",
        action="store_true",
    )

    parser.add_argument(
        "--legacy-blank-asset-rate",
        type=float,
        default=0.50,
    )

    parser.add_argument(
        "--minimum-current-rows",
        type=int,
        default=100,
    )

    parser.add_argument(
        "--minimum-current-cycles",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--no-history",
        action="store_true",
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
    args = _parser().parse_args(argv)

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

    configuration = ValidationConfiguration(
        database_path=Path(args.database),
        output_directory=Path(
            args.output_directory
        ),
        strict=args.strict,
        latest_cycle_only=args.latest_cycle,
        classify_legacy_data=(
            not args.disable_legacy_classification
        ),
        legacy_blank_asset_key_rate=(
            args.legacy_blank_asset_rate
        ),
        minimum_current_rows=(
            args.minimum_current_rows
        ),
        minimum_current_cycles=(
            args.minimum_current_cycles
        ),
        cycle_id=args.cycle_id,
        write_validation_history=(
            not args.no_history
        ),
        overwrite=(
            not args.no_overwrite
        ),
    )

    try:
        (
            summary,
            checks,
            issues,
            cycle_summaries,
            legacy_summary,
        ) = EnrichedDataValidator(
            configuration
        ).run()

        output_paths = export_validation_results(
            summary,
            checks,
            issues,
            cycle_summaries,
            legacy_summary,
            configuration,
        )

    except (
        EnrichedDataValidationError,
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
        "\nPhase 12D — "
        "Enriched-Data Quality Validation"
    )
    print("=" * 80)
    print(
        f"Database: {summary.database_path}"
    )
    print(
        f"Strict mode: {summary.strict_mode}"
    )
    print(
        f"All rows / cycles: "
        f"{summary.all_rows} / {summary.all_cycles}"
    )
    print(
        f"Legacy rows / cycles: "
        f"{summary.legacy_rows} / {summary.legacy_cycles}"
    )
    print(
        f"Current rows / cycles: "
        f"{summary.current_rows} / {summary.current_cycles}"
    )
    print(
        f"Rows examined by clean gate: "
        f"{summary.rows_examined}"
    )
    print()

    print("Legacy Classification")
    print("-" * 80)
    print(
        "Enabled: "
        f"{legacy_summary.classification_enabled}"
    )
    print(
        "Legacy cycle IDs: "
        + (
            ", ".join(
                legacy_summary.legacy_cycle_ids
            )
            if legacy_summary.legacy_cycle_ids
            else "None"
        )
    )
    print(
        "Rule: "
        f"{legacy_summary.classification_rule}"
    )
    print()

    print("Validation Summary")
    print("-" * 80)
    print(
        f"Checks run: {summary.checks_run}"
    )
    print(
        f"Checks passed: {summary.checks_passed}"
    )
    print(
        f"Checks failed: {summary.checks_failed}"
    )
    print(
        f"Errors: {summary.errors}"
    )
    print(
        f"Warnings: {summary.warnings}"
    )
    print(
        "Information notices: "
        f"{summary.information_notices}"
    )
    print(
        "Duplicate rows: "
        f"{summary.duplicate_rows}"
    )
    print(
        "Overall quality score: "
        f"{summary.overall_quality_score:.2f}/100"
    )
    print(
        "Clean-data gate passed: "
        f"{summary.clean_data_gate_passed}"
    )
    print(
        f"VALID: {summary.valid}"
    )
    print()

    print("Checks")
    print("-" * 80)

    for check in checks:
        print(
            f"{'PASS' if check.passed else 'FAIL'} | "
            f"{check.name:36} | "
            f"examined={check.examined:<6} | "
            f"errors={check.errors:<4} | "
            f"warnings={check.warnings:<4}"
        )
        print(
            f"       {check.details}"
        )

    print()

    print("Cycle Quality")
    print("-" * 80)

    for cycle in cycle_summaries:
        print(
            f"{cycle.cycle_id} | "
            f"rows={cycle.rows:<4} | "
            f"errors={cycle.error_count:<3} | "
            f"warnings={cycle.warning_count:<3} | "
            f"quality={cycle.quality_score:>6.2f}/100 | "
            f"valid={cycle.valid}"
        )

    if issues:
        print()
        print("Validation Issues")
        print("-" * 80)

        for issue in issues[:50]:
            location = []

            if issue.cycle_id:
                location.append(
                    f"cycle={issue.cycle_id}"
                )

            if issue.row_id is not None:
                location.append(
                    f"row={issue.row_id}"
                )

            if issue.token:
                location.append(
                    f"token={issue.token}"
                )

            location_text = (
                " | ".join(location)
                if location
                else "global"
            )

            print(
                f"{issue.severity:<7} | "
                f"{issue.code:<34} | "
                f"{location_text}"
            )
            print(
                f"          {issue.message}"
            )

        if len(issues) > 50:
            print(
                f"... {len(issues) - 50} additional issues "
                "are available in the CSV/JSON reports."
            )

    print()
    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(path)

    if summary.valid:
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())