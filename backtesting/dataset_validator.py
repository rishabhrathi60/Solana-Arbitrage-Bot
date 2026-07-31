"""
Phase 10B — Historical Backtest Dataset Validator

Validates the complete read-only research pipeline:

ScannerHistoryEvent
    ↓
HistoricalDatasetRow
    ↓
BacktestEvent

The validator checks schema-level invariants, chronology, arithmetic integrity,
cycle consistency, decision normalization, outcome labeling, execution-candidate
rules, and aggregate reconciliation.

This module never modifies SQLite or live trading state.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

try:
    from backtesting.event_builder import (
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from backtesting.historical_dataset import (
        DEFAULT_DATABASE_PATH,
        HistoricalDataset,
        HistoricalDatasetError,
        HistoricalDatasetRow,
        build_historical_dataset,
    )
except ModuleNotFoundError:
    from event_builder import (  # type: ignore
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from historical_dataset import (  # type: ignore
        DEFAULT_DATABASE_PATH,
        HistoricalDataset,
        HistoricalDatasetError,
        HistoricalDatasetRow,
        build_historical_dataset,
    )


LOGGER = logging.getLogger(__name__)

DEFAULT_REPORT_DIRECTORY = Path("backtesting") / "output"
DEFAULT_REPORT_PATH = DEFAULT_REPORT_DIRECTORY / "dataset_validation_report.json"

VALID_DECISIONS = frozenset(
    {
        "EXECUTE",
        "WATCH",
        "SKIP",
        "QUOTE_ERROR",
        "UNKNOWN",
    }
)

VALID_OUTCOMES = frozenset(
    {
        "POSITIVE",
        "NEGATIVE",
        "FLAT",
        "QUOTE_ERROR",
    }
)


class DatasetValidationError(RuntimeError):
    """Base exception for dataset validation failures."""


class DatasetValidationConfigurationError(DatasetValidationError):
    """Raised when validator configuration is invalid."""


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    severity: str
    code: str
    message: str
    source_event_id: int | None = None
    cycle_id: str | None = None
    field_name: str | None = None
    observed: Any = None
    expected: Any = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ValidationCheckResult:
    name: str
    passed: bool
    examined: int
    errors: int
    warnings: int
    details: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ValidationReport:
    generated_at: datetime
    database_path: str
    strict_mode: bool

    dataset_rows: int
    event_rows: int
    cycles: int
    unique_assets: int
    unique_tokens: int

    checks_run: int
    checks_passed: int
    checks_failed: int

    error_count: int
    warning_count: int
    info_count: int

    is_valid: bool
    checks: tuple[ValidationCheckResult, ...]
    issues: tuple[ValidationIssue, ...]

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["generated_at"] = self.generated_at.isoformat(sep=" ")
        return result

    def export_json(
        self,
        path: str | Path = DEFAULT_REPORT_PATH,
    ) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return output_path


@dataclass(slots=True)
class _CheckAccumulator:
    name: str
    examined: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)
    details: str = ""

    def add_error(
        self,
        code: str,
        message: str,
        *,
        source_event_id: int | None = None,
        cycle_id: str | None = None,
        field_name: str | None = None,
        observed: Any = None,
        expected: Any = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity="ERROR",
                code=code,
                message=message,
                source_event_id=source_event_id,
                cycle_id=cycle_id,
                field_name=field_name,
                observed=observed,
                expected=expected,
            )
        )

    def add_warning(
        self,
        code: str,
        message: str,
        *,
        source_event_id: int | None = None,
        cycle_id: str | None = None,
        field_name: str | None = None,
        observed: Any = None,
        expected: Any = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity="WARNING",
                code=code,
                message=message,
                source_event_id=source_event_id,
                cycle_id=cycle_id,
                field_name=field_name,
                observed=observed,
                expected=expected,
            )
        )

    def add_info(
        self,
        code: str,
        message: str,
        *,
        source_event_id: int | None = None,
        cycle_id: str | None = None,
        field_name: str | None = None,
        observed: Any = None,
        expected: Any = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity="INFO",
                code=code,
                message=message,
                source_event_id=source_event_id,
                cycle_id=cycle_id,
                field_name=field_name,
                observed=observed,
                expected=expected,
            )
        )

    def result(self) -> ValidationCheckResult:
        errors = sum(issue.severity == "ERROR" for issue in self.issues)
        warnings = sum(issue.severity == "WARNING" for issue in self.issues)

        return ValidationCheckResult(
            name=self.name,
            passed=errors == 0,
            examined=self.examined,
            errors=errors,
            warnings=warnings,
            details=self.details,
        )


class DatasetValidator:
    """
    Institutional integrity validator for historical research datasets.

    Validation is deterministic and read-only.
    """

    def __init__(
        self,
        *,
        arithmetic_tolerance_usd: float = 1e-8,
        bps_tolerance: float = 1e-8,
        score_tolerance: float = 1e-8,
        flat_tolerance_usd: float = 1e-12,
        warn_on_unknown_decision: bool = True,
    ) -> None:
        numeric_values = {
            "arithmetic_tolerance_usd": arithmetic_tolerance_usd,
            "bps_tolerance": bps_tolerance,
            "score_tolerance": score_tolerance,
            "flat_tolerance_usd": flat_tolerance_usd,
        }

        for name, value in numeric_values.items():
            number = float(value)

            if not math.isfinite(number):
                raise DatasetValidationConfigurationError(
                    f"{name} must be finite."
                )

            if number < 0:
                raise DatasetValidationConfigurationError(
                    f"{name} cannot be negative."
                )

        self.arithmetic_tolerance_usd = float(arithmetic_tolerance_usd)
        self.bps_tolerance = float(bps_tolerance)
        self.score_tolerance = float(score_tolerance)
        self.flat_tolerance_usd = float(flat_tolerance_usd)
        self.warn_on_unknown_decision = bool(warn_on_unknown_decision)

    def validate(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
        *,
        database_path: str | Path = DEFAULT_DATABASE_PATH,
        strict_mode: bool = True,
    ) -> ValidationReport:
        checks: list[ValidationCheckResult] = []
        issues: list[ValidationIssue] = []

        validators = (
            self._check_non_empty,
            self._check_row_count_reconciliation,
            self._check_dataset_chronology,
            self._check_event_chronology,
            self._check_source_identity_reconciliation,
            self._check_cycle_consistency,
            self._check_numeric_finiteness,
            self._check_successful_quote_arithmetic,
            self._check_return_calculations,
            self._check_decisions,
            self._check_outcomes,
            self._check_execution_candidate_rules,
            self._check_flags,
            self._check_score_derivatives,
            self._check_aggregate_reconciliation,
            self._check_information_quality,
        )

        for validator in validators:
            accumulator = validator(dataset, events)
            checks.append(accumulator.result())
            issues.extend(accumulator.issues)

        error_count = sum(issue.severity == "ERROR" for issue in issues)
        warning_count = sum(issue.severity == "WARNING" for issue in issues)
        info_count = sum(issue.severity == "INFO" for issue in issues)

        dataset_summary = dataset.summarize()

        return ValidationReport(
            generated_at=datetime.now(),
            database_path=str(database_path),
            strict_mode=bool(strict_mode),
            dataset_rows=len(dataset),
            event_rows=len(events),
            cycles=dataset_summary.total_cycles,
            unique_assets=dataset_summary.unique_assets,
            unique_tokens=dataset_summary.unique_tokens,
            checks_run=len(checks),
            checks_passed=sum(check.passed for check in checks),
            checks_failed=sum(not check.passed for check in checks),
            error_count=error_count,
            warning_count=warning_count,
            info_count=info_count,
            is_valid=error_count == 0,
            checks=tuple(checks),
            issues=tuple(issues),
        )

    def _check_non_empty(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        check = _CheckAccumulator(name="non_empty_inputs", examined=2)

        if dataset.is_empty:
            check.add_error(
                "EMPTY_DATASET",
                "Historical dataset contains no rows.",
            )

        if events.is_empty:
            check.add_error(
                "EMPTY_EVENT_COLLECTION",
                "Backtest event collection contains no events.",
            )

        check.details = (
            f"dataset_rows={len(dataset)}, event_rows={len(events)}"
        )
        return check

    def _check_row_count_reconciliation(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        check = _CheckAccumulator(
            name="row_count_reconciliation",
            examined=1,
        )

        if len(dataset) != len(events):
            check.add_error(
                "ROW_COUNT_MISMATCH",
                "Dataset and event collection row counts differ.",
                observed=len(events),
                expected=len(dataset),
            )

        check.details = (
            f"dataset_rows={len(dataset)}, event_rows={len(events)}"
        )
        return check

    def _check_dataset_chronology(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del events
        check = _CheckAccumulator(name="dataset_chronology")
        previous: HistoricalDatasetRow | None = None
        seen_ids: set[int] = set()

        for expected_number, row in enumerate(dataset, start=1):
            check.examined += 1

            if row.row_number != expected_number:
                check.add_error(
                    "DATASET_ROW_NUMBER",
                    "Dataset row numbers are not sequential.",
                    source_event_id=row.event_id,
                    observed=row.row_number,
                    expected=expected_number,
                )

            if row.event_id in seen_ids:
                check.add_error(
                    "DUPLICATE_DATASET_EVENT_ID",
                    "Dataset contains a duplicate event ID.",
                    source_event_id=row.event_id,
                )

            if previous is not None:
                if (row.scanned_at, row.event_id) < (
                    previous.scanned_at,
                    previous.event_id,
                ):
                    check.add_error(
                        "DATASET_OUT_OF_ORDER",
                        "Dataset rows are not chronological.",
                        source_event_id=row.event_id,
                        observed=(row.scanned_at.isoformat(), row.event_id),
                        expected=(
                            previous.scanned_at.isoformat(),
                            previous.event_id,
                        ),
                    )

                if row.cycle_number < previous.cycle_number:
                    check.add_error(
                        "DATASET_CYCLE_REVERSED",
                        "Dataset cycle number moved backwards.",
                        source_event_id=row.event_id,
                        observed=row.cycle_number,
                        expected=f">={previous.cycle_number}",
                    )

            seen_ids.add(row.event_id)
            previous = row

        check.details = f"examined_rows={check.examined}"
        return check

    def _check_event_chronology(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="event_chronology")
        previous: BacktestEvent | None = None
        seen_ids: set[int] = set()

        for expected_number, event in enumerate(events, start=1):
            check.examined += 1

            if event.event_number != expected_number:
                check.add_error(
                    "EVENT_NUMBER_SEQUENCE",
                    "Backtest event numbers are not sequential.",
                    source_event_id=event.source_event_id,
                    observed=event.event_number,
                    expected=expected_number,
                )

            if event.source_event_id in seen_ids:
                check.add_error(
                    "DUPLICATE_SOURCE_EVENT_ID",
                    "Backtest events contain a duplicate source ID.",
                    source_event_id=event.source_event_id,
                )

            if previous is not None:
                if (event.timestamp, event.source_event_id) < (
                    previous.timestamp,
                    previous.source_event_id,
                ):
                    check.add_error(
                        "EVENT_OUT_OF_ORDER",
                        "Backtest events are not chronological.",
                        source_event_id=event.source_event_id,
                    )

                if event.cycle_number < previous.cycle_number:
                    check.add_error(
                        "EVENT_CYCLE_REVERSED",
                        "Backtest event cycle number moved backwards.",
                        source_event_id=event.source_event_id,
                    )

            seen_ids.add(event.source_event_id)
            previous = event

        check.details = f"examined_events={check.examined}"
        return check

    def _check_source_identity_reconciliation(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        check = _CheckAccumulator(name="source_identity_reconciliation")
        dataset_by_id = {row.event_id: row for row in dataset}
        event_by_id = {event.source_event_id: event for event in events}
        all_ids = sorted(set(dataset_by_id) | set(event_by_id))

        for source_id in all_ids:
            check.examined += 1
            row = dataset_by_id.get(source_id)
            event = event_by_id.get(source_id)

            if row is None:
                check.add_error(
                    "EVENT_WITHOUT_DATASET_ROW",
                    "Backtest event has no corresponding dataset row.",
                    source_event_id=source_id,
                )
                continue

            if event is None:
                check.add_error(
                    "DATASET_ROW_WITHOUT_EVENT",
                    "Dataset row has no corresponding backtest event.",
                    source_event_id=source_id,
                )
                continue

            comparisons = {
                "timestamp": (event.timestamp, row.scanned_at),
                "cycle_number": (event.cycle_number, row.cycle_number),
                "cycle_id": (event.cycle_id, row.cycle_id),
                "cycle_position": (
                    event.cycle_position,
                    row.cycle_position,
                ),
                "token_key": (event.token_key, row.token_key),
                "asset_key": (event.asset_key, row.asset_key),
                "decision": (event.decision, row.decision),
                "quote_successful": (
                    event.quote_successful,
                    row.quote_successful,
                ),
                "eligible": (event.eligible, row.eligible),
                "profitable": (event.profitable, row.profitable),
            }

            for field_name, (observed, expected) in comparisons.items():
                if observed != expected:
                    check.add_error(
                        "SOURCE_FIELD_MISMATCH",
                        "Backtest event does not match source dataset row.",
                        source_event_id=source_id,
                        field_name=field_name,
                        observed=observed,
                        expected=expected,
                    )

        check.details = f"reconciled_source_ids={check.examined}"
        return check

    def _check_cycle_consistency(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        check = _CheckAccumulator(name="cycle_consistency")
        dataset_groups = dataset.group_by_cycle()
        event_groups = events.group_by_cycle()
        all_cycle_ids = list(
            dict.fromkeys(
                [*dataset_groups.keys(), *event_groups.keys()]
            )
        )

        previous_cycle_number = 0

        for cycle_id in all_cycle_ids:
            check.examined += 1
            rows = dataset_groups.get(cycle_id, ())
            cycle_events = event_groups.get(cycle_id, ())

            if not rows:
                check.add_error(
                    "CYCLE_WITHOUT_DATASET_ROWS",
                    "Cycle exists in events but not in dataset.",
                    cycle_id=cycle_id,
                )
                continue

            if not cycle_events:
                check.add_error(
                    "CYCLE_WITHOUT_EVENTS",
                    "Cycle exists in dataset but not in events.",
                    cycle_id=cycle_id,
                )
                continue

            expected_cycle_number = rows[0].cycle_number
            timestamps = {row.scanned_at for row in rows}
            event_timestamps = {event.timestamp for event in cycle_events}
            dataset_positions = [row.cycle_position for row in rows]
            event_positions = [
                event.cycle_position for event in cycle_events
            ]

            if expected_cycle_number <= previous_cycle_number:
                check.add_error(
                    "NON_INCREASING_CYCLE_NUMBER",
                    "Cycle numbers are not strictly increasing.",
                    cycle_id=cycle_id,
                    observed=expected_cycle_number,
                    expected=f">{previous_cycle_number}",
                )

            if len(timestamps) != 1:
                check.add_error(
                    "DATASET_CYCLE_TIMESTAMP_MIX",
                    "Dataset cycle contains multiple timestamps.",
                    cycle_id=cycle_id,
                    observed=sorted(value.isoformat() for value in timestamps),
                )

            if len(event_timestamps) != 1:
                check.add_error(
                    "EVENT_CYCLE_TIMESTAMP_MIX",
                    "Event cycle contains multiple timestamps.",
                    cycle_id=cycle_id,
                    observed=sorted(
                        value.isoformat() for value in event_timestamps
                    ),
                )

            expected_positions = list(range(1, len(rows) + 1))

            if dataset_positions != expected_positions:
                check.add_error(
                    "DATASET_CYCLE_POSITION_SEQUENCE",
                    "Dataset cycle positions are not sequential.",
                    cycle_id=cycle_id,
                    observed=dataset_positions,
                    expected=expected_positions,
                )

            if event_positions != expected_positions:
                check.add_error(
                    "EVENT_CYCLE_POSITION_SEQUENCE",
                    "Event cycle positions are not sequential.",
                    cycle_id=cycle_id,
                    observed=event_positions,
                    expected=expected_positions,
                )

            if len(rows) != len(cycle_events):
                check.add_error(
                    "CYCLE_SIZE_MISMATCH",
                    "Dataset and event cycle sizes differ.",
                    cycle_id=cycle_id,
                    observed=len(cycle_events),
                    expected=len(rows),
                )

            previous_cycle_number = expected_cycle_number

        check.details = f"cycles_examined={check.examined}"
        return check

    def _check_numeric_finiteness(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="numeric_finiteness")

        numeric_fields = (
            "starting_amount_usd",
            "ending_amount_usd",
            "quoted_profit_usd",
            "estimated_cost_usd",
            "net_profit_usd",
            "gross_return_bps",
            "cost_bps",
            "net_return_bps",
            "market_score",
            "liquidity_score",
            "volume_score",
            "pair_score",
            "intelligence_score",
            "composite_market_score",
            "score_dispersion",
            "minimum_component_score",
            "maximum_component_score",
        )

        for event in events:
            check.examined += 1

            for field_name in numeric_fields:
                value = getattr(event, field_name)

                if not math.isfinite(float(value)):
                    check.add_error(
                        "NON_FINITE_NUMERIC_VALUE",
                        "Backtest event contains a non-finite numeric value.",
                        source_event_id=event.source_event_id,
                        field_name=field_name,
                        observed=value,
                    )

            if (
                event.gross_to_cost_ratio is not None
                and not math.isfinite(float(event.gross_to_cost_ratio))
            ):
                check.add_error(
                    "NON_FINITE_GROSS_TO_COST_RATIO",
                    "gross_to_cost_ratio is non-finite.",
                    source_event_id=event.source_event_id,
                    field_name="gross_to_cost_ratio",
                    observed=event.gross_to_cost_ratio,
                )

        check.details = (
            f"events_examined={check.examined}, "
            f"numeric_fields={len(numeric_fields)}"
        )
        return check

    def _check_successful_quote_arithmetic(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="successful_quote_arithmetic")

        for event in events:
            if not event.quote_successful:
                continue

            check.examined += 1

            calculated_quoted_profit = (
                event.ending_amount_usd - event.starting_amount_usd
            )
            calculated_net_profit = (
                event.quoted_profit_usd - event.estimated_cost_usd
            )

            if not math.isclose(
                event.quoted_profit_usd,
                calculated_quoted_profit,
                rel_tol=0.0,
                abs_tol=self.arithmetic_tolerance_usd,
            ):
                check.add_error(
                    "QUOTED_PROFIT_ARITHMETIC",
                    "Quoted profit does not equal ending minus starting amount.",
                    source_event_id=event.source_event_id,
                    field_name="quoted_profit_usd",
                    observed=event.quoted_profit_usd,
                    expected=calculated_quoted_profit,
                )

            if not math.isclose(
                event.net_profit_usd,
                calculated_net_profit,
                rel_tol=0.0,
                abs_tol=self.arithmetic_tolerance_usd,
            ):
                check.add_error(
                    "NET_PROFIT_ARITHMETIC",
                    "Net profit does not equal quoted profit minus cost.",
                    source_event_id=event.source_event_id,
                    field_name="net_profit_usd",
                    observed=event.net_profit_usd,
                    expected=calculated_net_profit,
                )

        check.details = (
            f"successful_quotes_examined={check.examined}, "
            f"tolerance={self.arithmetic_tolerance_usd}"
        )
        return check

    def _check_return_calculations(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="return_calculations")

        for event in events:
            check.examined += 1

            if event.starting_amount_usd > 0:
                expected_gross_bps = (
                    event.quoted_profit_usd
                    / event.starting_amount_usd
                    * 10_000.0
                )
                expected_cost_bps = (
                    event.estimated_cost_usd
                    / event.starting_amount_usd
                    * 10_000.0
                )
                expected_net_bps = (
                    event.net_profit_usd
                    / event.starting_amount_usd
                    * 10_000.0
                )
            else:
                expected_gross_bps = 0.0
                expected_cost_bps = 0.0
                expected_net_bps = 0.0

            comparisons = {
                "gross_return_bps": (
                    event.gross_return_bps,
                    expected_gross_bps,
                ),
                "cost_bps": (
                    event.cost_bps,
                    expected_cost_bps,
                ),
                "net_return_bps": (
                    event.net_return_bps,
                    expected_net_bps,
                ),
            }

            for field_name, (observed, expected) in comparisons.items():
                if not math.isclose(
                    observed,
                    expected,
                    rel_tol=0.0,
                    abs_tol=self.bps_tolerance,
                ):
                    check.add_error(
                        "RETURN_CALCULATION_MISMATCH",
                        "Stored return metric does not match calculation.",
                        source_event_id=event.source_event_id,
                        field_name=field_name,
                        observed=observed,
                        expected=expected,
                    )

            if event.estimated_cost_usd > 0:
                expected_ratio = (
                    event.quoted_profit_usd / event.estimated_cost_usd
                )

                if event.gross_to_cost_ratio is None:
                    check.add_error(
                        "MISSING_GROSS_TO_COST_RATIO",
                        "Positive cost requires gross_to_cost_ratio.",
                        source_event_id=event.source_event_id,
                        observed=None,
                        expected=expected_ratio,
                    )
                elif not math.isclose(
                    event.gross_to_cost_ratio,
                    expected_ratio,
                    rel_tol=0.0,
                    abs_tol=self.bps_tolerance,
                ):
                    check.add_error(
                        "GROSS_TO_COST_RATIO_MISMATCH",
                        "gross_to_cost_ratio is inconsistent.",
                        source_event_id=event.source_event_id,
                        observed=event.gross_to_cost_ratio,
                        expected=expected_ratio,
                    )
            elif event.gross_to_cost_ratio is not None:
                check.add_warning(
                    "UNEXPECTED_GROSS_TO_COST_RATIO",
                    "Zero cost normally requires a null gross_to_cost_ratio.",
                    source_event_id=event.source_event_id,
                    observed=event.gross_to_cost_ratio,
                    expected=None,
                )

        check.details = (
            f"events_examined={check.examined}, "
            f"bps_tolerance={self.bps_tolerance}"
        )
        return check

    def _check_decisions(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="decision_normalization")
        counts: Counter[str] = Counter()

        for event in events:
            check.examined += 1
            counts[event.decision] += 1

            if event.decision not in VALID_DECISIONS:
                check.add_error(
                    "INVALID_NORMALIZED_DECISION",
                    "Event contains an unsupported normalized decision.",
                    source_event_id=event.source_event_id,
                    field_name="decision",
                    observed=event.decision,
                    expected=sorted(VALID_DECISIONS),
                )

            if (
                event.decision == "QUOTE_ERROR"
                and event.quote_successful
            ):
                check.add_error(
                    "QUOTE_ERROR_DECISION_SUCCESSFUL_QUOTE",
                    "QUOTE_ERROR decision cannot have quote_successful=True.",
                    source_event_id=event.source_event_id,
                )

            if (
                not event.quote_successful
                and event.decision != "QUOTE_ERROR"
            ):
                check.add_error(
                    "FAILED_QUOTE_DECISION_MISMATCH",
                    "Failed quote must normalize to QUOTE_ERROR.",
                    source_event_id=event.source_event_id,
                    observed=event.decision,
                    expected="QUOTE_ERROR",
                )

            if (
                event.decision == "UNKNOWN"
                and self.warn_on_unknown_decision
            ):
                check.add_warning(
                    "UNKNOWN_DECISION",
                    "Event contains an UNKNOWN normalized decision.",
                    source_event_id=event.source_event_id,
                    observed=event.decision_raw,
                )

        check.details = (
            "decision_counts="
            + json.dumps(dict(sorted(counts.items())), ensure_ascii=False)
        )
        return check

    def _check_outcomes(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="outcome_labels")
        counts: Counter[str] = Counter()

        for event in events:
            check.examined += 1
            counts[event.outcome_label] += 1

            if not event.quote_successful:
                expected = "QUOTE_ERROR"
            elif event.net_profit_usd > self.flat_tolerance_usd:
                expected = "POSITIVE"
            elif event.net_profit_usd < -self.flat_tolerance_usd:
                expected = "NEGATIVE"
            else:
                expected = "FLAT"

            if event.outcome_label not in VALID_OUTCOMES:
                check.add_error(
                    "INVALID_OUTCOME_LABEL",
                    "Event contains an unsupported outcome label.",
                    source_event_id=event.source_event_id,
                    observed=event.outcome_label,
                    expected=sorted(VALID_OUTCOMES),
                )

            if event.outcome_label != expected:
                check.add_error(
                    "OUTCOME_LABEL_MISMATCH",
                    "Outcome label does not match quote/profit state.",
                    source_event_id=event.source_event_id,
                    observed=event.outcome_label,
                    expected=expected,
                )

            expected_profitable = (
                event.quote_successful
                and event.net_profit_usd > 0.0
            )

            if event.profitable != expected_profitable:
                check.add_error(
                    "PROFITABLE_FLAG_MISMATCH",
                    "Profitable flag does not match quote/profit state.",
                    source_event_id=event.source_event_id,
                    observed=event.profitable,
                    expected=expected_profitable,
                )

        check.details = (
            "outcome_counts="
            + json.dumps(dict(sorted(counts.items())))
        )
        return check

    def _check_execution_candidate_rules(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="execution_candidate_rules")

        for event in events:
            check.examined += 1

            expected_candidate = (
                event.quote_successful
                and event.eligible
                and event.decision == "EXECUTE"
            )

            if event.execution_candidate != expected_candidate:
                check.add_error(
                    "EXECUTION_CANDIDATE_MISMATCH",
                    "Execution-candidate flag violates builder rules.",
                    source_event_id=event.source_event_id,
                    observed=event.execution_candidate,
                    expected=expected_candidate,
                )

            if event.informational_only == event.execution_candidate:
                check.add_error(
                    "INFORMATIONAL_FLAG_MISMATCH",
                    "informational_only must be the inverse of "
                    "execution_candidate.",
                    source_event_id=event.source_event_id,
                    observed=event.informational_only,
                    expected=not event.execution_candidate,
                )

            expected_rank = {
                "QUOTE_ERROR": 0,
                "UNKNOWN": 0,
                "SKIP": 1,
                "WATCH": 2,
                "EXECUTE": 3,
            }.get(event.decision, 0)

            if event.decision_rank != expected_rank:
                check.add_error(
                    "DECISION_RANK_MISMATCH",
                    "Decision rank does not match normalized decision.",
                    source_event_id=event.source_event_id,
                    observed=event.decision_rank,
                    expected=expected_rank,
                )

        check.details = f"events_examined={check.examined}"
        return check

    def _check_flags(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="derived_flags")

        for event in events:
            check.examined += 1

            expected_has_mint = event.mint is not None
            expected_has_route = bool(
                event.buy_route and event.sell_route
            )
            expected_has_error = bool(event.error)

            comparisons = {
                "has_mint": (event.has_mint, expected_has_mint),
                "has_route": (event.has_route, expected_has_route),
                "has_error": (event.has_error, expected_has_error),
            }

            for field_name, (observed, expected) in comparisons.items():
                if observed != expected:
                    check.add_error(
                        "DERIVED_FLAG_MISMATCH",
                        "Derived boolean flag is inconsistent.",
                        source_event_id=event.source_event_id,
                        field_name=field_name,
                        observed=observed,
                        expected=expected,
                    )

            expected_asset_key = (
                event.mint
                if event.mint
                else f"SYMBOL:{event.token_key}"
            )

            if event.asset_key != expected_asset_key:
                check.add_error(
                    "ASSET_KEY_MISMATCH",
                    "asset_key is inconsistent with mint/token.",
                    source_event_id=event.source_event_id,
                    observed=event.asset_key,
                    expected=expected_asset_key,
                )

            if event.buy_route or event.sell_route:
                expected_route_pair = (
                    f"{event.buy_route or 'UNKNOWN'} || "
                    f"{event.sell_route or 'UNKNOWN'}"
                )
            else:
                expected_route_pair = "UNKNOWN"

            if event.route_pair != expected_route_pair:
                check.add_error(
                    "ROUTE_PAIR_MISMATCH",
                    "route_pair is inconsistent with route fields.",
                    source_event_id=event.source_event_id,
                    observed=event.route_pair,
                    expected=expected_route_pair,
                )

        check.details = f"events_examined={check.examined}"
        return check

    def _check_score_derivatives(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="score_derivatives")

        normalized_weights = (
            0.30,
            0.25,
            0.20,
            0.15,
            0.10,
        )

        for event in events:
            check.examined += 1
            scores = (
                event.market_score,
                event.liquidity_score,
                event.volume_score,
                event.pair_score,
                event.intelligence_score,
            )

            expected_composite = sum(
                score * weight
                for score, weight in zip(scores, normalized_weights)
            )
            expected_dispersion = statistics.pstdev(scores)
            expected_minimum = min(scores)
            expected_maximum = max(scores)

            comparisons = {
                "composite_market_score": (
                    event.composite_market_score,
                    expected_composite,
                ),
                "score_dispersion": (
                    event.score_dispersion,
                    expected_dispersion,
                ),
                "minimum_component_score": (
                    event.minimum_component_score,
                    expected_minimum,
                ),
                "maximum_component_score": (
                    event.maximum_component_score,
                    expected_maximum,
                ),
            }

            for field_name, (observed, expected) in comparisons.items():
                if not math.isclose(
                    observed,
                    expected,
                    rel_tol=0.0,
                    abs_tol=self.score_tolerance,
                ):
                    check.add_error(
                        "SCORE_DERIVATIVE_MISMATCH",
                        "Derived score metric is inconsistent.",
                        source_event_id=event.source_event_id,
                        field_name=field_name,
                        observed=observed,
                        expected=expected,
                    )

            for field_name, score in zip(
                (
                    "market_score",
                    "liquidity_score",
                    "volume_score",
                    "pair_score",
                    "intelligence_score",
                ),
                scores,
            ):
                if score < 0.0 or score > 100.0:
                    check.add_warning(
                        "SCORE_OUTSIDE_EXPECTED_RANGE",
                        "Score is outside the expected 0-100 range.",
                        source_event_id=event.source_event_id,
                        field_name=field_name,
                        observed=score,
                        expected="0 <= score <= 100",
                    )

        check.details = (
            f"events_examined={check.examined}, "
            f"score_tolerance={self.score_tolerance}"
        )
        return check

    def _check_aggregate_reconciliation(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        check = _CheckAccumulator(
            name="aggregate_reconciliation",
            examined=1,
        )
        dataset_summary = dataset.summarize()
        event_summary = events.summarize()

        comparisons = {
            "total_rows": (
                event_summary.total_events,
                dataset_summary.total_rows,
            ),
            "total_cycles": (
                event_summary.total_cycles,
                dataset_summary.total_cycles,
            ),
            "unique_assets": (
                event_summary.unique_assets,
                dataset_summary.unique_assets,
            ),
            "unique_tokens": (
                event_summary.unique_tokens,
                dataset_summary.unique_tokens,
            ),
            "successful_quotes": (
                event_summary.successful_quotes,
                dataset_summary.successful_quotes,
            ),
            "quote_errors": (
                event_summary.quote_errors,
                dataset_summary.quote_errors,
            ),
            "profitable_events": (
                event_summary.profitable_events,
                dataset_summary.profitable_events,
            ),
            "eligible_events": (
                event_summary.eligible_events,
                dataset_summary.eligible_events,
            ),
            "execute_decisions": (
                event_summary.execute_decisions,
                dataset_summary.execute_decisions,
            ),
            "watch_decisions": (
                event_summary.watch_decisions,
                dataset_summary.watch_decisions,
            ),
            "skip_decisions": (
                event_summary.skip_decisions,
                dataset_summary.skip_decisions,
            ),
            "quote_error_decisions": (
                event_summary.quote_error_decisions,
                dataset_summary.quote_error_decisions,
            ),
            "unknown_decisions": (
                event_summary.unknown_decisions,
                dataset_summary.unknown_decisions,
            ),
        }

        for field_name, (observed, expected) in comparisons.items():
            if observed != expected:
                check.add_error(
                    "AGGREGATE_COUNT_MISMATCH",
                    "Dataset and event summaries do not reconcile.",
                    field_name=field_name,
                    observed=observed,
                    expected=expected,
                )

        numeric_comparisons = {
            "average_net_profit_usd": (
                event_summary.average_net_profit_usd,
                dataset_summary.average_net_profit_usd,
            ),
            "median_net_profit_usd": (
                event_summary.median_net_profit_usd,
                dataset_summary.median_net_profit_usd,
            ),
            "best_net_profit_usd": (
                event_summary.best_net_profit_usd,
                dataset_summary.best_net_profit_usd,
            ),
            "worst_net_profit_usd": (
                event_summary.worst_net_profit_usd,
                dataset_summary.worst_net_profit_usd,
            ),
            "average_net_return_bps": (
                event_summary.average_net_return_bps,
                dataset_summary.average_net_return_bps,
            ),
        }

        for field_name, (observed, expected) in numeric_comparisons.items():
            if not math.isclose(
                observed,
                expected,
                rel_tol=0.0,
                abs_tol=self.arithmetic_tolerance_usd,
            ):
                check.add_error(
                    "AGGREGATE_NUMERIC_MISMATCH",
                    "Dataset and event numeric summaries differ.",
                    field_name=field_name,
                    observed=observed,
                    expected=expected,
                )

        expected_informational = (
            event_summary.total_events
            - event_summary.execution_candidates
        )

        if event_summary.informational_events != expected_informational:
            check.add_error(
                "INFORMATIONAL_AGGREGATE_MISMATCH",
                "Informational event count does not reconcile.",
                observed=event_summary.informational_events,
                expected=expected_informational,
            )

        outcome_total = (
            event_summary.positive_outcomes
            + event_summary.negative_outcomes
            + event_summary.flat_outcomes
            + event_summary.quote_error_outcomes
        )

        if outcome_total != event_summary.total_events:
            check.add_error(
                "OUTCOME_TOTAL_MISMATCH",
                "Outcome counts do not sum to total events.",
                observed=outcome_total,
                expected=event_summary.total_events,
            )

        check.details = "dataset and event aggregate summaries compared"
        return check

    def _check_information_quality(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
    ) -> _CheckAccumulator:
        del dataset
        check = _CheckAccumulator(name="information_quality")

        if not events:
            check.details = "no events available"
            return check

        check.examined = len(events)

        missing_mints = sum(not event.has_mint for event in events)
        missing_routes = sum(not event.has_route for event in events)
        unknown_decisions = sum(
            event.decision == "UNKNOWN" for event in events
        )
        quote_errors = sum(
            not event.quote_successful for event in events
        )
        execution_candidates = sum(
            event.execution_candidate for event in events
        )

        if missing_mints:
            check.add_info(
                "ROWS_WITHOUT_MINT",
                "Some events use symbol-based asset keys because mint is absent.",
                observed=missing_mints,
                expected=0,
            )

        if missing_routes:
            check.add_info(
                "ROWS_WITHOUT_COMPLETE_ROUTE",
                "Some events do not contain both route fields.",
                observed=missing_routes,
                expected=0,
            )

        if quote_errors:
            check.add_info(
                "HISTORICAL_QUOTE_ERRORS",
                "Historical quote failures were preserved as observations.",
                observed=quote_errors,
            )

        if unknown_decisions:
            check.add_warning(
                "UNKNOWN_DECISION_AGGREGATE",
                "One or more events have UNKNOWN decisions.",
                observed=unknown_decisions,
                expected=0,
            )

        if execution_candidates == 0:
            check.add_warning(
                "NO_EXECUTION_CANDIDATES",
                "Dataset contains no execution candidates.",
                observed=0,
                expected=">=1",
            )

        check.details = (
            f"missing_mints={missing_mints}, "
            f"missing_routes={missing_routes}, "
            f"quote_errors={quote_errors}, "
            f"unknown_decisions={unknown_decisions}, "
            f"execution_candidates={execution_candidates}"
        )
        return check


def validate_historical_pipeline(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    strict: bool = True,
    validator: DatasetValidator | None = None,
) -> ValidationReport:
    dataset = build_historical_dataset(
        database_path,
        strict=strict,
    )
    events = build_backtest_events(
        database_path,
        strict=strict,
    )

    active_validator = validator or DatasetValidator()

    return active_validator.validate(
        dataset,
        events,
        database_path=database_path,
        strict_mode=strict,
    )


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the complete historical scanner backtest pipeline."
        )
    )
    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
        help="Path to trades.db",
    )
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="Skip malformed source rows during loading",
    )
    parser.add_argument(
        "--show-issues",
        type=int,
        default=20,
        help="Maximum number of validation issues to print",
    )
    parser.add_argument(
        "--errors-only",
        action="store_true",
        help="Print only ERROR validation issues",
    )
    parser.add_argument(
        "--export-json",
        nargs="?",
        const=str(DEFAULT_REPORT_PATH),
        help="Export the complete validation report to JSON",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    strict = not args.non_strict

    try:
        report = validate_historical_pipeline(
            args.database,
            strict=strict,
        )
    except (
        DatasetValidationError,
        HistoricalDatasetError,
        EventBuilderError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nHistorical Dataset Validator")
    print("=" * 76)
    print(f"Database: {report.database_path}")
    print(f"Strict mode: {report.strict_mode}")
    print(f"Dataset rows: {report.dataset_rows}")
    print(f"Backtest events: {report.event_rows}")
    print(f"Cycles: {report.cycles}")
    print(f"Unique assets: {report.unique_assets}")
    print(f"Unique tokens: {report.unique_tokens}")
    print()

    print("Validation Summary")
    print("-" * 76)
    print(f"Checks run: {report.checks_run}")
    print(f"Checks passed: {report.checks_passed}")
    print(f"Checks failed: {report.checks_failed}")
    print(f"Errors: {report.error_count}")
    print(f"Warnings: {report.warning_count}")
    print(f"Information notices: {report.info_count}")
    print(f"VALID: {report.is_valid}")
    print()

    print("Checks")
    print("-" * 76)

    for check in report.checks:
        status = "PASS" if check.passed else "FAIL"
        print(
            f"{status:4} | {check.name:36} | "
            f"examined={check.examined:<5} | "
            f"errors={check.errors:<3} | "
            f"warnings={check.warnings:<3}"
        )

        if check.details:
            print(f"       {check.details}")

    visible_issues = [
        issue
        for issue in report.issues
        if not args.errors_only or issue.severity == "ERROR"
    ]

    issue_limit = max(args.show_issues, 0)

    if visible_issues and issue_limit:
        print()
        print("Validation Issues")
        print("-" * 76)

        for issue in visible_issues[:issue_limit]:
            location_parts: list[str] = []

            if issue.source_event_id is not None:
                location_parts.append(
                    f"event={issue.source_event_id}"
                )

            if issue.cycle_id:
                location_parts.append(
                    f"cycle={issue.cycle_id}"
                )

            if issue.field_name:
                location_parts.append(
                    f"field={issue.field_name}"
                )

            location = (
                " | " + ", ".join(location_parts)
                if location_parts
                else ""
            )

            print(
                f"{issue.severity:7} | {issue.code}{location}"
            )
            print(f"          {issue.message}")

            if issue.observed is not None:
                print(f"          observed={issue.observed!r}")

            if issue.expected is not None:
                print(f"          expected={issue.expected!r}")

        hidden_count = len(visible_issues) - issue_limit

        if hidden_count > 0:
            print(f"\n... {hidden_count} additional issues not shown")

    if args.export_json:
        output_path = report.export_json(args.export_json)
        print(f"\nJSON report exported: {output_path}")

    return 0 if report.is_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())