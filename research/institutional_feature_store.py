"""
Phase 13C — Institutional Feature Store and Backtest Adapter

Consumes the canonical Phase 13B institutional dataset:

    research/institutional_dataset/institutional_events.jsonl
    research/institutional_dataset/institutional_cycles.csv

and produces:

    research/institutional_feature_store/features.csv
    research/institutional_feature_store/features.jsonl
    research/institutional_feature_store/cycles.csv
    research/institutional_feature_store/feature_store_metadata.json
    research/institutional_feature_store/feature_manifest.json
    research/institutional_feature_store/adapter_validation.json

The module also provides a programmatic backtest adapter:

    from research.institutional_feature_store import (
        load_backtest_events,
        load_feature_rows,
    )

    events = load_backtest_events()
    features = load_feature_rows()

Design goals
------------
- Preserve chronology.
- Preserve source type and validation status.
- Prevent future-data leakage.
- Derive only same-row or prior-history features.
- Keep historical and verified-live observations distinguishable.
- Exclude demo rows and noneligible verified-live rows.
- Never modify scanner, wallet, execution, or risk state.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import statistics
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "13C.1.0"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_EVENTS_JSONL = (
    PROJECT_ROOT
    / "research"
    / "institutional_dataset"
    / "institutional_events.jsonl"
)

DEFAULT_CYCLES_CSV = (
    PROJECT_ROOT
    / "research"
    / "institutional_dataset"
    / "institutional_cycles.csv"
)

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "research"
    / "institutional_feature_store"
)

FEATURES_CSV = "features.csv"
FEATURES_JSONL = "features.jsonl"
CYCLES_CSV = "cycles.csv"
METADATA_JSON = "feature_store_metadata.json"
MANIFEST_JSON = "feature_manifest.json"
VALIDATION_JSON = "adapter_validation.json"


class InstitutionalFeatureStoreError(RuntimeError):
    """Base exception for Phase 13C failures."""


@dataclass(frozen=True, slots=True)
class FeatureStoreConfiguration:
    events_jsonl: Path = DEFAULT_EVENTS_JSONL
    cycles_csv: Path = DEFAULT_CYCLES_CSV
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    rolling_window: int = 20
    minimum_rows: int = 1
    minimum_cycles: int = 1
    require_verified_live_gate: bool = True

    def validate(self) -> None:
        if self.rolling_window <= 0:
            raise InstitutionalFeatureStoreError(
                "rolling_window must be positive."
            )

        if self.minimum_rows <= 0:
            raise InstitutionalFeatureStoreError(
                "minimum_rows must be positive."
            )

        if self.minimum_cycles <= 0:
            raise InstitutionalFeatureStoreError(
                "minimum_cycles must be positive."
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
class FeatureStoreSummary:
    generated_at: str
    schema_version: str

    rows: int
    cycles: int
    unique_assets: int
    unique_tokens: int

    historical_rows: int
    verified_live_rows: int
    verified_rows: int
    verified_with_warning_rows: int

    successful_quotes: int
    quote_errors: int
    eligible_rows: int
    profitable_rows: int

    feature_count: int
    label_count: int
    rolling_window: int

    first_event_time: str | None
    last_event_time: str | None

    valid: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


FEATURE_COLUMNS: tuple[str, ...] = (
    "starting_amount_usd",
    "ending_amount_usd",
    "gross_profit_usd",
    "estimated_cost_usd",
    "gross_edge_bps",
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
    "market_score",
    "liquidity_score",
    "volume_score",
    "pair_score",
    "intelligence_score",
    "ai_priority",
    "opportunity_probability",
    "expected_profit_usd",
    "combined_confidence",
    "prediction_confidence",
    "downside_risk",
    "trend_score",
    "stability_score",
    "score_mean",
    "score_std",
    "score_min",
    "score_max",
    "score_range",
    "quote_latency_ms",
    "quote_age_ms",
    "enrichment_quality_score",
    "cycle_position",
    "cycle_size",
    "cycle_elapsed_seconds",
    "scanner_speed_tokens_per_minute",
    "source_is_historical",
    "source_is_verified_live",
    "validation_is_verified",
    "validation_is_verified_with_warning",
    "prior_asset_observations",
    "prior_asset_profitable_count",
    "prior_asset_win_rate",
    "prior_asset_average_net_profit_usd",
    "prior_asset_average_net_edge_bps",
    "prior_asset_average_cost_bps",
    "prior_asset_average_latency_ms",
    "prior_asset_profit_std_usd",
    "prior_asset_best_profit_usd",
    "prior_asset_worst_profit_usd",
    "prior_cycle_observations",
    "prior_cycle_profitable_count",
    "prior_cycle_win_rate",
    "prior_cycle_average_net_profit_usd",
    "prior_cycle_average_net_edge_bps",
    "prior_cycle_average_cost_bps",
    "prior_global_observations",
    "prior_global_profitable_count",
    "prior_global_win_rate",
    "prior_global_average_net_profit_usd",
    "prior_global_average_net_edge_bps",
    "rolling_asset_observations",
    "rolling_asset_win_rate",
    "rolling_asset_average_net_profit_usd",
    "rolling_asset_average_net_edge_bps",
    "rolling_asset_average_cost_bps",
    "rolling_asset_average_latency_ms",
    "rolling_asset_profit_std_usd",
    "rolling_global_observations",
    "rolling_global_win_rate",
    "rolling_global_average_net_profit_usd",
    "rolling_global_average_net_edge_bps",
    "rolling_global_average_cost_bps",
)

LABEL_COLUMNS: tuple[str, ...] = (
    "label_quote_successful",
    "label_eligible",
    "label_profitable",
    "label_outcome",
    "label_decision",
    "label_net_profit_usd",
    "label_net_edge_bps",
)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(
    value: Any,
    default: float = 0.0,
) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    return numeric if math.isfinite(numeric) else default


def safe_int(
    value: Any,
    default: int = 0,
) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }

    return bool(value)


def parse_timestamp(value: Any) -> datetime | None:
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

    return parsed.astimezone(timezone.utc)


def normalized_text(
    value: Any,
    default: str = "",
) -> str:
    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise InstitutionalFeatureStoreError(
            f"Input does not exist: {path}"
        )

    rows: list[dict[str, Any]] = []

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        for line_number, line in enumerate(
            handle,
            start=1,
        ):
            text = line.strip()

            if not text:
                continue

            try:
                payload = json.loads(text)
            except json.JSONDecodeError as error:
                raise InstitutionalFeatureStoreError(
                    f"Invalid JSONL at {path}:{line_number}: {error}"
                ) from error

            if not isinstance(payload, dict):
                raise InstitutionalFeatureStoreError(
                    f"Expected object at {path}:{line_number}."
                )

            rows.append(payload)

    return rows


def load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise InstitutionalFeatureStoreError(
            f"Input does not exist: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as handle:
        return [
            dict(row)
            for row in csv.DictReader(handle)
        ]


def _mean(values: Sequence[float]) -> float:
    return (
        statistics.fmean(values)
        if values
        else 0.0
    )


def _pstdev(values: Sequence[float]) -> float:
    return (
        statistics.pstdev(values)
        if len(values) > 1
        else 0.0
    )


def _history_features(
    history: Sequence[Mapping[str, Any]],
    *,
    prefix: str,
) -> dict[str, Any]:
    successful = [
        row
        for row in history
        if safe_bool(
            row.get("quote_successful")
        )
    ]

    profits = [
        safe_float(
            row.get("net_profit_usd")
        )
        for row in successful
    ]

    net_edges = [
        safe_float(
            row.get("net_edge_bps")
        )
        for row in successful
    ]

    costs = [
        safe_float(
            row.get("total_cost_bps")
        )
        for row in successful
    ]

    latencies = [
        safe_float(
            row.get("quote_latency_ms")
        )
        for row in successful
    ]

    profitable_count = sum(
        profit > 0
        for profit in profits
    )

    observations = len(successful)

    result = {
        f"{prefix}_observations": observations,
        f"{prefix}_profitable_count": profitable_count,
        f"{prefix}_win_rate": (
            profitable_count / observations
            if observations
            else 0.0
        ),
        f"{prefix}_average_net_profit_usd": _mean(profits),
        f"{prefix}_average_net_edge_bps": _mean(net_edges),
        f"{prefix}_average_cost_bps": _mean(costs),
    }

    if prefix in {
        "prior_asset",
        "rolling_asset",
    }:
        result.update(
            {
                f"{prefix}_average_latency_ms": _mean(latencies),
                f"{prefix}_profit_std_usd": _pstdev(profits),
                f"{prefix}_best_profit_usd": (
                    max(profits)
                    if profits
                    else 0.0
                ),
                f"{prefix}_worst_profit_usd": (
                    min(profits)
                    if profits
                    else 0.0
                ),
            }
        )

    return result


def _canonical_sort_key(
    row: Mapping[str, Any],
) -> tuple[Any, ...]:
    parsed = parse_timestamp(
        row.get("event_time")
    )

    timestamp_key = (
        parsed.timestamp()
        if parsed is not None
        else float("inf")
    )

    return (
        timestamp_key,
        normalized_text(
            row.get("source_type")
        ),
        normalized_text(
            row.get("source_cycle_id")
        ),
        safe_int(
            row.get("cycle_position")
        ),
        normalized_text(
            row.get("institutional_event_id")
        ),
    )


def _base_features(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    source_type = normalized_text(
        row.get("source_type")
    ).upper()

    validation_status = normalized_text(
        row.get("validation_status")
    ).upper()

    result = {
        "institutional_event_id": row.get(
            "institutional_event_id"
        ),
        "institutional_schema_version": row.get(
            "institutional_schema_version"
        ),
        "feature_store_schema_version": SCHEMA_VERSION,
        "source_type": source_type,
        "source_dataset": row.get(
            "source_dataset"
        ),
        "source_schema_version": row.get(
            "source_schema_version"
        ),
        "source_row_id": row.get(
            "source_row_id"
        ),
        "source_event_id": row.get(
            "source_event_id"
        ),
        "cycle_id": row.get(
            "source_cycle_id"
        ),
        "cycle_number": safe_int(
            row.get("source_cycle_number")
        ),
        "event_time": row.get(
            "event_time"
        ),
        "scan_time": row.get(
            "scan_time"
        ),
        "token": row.get("token"),
        "token_key": row.get(
            "token_key"
        ),
        "mint": row.get("mint"),
        "asset_key": row.get(
            "asset_key"
        ),
        "buy_route": row.get(
            "buy_route"
        ),
        "sell_route": row.get(
            "sell_route"
        ),
        "route_pair": row.get(
            "route_pair"
        ),
        "validation_status": (
            validation_status
        ),
        "research_eligible": safe_bool(
            row.get("research_eligible")
        ),
    }

    numeric_feature_names = (
        "starting_amount_usd",
        "ending_amount_usd",
        "gross_profit_usd",
        "estimated_cost_usd",
        "gross_edge_bps",
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
        "market_score",
        "liquidity_score",
        "volume_score",
        "pair_score",
        "intelligence_score",
        "ai_priority",
        "opportunity_probability",
        "expected_profit_usd",
        "combined_confidence",
        "prediction_confidence",
        "downside_risk",
        "trend_score",
        "stability_score",
        "score_mean",
        "score_std",
        "score_min",
        "score_max",
        "score_range",
        "quote_latency_ms",
        "quote_age_ms",
        "enrichment_quality_score",
        "cycle_position",
        "cycle_size",
        "cycle_elapsed_seconds",
        "scanner_speed_tokens_per_minute",
    )

    for name in numeric_feature_names:
        result[name] = safe_float(
            row.get(name)
        )

    result.update(
        {
            "source_is_historical": (
                source_type == "HISTORICAL"
            ),
            "source_is_verified_live": (
                source_type == "VERIFIED_LIVE"
            ),
            "validation_is_verified": (
                validation_status == "VERIFIED"
            ),
            "validation_is_verified_with_warning": (
                validation_status
                == "VERIFIED_WITH_WARNING"
            ),
            "is_stale_quote": safe_bool(
                row.get("is_stale_quote")
            ),
            "is_high_latency": safe_bool(
                row.get("is_high_latency")
            ),
            "is_high_price_impact": safe_bool(
                row.get("is_high_price_impact")
            ),
            "is_high_cost": safe_bool(
                row.get("is_high_cost")
            ),
            "is_low_liquidity": safe_bool(
                row.get("is_low_liquidity")
            ),
            "is_low_activity": safe_bool(
                row.get("is_low_activity")
            ),
        }
    )

    return result


def _labels(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    quote_successful = safe_bool(
        row.get("quote_successful")
    )
    net_profit = safe_float(
        row.get("net_profit_usd")
    )

    return {
        "label_quote_successful": quote_successful,
        "label_eligible": safe_bool(
            row.get("eligible")
        ),
        "label_profitable": (
            quote_successful
            and net_profit > 0
        ),
        "label_outcome": normalized_text(
            row.get("outcome"),
            "UNKNOWN",
        ),
        "label_decision": normalized_text(
            row.get("decision"),
            "UNKNOWN",
        ),
        "label_net_profit_usd": net_profit,
        "label_net_edge_bps": safe_float(
            row.get("net_edge_bps")
        ),
    }


class InstitutionalFeatureStoreBuilder:
    def __init__(
        self,
        configuration: FeatureStoreConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or FeatureStoreConfiguration()
        )
        self.configuration.validate()

    def build(
        self,
    ) -> tuple[
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
        FeatureStoreSummary,
        tuple[ValidationCheck, ...],
        dict[str, Any],
    ]:
        institutional_rows = load_jsonl(
            self.configuration.events_jsonl
        )

        cycle_rows = load_csv(
            self.configuration.cycles_csv
        )

        filtered_rows = [
            row
            for row in institutional_rows
            if self._include_row(row)
        ]

        filtered_rows.sort(
            key=_canonical_sort_key
        )

        feature_rows = self._build_feature_rows(
            filtered_rows
        )

        adapted_cycle_rows = (
            self._adapt_cycle_rows(
                cycle_rows,
                feature_rows,
            )
        )

        summary = self._summary(
            feature_rows,
            adapted_cycle_rows,
        )

        checks = self._validate(
            feature_rows,
            adapted_cycle_rows,
            summary,
        )

        valid = all(
            check.passed
            for check in checks
        )

        summary = FeatureStoreSummary(
            **{
                **summary.to_dict(),
                "valid": valid,
            }
        )

        field_catalog = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": utc_now_text(),
            "feature_columns": list(
                FEATURE_COLUMNS
            ),
            "label_columns": list(
                LABEL_COLUMNS
            ),
            "identifier_columns": [
                "institutional_event_id",
                "source_type",
                "source_dataset",
                "source_schema_version",
                "source_row_id",
                "source_event_id",
                "cycle_id",
                "cycle_number",
                "event_time",
                "scan_time",
                "token",
                "token_key",
                "mint",
                "asset_key",
                "buy_route",
                "sell_route",
                "route_pair",
                "validation_status",
                "research_eligible",
            ],
            "leakage_policy": {
                "same_row_features": True,
                "prior_history_features": True,
                "future_rows_used": False,
                "labels_used_as_features": False,
                "rolling_window": (
                    self.configuration.rolling_window
                ),
            },
        }

        return (
            tuple(feature_rows),
            tuple(adapted_cycle_rows),
            summary,
            tuple(checks),
            field_catalog,
        )

    def _include_row(
        self,
        row: Mapping[str, Any],
    ) -> bool:
        source_type = normalized_text(
            row.get("source_type")
        ).upper()

        if source_type == "HISTORICAL":
            return True

        if source_type != "VERIFIED_LIVE":
            return False

        if not self.configuration.require_verified_live_gate:
            return True

        return (
            safe_bool(
                row.get("research_eligible")
            )
            and normalized_text(
                row.get(
                    "validation_status"
                )
            ).upper()
            in {
                "VERIFIED",
                "VERIFIED_WITH_WARNING",
            }
        )

    def _build_feature_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        asset_history: dict[
            str,
            list[Mapping[str, Any]],
        ] = defaultdict(list)

        global_history: list[
            Mapping[str, Any]
        ] = []

        rolling_asset: dict[
            str,
            deque[Mapping[str, Any]],
        ] = defaultdict(
            lambda: deque(
                maxlen=self.configuration.rolling_window
            )
        )

        rolling_global: deque[
            Mapping[str, Any]
        ] = deque(
            maxlen=self.configuration.rolling_window
        )

        completed_cycle_history: list[
            Mapping[str, Any]
        ] = []

        current_cycle_key: tuple[
            str,
            int,
        ] | None = None

        current_cycle_rows: list[
            Mapping[str, Any]
        ] = []

        output: list[dict[str, Any]] = []

        for row in rows:
            cycle_key = (
                normalized_text(
                    row.get(
                        "source_cycle_id"
                    )
                ),
                safe_int(
                    row.get(
                        "source_cycle_number"
                    )
                ),
            )

            if (
                current_cycle_key is not None
                and cycle_key
                != current_cycle_key
            ):
                completed_cycle_history.extend(
                    current_cycle_rows
                )
                current_cycle_rows = []

            current_cycle_key = cycle_key

            asset_key = normalized_text(
                row.get("asset_key"),
                "UNKNOWN",
            )

            prior_asset_rows = (
                asset_history[asset_key]
            )

            prior_cycle_rows = (
                completed_cycle_history
            )

            prior_global_rows = (
                global_history
            )

            rolling_asset_rows = list(
                rolling_asset[asset_key]
            )

            rolling_global_rows = list(
                rolling_global
            )

            feature_row = _base_features(
                row
            )

            feature_row.update(
                _history_features(
                    prior_asset_rows,
                    prefix="prior_asset",
                )
            )

            feature_row.update(
                _history_features(
                    prior_cycle_rows,
                    prefix="prior_cycle",
                )
            )

            feature_row.update(
                _history_features(
                    prior_global_rows,
                    prefix="prior_global",
                )
            )

            feature_row.update(
                _history_features(
                    rolling_asset_rows,
                    prefix="rolling_asset",
                )
            )

            feature_row.update(
                _history_features(
                    rolling_global_rows,
                    prefix="rolling_global",
                )
            )

            feature_row.update(
                _labels(row)
            )

            output.append(
                feature_row
            )

            asset_history[
                asset_key
            ].append(row)

            global_history.append(row)

            rolling_asset[
                asset_key
            ].append(row)

            rolling_global.append(row)

            current_cycle_rows.append(row)

        return output

    @staticmethod
    def _adapt_cycle_rows(
        cycle_rows: Sequence[Mapping[str, Any]],
        feature_rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        feature_counts: dict[
            tuple[str, int],
            int,
        ] = defaultdict(int)

        for row in feature_rows:
            feature_counts[
                (
                    normalized_text(
                        row.get("cycle_id")
                    ),
                    safe_int(
                        row.get("cycle_number")
                    ),
                )
            ] += 1

        adapted: list[
            dict[str, Any]
        ] = []

        for row in cycle_rows:
            cycle_id = normalized_text(
                row.get("source_cycle_id")
            )
            cycle_number = safe_int(
                row.get(
                    "source_cycle_number"
                )
            )

            key = (
                cycle_id,
                cycle_number,
            )

            if key not in feature_counts:
                continue

            adapted.append(
                {
                    **dict(row),
                    "feature_store_schema_version": (
                        SCHEMA_VERSION
                    ),
                    "feature_rows": (
                        feature_counts[key]
                    ),
                }
            )

        return adapted

    @staticmethod
    def _summary(
        rows: Sequence[Mapping[str, Any]],
        cycle_rows: Sequence[Mapping[str, Any]],
    ) -> FeatureStoreSummary:
        event_times = [
            normalized_text(
                row.get("event_time")
            )
            for row in rows
            if normalized_text(
                row.get("event_time")
            )
        ]

        return FeatureStoreSummary(
            generated_at=utc_now_text(),
            schema_version=SCHEMA_VERSION,
            rows=len(rows),
            cycles=len(cycle_rows),
            unique_assets=len(
                {
                    normalized_text(
                        row.get("asset_key")
                    )
                    for row in rows
                    if normalized_text(
                        row.get("asset_key")
                    )
                }
            ),
            unique_tokens=len(
                {
                    normalized_text(
                        row.get("token")
                    )
                    for row in rows
                    if normalized_text(
                        row.get("token")
                    )
                }
            ),
            historical_rows=sum(
                normalized_text(
                    row.get("source_type")
                ).upper()
                == "HISTORICAL"
                for row in rows
            ),
            verified_live_rows=sum(
                normalized_text(
                    row.get("source_type")
                ).upper()
                == "VERIFIED_LIVE"
                for row in rows
            ),
            verified_rows=sum(
                normalized_text(
                    row.get(
                        "validation_status"
                    )
                ).upper()
                == "VERIFIED"
                for row in rows
            ),
            verified_with_warning_rows=sum(
                normalized_text(
                    row.get(
                        "validation_status"
                    )
                ).upper()
                == "VERIFIED_WITH_WARNING"
                for row in rows
            ),
            successful_quotes=sum(
                safe_bool(
                    row.get(
                        "label_quote_successful"
                    )
                )
                for row in rows
            ),
            quote_errors=sum(
                not safe_bool(
                    row.get(
                        "label_quote_successful"
                    )
                )
                for row in rows
            ),
            eligible_rows=sum(
                safe_bool(
                    row.get(
                        "label_eligible"
                    )
                )
                for row in rows
            ),
            profitable_rows=sum(
                safe_bool(
                    row.get(
                        "label_profitable"
                    )
                )
                for row in rows
            ),
            feature_count=len(
                FEATURE_COLUMNS
            ),
            label_count=len(
                LABEL_COLUMNS
            ),
            rolling_window=20,
            first_event_time=(
                min(event_times)
                if event_times
                else None
            ),
            last_event_time=(
                max(event_times)
                if event_times
                else None
            ),
            valid=False,
        )

    def _validate(
        self,
        rows: Sequence[Mapping[str, Any]],
        cycle_rows: Sequence[Mapping[str, Any]],
        summary: FeatureStoreSummary,
    ) -> list[ValidationCheck]:
        checks: list[
            ValidationCheck
        ] = []

        checks.append(
            ValidationCheck(
                name="minimum_rows",
                passed=(
                    len(rows)
                    >= self.configuration.minimum_rows
                ),
                observed=len(rows),
                expected=(
                    f">= {self.configuration.minimum_rows}"
                ),
                details="Feature-store row minimum.",
            )
        )

        checks.append(
            ValidationCheck(
                name="minimum_cycles",
                passed=(
                    len(cycle_rows)
                    >= self.configuration.minimum_cycles
                ),
                observed=len(cycle_rows),
                expected=(
                    f">= {self.configuration.minimum_cycles}"
                ),
                details="Feature-store cycle minimum.",
            )
        )

        duplicate_ids = (
            len(rows)
            - len(
                {
                    row[
                        "institutional_event_id"
                    ]
                    for row in rows
                }
            )
        )

        checks.append(
            ValidationCheck(
                name="unique_event_ids",
                passed=duplicate_ids == 0,
                observed=duplicate_ids,
                expected=0,
                details=(
                    "institutional_event_id must be unique."
                ),
            )
        )

        blank_asset_keys = sum(
            not normalized_text(
                row.get("asset_key")
            )
            for row in rows
        )

        checks.append(
            ValidationCheck(
                name="asset_key_completeness",
                passed=blank_asset_keys == 0,
                observed=blank_asset_keys,
                expected=0,
                details=(
                    "All feature rows require asset_key."
                ),
            )
        )

        invalid_live_rows = sum(
            normalized_text(
                row.get("source_type")
            ).upper()
            == "VERIFIED_LIVE"
            and (
                not safe_bool(
                    row.get("research_eligible")
                )
                or normalized_text(
                    row.get(
                        "validation_status"
                    )
                ).upper()
                not in {
                    "VERIFIED",
                    "VERIFIED_WITH_WARNING",
                }
            )
            for row in rows
        )

        checks.append(
            ValidationCheck(
                name="verified_live_gate",
                passed=invalid_live_rows == 0,
                observed=invalid_live_rows,
                expected=0,
                details=(
                    "Verified-live rows must pass the "
                    "research eligibility gate."
                ),
            )
        )

        chronology_errors = 0
        previous: datetime | None = None

        for row in rows:
            current = parse_timestamp(
                row.get("event_time")
            )

            if current is None:
                continue

            if (
                previous is not None
                and current < previous
            ):
                chronology_errors += 1

            previous = current

        checks.append(
            ValidationCheck(
                name="chronology",
                passed=chronology_errors == 0,
                observed=chronology_errors,
                expected=0,
                details=(
                    "Feature rows must be chronological."
                ),
            )
        )

        missing_features = 0
        nonfinite_features = 0

        for row in rows:
            for field in FEATURE_COLUMNS:
                if field not in row:
                    missing_features += 1
                    continue

                value = row[field]

                if isinstance(value, bool):
                    continue

                if value is None:
                    missing_features += 1
                    continue

                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    nonfinite_features += 1
                    continue

                if not math.isfinite(numeric):
                    nonfinite_features += 1

        checks.append(
            ValidationCheck(
                name="feature_completeness",
                passed=missing_features == 0,
                observed=missing_features,
                expected=0,
                details=(
                    "All declared feature columns must exist."
                ),
            )
        )

        checks.append(
            ValidationCheck(
                name="feature_finiteness",
                passed=nonfinite_features == 0,
                observed=nonfinite_features,
                expected=0,
                details=(
                    "All numeric feature values must be finite."
                ),
            )
        )

        missing_labels = sum(
            field not in row
            for row in rows
            for field in LABEL_COLUMNS
        )

        checks.append(
            ValidationCheck(
                name="label_completeness",
                passed=missing_labels == 0,
                observed=missing_labels,
                expected=0,
                details=(
                    "All declared label columns must exist."
                ),
            )
        )

        future_leakage_errors = 0

        for row in rows:
            prior_count = safe_int(
                row.get(
                    "prior_global_observations"
                )
            )

            rolling_count = safe_int(
                row.get(
                    "rolling_global_observations"
                )
            )

            if prior_count < rolling_count:
                future_leakage_errors += 1

            if (
                rolling_count
                > self.configuration.rolling_window
            ):
                future_leakage_errors += 1

        checks.append(
            ValidationCheck(
                name="history_window_consistency",
                passed=(
                    future_leakage_errors == 0
                ),
                observed=future_leakage_errors,
                expected=0,
                details=(
                    "Rolling counts cannot exceed prior counts "
                    "or configured window."
                ),
            )
        )

        feature_cycle_ids = {
            (
                normalized_text(
                    row.get("cycle_id")
                ),
                safe_int(
                    row.get("cycle_number")
                ),
            )
            for row in rows
        }

        cycle_ids = {
            (
                normalized_text(
                    row.get("source_cycle_id")
                ),
                safe_int(
                    row.get(
                        "source_cycle_number"
                    )
                ),
            )
            for row in cycle_rows
        }

        checks.append(
            ValidationCheck(
                name="cycle_reconciliation",
                passed=(
                    feature_cycle_ids
                    == cycle_ids
                ),
                observed=len(
                    feature_cycle_ids
                ),
                expected=len(
                    cycle_ids
                ),
                details=(
                    "Feature rows and cycle adapter rows "
                    "must share identical cycle identities."
                ),
            )
        )

        return checks


def load_feature_rows(
    path: str | Path = (
        DEFAULT_OUTPUT_DIRECTORY
        / FEATURES_JSONL
    ),
) -> list[dict[str, Any]]:
    """Load generated institutional feature rows."""

    return load_jsonl(
        Path(path)
    )


def load_backtest_events(
    path: str | Path = (
        DEFAULT_OUTPUT_DIRECTORY
        / FEATURES_JSONL
    ),
    *,
    include_historical: bool = True,
    include_verified_live: bool = True,
) -> list[dict[str, Any]]:
    """
    Convert feature-store rows into a stable backtest-event interface.

    Returned fields include:
        event_id
        cycle_id
        cycle_number
        timestamp
        token
        asset_key
        source_type
        validation_status
        quote_successful
        eligible
        decision
        outcome
        starting_amount_usd
        ending_amount_usd
        gross_profit_usd
        estimated_cost_usd
        net_profit_usd
        gross_edge_bps
        net_edge_bps
        total_cost_bps
        features
        labels
    """

    rows = load_feature_rows(
        path
    )

    events: list[
        dict[str, Any]
    ] = []

    for row in rows:
        source_type = normalized_text(
            row.get("source_type")
        ).upper()

        if (
            source_type == "HISTORICAL"
            and not include_historical
        ):
            continue

        if (
            source_type == "VERIFIED_LIVE"
            and not include_verified_live
        ):
            continue

        features = {
            name: row[name]
            for name in FEATURE_COLUMNS
        }

        labels = {
            name: row[name]
            for name in LABEL_COLUMNS
        }

        events.append(
            {
                "event_id": row[
                    "institutional_event_id"
                ],
                "cycle_id": row[
                    "cycle_id"
                ],
                "cycle_number": safe_int(
                    row["cycle_number"]
                ),
                "timestamp": row[
                    "event_time"
                ],
                "token": row["token"],
                "asset_key": row[
                    "asset_key"
                ],
                "source_type": source_type,
                "validation_status": row[
                    "validation_status"
                ],
                "research_eligible": safe_bool(
                    row[
                        "research_eligible"
                    ]
                ),
                "quote_successful": safe_bool(
                    row[
                        "label_quote_successful"
                    ]
                ),
                "eligible": safe_bool(
                    row[
                        "label_eligible"
                    ]
                ),
                "decision": row[
                    "label_decision"
                ],
                "outcome": row[
                    "label_outcome"
                ],
                "starting_amount_usd": safe_float(
                    row[
                        "starting_amount_usd"
                    ]
                ),
                "ending_amount_usd": safe_float(
                    row[
                        "ending_amount_usd"
                    ]
                ),
                "gross_profit_usd": safe_float(
                    row[
                        "gross_profit_usd"
                    ]
                ),
                "estimated_cost_usd": safe_float(
                    row[
                        "estimated_cost_usd"
                    ]
                ),
                "net_profit_usd": safe_float(
                    row[
                        "label_net_profit_usd"
                    ]
                ),
                "gross_edge_bps": safe_float(
                    row[
                        "gross_edge_bps"
                    ]
                ),
                "net_edge_bps": safe_float(
                    row[
                        "label_net_edge_bps"
                    ]
                ),
                "total_cost_bps": safe_float(
                    row[
                        "total_cost_bps"
                    ]
                ),
                "features": features,
                "labels": labels,
            }
        )

    events.sort(
        key=lambda event: (
            parse_timestamp(
                event.get("timestamp")
            )
            or datetime.max.replace(
                tzinfo=timezone.utc
            ),
            event["cycle_id"],
            event["event_id"],
        )
    )

    return events


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    if not rows:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    fields: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for field in row.keys():
            if field not in seen:
                seen.add(field)
                fields.append(field)

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
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


def write_jsonl(
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(
                1024 * 1024
            ),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def export_feature_store(
    *,
    feature_rows: Sequence[Mapping[str, Any]],
    cycle_rows: Sequence[Mapping[str, Any]],
    summary: FeatureStoreSummary,
    checks: Sequence[ValidationCheck],
    field_catalog: Mapping[str, Any],
    configuration: FeatureStoreConfiguration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    features_csv = output / FEATURES_CSV
    features_jsonl = output / FEATURES_JSONL
    cycles_csv = output / CYCLES_CSV
    metadata_json = output / METADATA_JSON
    manifest_json = output / MANIFEST_JSON
    validation_json = output / VALIDATION_JSON

    destinations = (
        features_csv,
        features_jsonl,
        cycles_csv,
        metadata_json,
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
            raise InstitutionalFeatureStoreError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    write_csv(
        features_csv,
        feature_rows,
    )

    write_jsonl(
        features_jsonl,
        feature_rows,
    )

    write_csv(
        cycles_csv,
        cycle_rows,
    )

    metadata_json.write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": utc_now_text(),
                "summary": summary.to_dict(),
                "field_catalog": dict(
                    field_catalog
                ),
                "source_files": {
                    "institutional_events": str(
                        configuration.events_jsonl
                    ),
                    "institutional_cycles": str(
                        configuration.cycles_csv
                    ),
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
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

    file_metadata: dict[
        str,
        Any,
    ] = {}

    for path, row_count in (
        (features_csv, len(feature_rows)),
        (features_jsonl, len(feature_rows)),
        (cycles_csv, len(cycle_rows)),
        (metadata_json, None),
        (validation_json, len(checks)),
    ):
        file_metadata[path.name] = {
            "path": str(path),
            "rows": row_count,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }

    manifest_json.write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": utc_now_text(),
                "summary": summary.to_dict(),
                "governance": {
                    "future_data_used": False,
                    "labels_used_as_features": False,
                    "verified_live_gate_enforced": (
                        configuration
                        .require_verified_live_gate
                    ),
                    "demo_rows_included": False,
                    "raw_trading_state_modified": False,
                },
                "files": file_metadata,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return destinations


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Phase 13C institutional feature store "
            "and backtest adapter."
        )
    )

    parser.add_argument(
        "--events-jsonl",
        default=str(
            DEFAULT_EVENTS_JSONL
        ),
    )

    parser.add_argument(
        "--cycles-csv",
        default=str(
            DEFAULT_CYCLES_CSV
        ),
    )

    parser.add_argument(
        "--output-directory",
        default=str(
            DEFAULT_OUTPUT_DIRECTORY
        ),
    )

    parser.add_argument(
        "--rolling-window",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--minimum-rows",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--minimum-cycles",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--disable-verified-live-gate",
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

    configuration = FeatureStoreConfiguration(
        events_jsonl=Path(
            args.events_jsonl
        ),
        cycles_csv=Path(
            args.cycles_csv
        ),
        output_directory=Path(
            args.output_directory
        ),
        overwrite=(
            not args.no_overwrite
        ),
        rolling_window=(
            args.rolling_window
        ),
        minimum_rows=(
            args.minimum_rows
        ),
        minimum_cycles=(
            args.minimum_cycles
        ),
        require_verified_live_gate=(
            not args.disable_verified_live_gate
        ),
    )

    try:
        (
            feature_rows,
            cycle_rows,
            summary,
            checks,
            field_catalog,
        ) = InstitutionalFeatureStoreBuilder(
            configuration
        ).build()

        output_paths = export_feature_store(
            feature_rows=feature_rows,
            cycle_rows=cycle_rows,
            summary=summary,
            checks=checks,
            field_catalog=field_catalog,
            configuration=configuration,
        )

    except (
        InstitutionalFeatureStoreError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 13C — Institutional "
        "Feature Store and Backtest Adapter"
    )
    print("=" * 80)

    print("Feature Store")
    print("-" * 80)
    print(
        f"Rows: {summary.rows}"
    )
    print(
        f"Cycles: {summary.cycles}"
    )
    print(
        f"Unique assets: {summary.unique_assets}"
    )
    print(
        f"Unique tokens: {summary.unique_tokens}"
    )
    print(
        "Historical / verified-live rows: "
        f"{summary.historical_rows} / "
        f"{summary.verified_live_rows}"
    )
    print(
        "VERIFIED / VERIFIED_WITH_WARNING: "
        f"{summary.verified_rows} / "
        f"{summary.verified_with_warning_rows}"
    )
    print(
        "Successful quotes / errors: "
        f"{summary.successful_quotes} / "
        f"{summary.quote_errors}"
    )
    print(
        "Eligible / profitable: "
        f"{summary.eligible_rows} / "
        f"{summary.profitable_rows}"
    )
    print(
        "Feature / label columns: "
        f"{summary.feature_count} / "
        f"{summary.label_count}"
    )
    print(
        "Rolling history window: "
        f"{summary.rolling_window}"
    )
    print()

    print("Validation")
    print("-" * 80)

    for check in checks:
        print(
            f"{'PASS' if check.passed else 'FAIL'} | "
            f"{check.name:34} | "
            f"observed={check.observed} | "
            f"expected={check.expected}"
        )

    print()
    print(
        f"VALID: {summary.valid}"
    )
    print()

    print("Backtest Adapter")
    print("-" * 80)
    print(
        "Import: "
        "from research.institutional_feature_store "
        "import load_backtest_events"
    )
    print(
        "Generated events are chronological and preserve "
        "source and validation status."
    )
    print()

    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(path)

    return 0 if summary.valid else 2


if __name__ == "__main__":
    raise SystemExit(main())