"""
Phase 13B — Institutional Dataset Merger

Merges:
    1. Historical backtest events
    2. Verified live scanner features

into one canonical institutional research dataset while preserving:

- source identity
- source schema/version
- cycle identity
- validation status
- chronology
- feature completeness
- demo-trade exclusion
- duplicate prevention
- source-specific raw fields

Default inputs
--------------
Historical:
    backtesting/exports/historical_backtest_events.jsonl

Verified live:
    research/verified_dataset/verified_live_features.jsonl

Outputs
-------
research/institutional_dataset/
    institutional_events.csv
    institutional_events.jsonl
    institutional_cycles.csv
    institutional_dataset_manifest.json
    institutional_dataset_validation.json
    source_field_catalog.json

Important governance
--------------------
- This module never writes to scanner or trading tables.
- It never includes demo_paper_trades.
- It never changes labels, profits, decisions, or validation statuses.
- It preserves both normalized canonical fields and source-specific raw payloads.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "13B.1.0"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_HISTORICAL_JSONL = (
    PROJECT_ROOT
    / "backtesting"
    / "exports"
    / "historical_backtest_events.jsonl"
)

DEFAULT_VERIFIED_LIVE_JSONL = (
    PROJECT_ROOT
    / "research"
    / "verified_dataset"
    / "verified_live_features.jsonl"
)

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "research"
    / "institutional_dataset"
)

EVENTS_CSV = "institutional_events.csv"
EVENTS_JSONL = "institutional_events.jsonl"
CYCLES_CSV = "institutional_cycles.csv"
MANIFEST_JSON = "institutional_dataset_manifest.json"
VALIDATION_JSON = "institutional_dataset_validation.json"
FIELD_CATALOG_JSON = "source_field_catalog.json"


class InstitutionalDatasetError(RuntimeError):
    """Base exception for Phase 13B failures."""


@dataclass(frozen=True, slots=True)
class MergerConfiguration:
    historical_jsonl: Path = DEFAULT_HISTORICAL_JSONL
    verified_live_jsonl: Path = DEFAULT_VERIFIED_LIVE_JSONL
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    include_historical: bool = True
    include_verified_live: bool = True

    minimum_total_rows: int = 1
    minimum_total_cycles: int = 1
    minimum_verified_live_rows: int = 0
    minimum_verified_live_cycles: int = 0

    arithmetic_tolerance_usd: float = 1e-8
    bps_tolerance: float = 1e-6

    def validate(self) -> None:
        if not self.include_historical and not self.include_verified_live:
            raise InstitutionalDatasetError(
                "At least one source must be enabled."
            )

        if self.minimum_total_rows <= 0:
            raise InstitutionalDatasetError(
                "minimum_total_rows must be positive."
            )

        if self.minimum_total_cycles <= 0:
            raise InstitutionalDatasetError(
                "minimum_total_cycles must be positive."
            )

        if self.minimum_verified_live_rows < 0:
            raise InstitutionalDatasetError(
                "minimum_verified_live_rows cannot be negative."
            )

        if self.minimum_verified_live_cycles < 0:
            raise InstitutionalDatasetError(
                "minimum_verified_live_cycles cannot be negative."
            )

        if self.arithmetic_tolerance_usd < 0:
            raise InstitutionalDatasetError(
                "arithmetic_tolerance_usd cannot be negative."
            )

        if self.bps_tolerance < 0:
            raise InstitutionalDatasetError(
                "bps_tolerance cannot be negative."
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

    total_rows: int
    total_cycles: int
    unique_assets: int
    unique_tokens: int

    historical_rows: int
    historical_cycles: int
    verified_live_rows: int
    verified_live_cycles: int

    verified_rows: int
    verified_with_warning_rows: int
    unvalidated_historical_rows: int

    successful_quotes: int
    quote_errors: int
    eligible_observations: int
    profitable_observations: int

    average_net_profit_usd: float
    best_net_profit_usd: float
    worst_net_profit_usd: float
    average_total_cost_bps: float
    average_quote_latency_ms: float

    duplicate_rows_removed: int
    demo_rows_excluded: int
    invalid_rows_excluded: int

    first_event_time: str | None
    last_event_time: str | None

    valid: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


CANONICAL_FIELD_ORDER: tuple[str, ...] = (
    "institutional_event_id",
    "institutional_schema_version",
    "source_type",
    "source_dataset",
    "source_schema_version",
    "source_row_id",
    "source_event_id",
    "source_cycle_id",
    "source_cycle_number",
    "event_time",
    "scan_time",
    "token",
    "token_key",
    "mint",
    "asset_key",
    "decision",
    "outcome",
    "quote_successful",
    "eligible",
    "research_eligible",
    "validation_status",
    "validation_quality_score",
    "validation_errors",
    "validation_warnings",
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
    "buy_route",
    "sell_route",
    "route_pair",
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
    "is_stale_quote",
    "is_high_latency",
    "is_high_price_impact",
    "is_high_cost",
    "is_low_liquidity",
    "is_low_activity",
    "enrichment_quality_score",
    "cycle_position",
    "cycle_size",
    "cycle_elapsed_seconds",
    "scanner_speed_tokens_per_minute",
    "raw_source_payload",
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


def first_present(
    source: Mapping[str, Any],
    *keys: str,
    default: Any = None,
) -> Any:
    for key in keys:
        if key in source and source[key] is not None:
            return source[key]

    return default


def text_or_none(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def normalized_text(
    value: Any,
    default: str = "",
) -> str:
    text = text_or_none(value)
    return text if text is not None else default


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


def canonical_timestamp(value: Any) -> str | None:
    parsed = parse_timestamp(value)

    if parsed is None:
        return text_or_none(value)

    return parsed.isoformat(
        timespec="milliseconds"
    )


def calculate_bps(
    amount: float,
    starting_amount: float,
) -> float:
    if starting_amount <= 0:
        return 0.0

    return (
        amount
        / starting_amount
        * 10_000.0
    )


def derive_asset_key(
    row: Mapping[str, Any],
) -> str:
    existing = normalized_text(
        first_present(
            row,
            "asset_key",
            default="",
        )
    )

    if existing:
        return existing

    mint = normalized_text(
        first_present(
            row,
            "mint",
            "token_mint",
            default="",
        )
    )

    if mint:
        return f"mint:{mint}"

    token = normalized_text(
        first_present(
            row,
            "token",
            "symbol",
            "token_symbol",
            default="UNKNOWN",
        ),
        "UNKNOWN",
    ).upper()

    return f"symbol:{token}"


def normalize_decision(value: Any) -> str:
    text = normalized_text(
        value,
        "UNKNOWN",
    ).upper()

    if "QUOTE ERROR" in text:
        return "QUOTE_ERROR"

    if "EXECUTE" in text or "TEST FURTHER" in text:
        return "EXECUTE"

    if "WATCH" in text:
        return "WATCH"

    if "SKIP" in text:
        return "SKIP"

    return text


def normalize_outcome(
    quote_successful: bool,
    net_profit_usd: float,
) -> str:
    if not quote_successful:
        return "QUOTE_ERROR"

    if net_profit_usd > 0:
        return "POSITIVE"

    if net_profit_usd < 0:
        return "NEGATIVE"

    return "FLAT"


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise InstitutionalDatasetError(
            f"Input file does not exist: {path}"
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
                raise InstitutionalDatasetError(
                    f"Invalid JSONL at {path}:{line_number}: {error}"
                ) from error

            if not isinstance(payload, dict):
                raise InstitutionalDatasetError(
                    f"Expected JSON object at {path}:{line_number}."
                )

            rows.append(payload)

    return rows


def normalize_historical_row(
    row: Mapping[str, Any],
    position: int,
) -> dict[str, Any]:
    starting = safe_float(
        first_present(
            row,
            "starting_amount_usd",
            "starting_amount",
            default=0.0,
        )
    )

    ending = safe_float(
        first_present(
            row,
            "ending_amount_usd",
            "ending_amount",
            default=0.0,
        )
    )

    gross = safe_float(
        first_present(
            row,
            "gross_profit_usd",
            "gross_profit",
            "quoted_profit",
            "quoted_profit_usd",
            default=ending - starting,
        )
    )

    cost = safe_float(
        first_present(
            row,
            "estimated_cost_usd",
            "estimated_cost",
            "total_cost_usd",
            default=0.0,
        )
    )

    net = safe_float(
        first_present(
            row,
            "net_profit_usd",
            "net_profit",
            default=gross - cost,
        )
    )

    quote_successful = safe_bool(
        first_present(
            row,
            "quote_successful",
            default=(
                normalize_decision(
                    row.get("decision")
                )
                != "QUOTE_ERROR"
            ),
        )
    )

    token = normalized_text(
        first_present(
            row,
            "token",
            "symbol",
            default="UNKNOWN",
        ),
        "UNKNOWN",
    )

    cycle_id = normalized_text(
        first_present(
            row,
            "cycle_id",
            "source_cycle_id",
            default=f"HISTORICAL-CYCLE-{position:08d}",
        )
    )

    event_time = canonical_timestamp(
        first_present(
            row,
            "event_time",
            "scan_time",
            "timestamp",
            "created_at",
            default=None,
        )
    )

    canonical = {
        "institutional_event_id": (
            f"HISTORICAL:{cycle_id}:{position:08d}"
        ),
        "institutional_schema_version": SCHEMA_VERSION,
        "source_type": "HISTORICAL",
        "source_dataset": "historical_backtest_events",
        "source_schema_version": normalized_text(
            first_present(
                row,
                "schema_version",
                "dataset_schema_version",
                default="UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "source_row_id": first_present(
            row,
            "id",
            "source_event_id",
            default=position,
        ),
        "source_event_id": first_present(
            row,
            "source_event_id",
            "event_id",
            "id",
            default=position,
        ),
        "source_cycle_id": cycle_id,
        "source_cycle_number": safe_int(
            first_present(
                row,
                "cycle_number",
                default=0,
            )
        ),
        "event_time": event_time,
        "scan_time": event_time,
        "token": token,
        "token_key": normalized_text(
            first_present(
                row,
                "token_key",
                default=token.upper(),
            ),
            token.upper(),
        ),
        "mint": text_or_none(
            first_present(
                row,
                "mint",
                "token_mint",
                default=None,
            )
        ),
        "asset_key": derive_asset_key(row),
        "decision": normalize_decision(
            row.get("decision")
        ),
        "outcome": normalize_outcome(
            quote_successful,
            net,
        ),
        "quote_successful": quote_successful,
        "eligible": safe_bool(
            row.get("eligible")
        ),
        "research_eligible": False,
        "validation_status": "HISTORICAL_UNVALIDATED",
        "validation_quality_score": 0.0,
        "validation_errors": 0,
        "validation_warnings": 0,
        "starting_amount_usd": starting,
        "ending_amount_usd": ending,
        "gross_profit_usd": gross,
        "estimated_cost_usd": cost,
        "net_profit_usd": net,
        "gross_edge_bps": safe_float(
            first_present(
                row,
                "gross_edge_bps",
                "gross_return_bps",
                default=calculate_bps(
                    gross,
                    starting,
                ),
            )
        ),
        "net_edge_bps": safe_float(
            first_present(
                row,
                "net_edge_bps",
                "net_return_bps",
                default=calculate_bps(
                    net,
                    starting,
                ),
            )
        ),
        "total_cost_bps": safe_float(
            first_present(
                row,
                "total_cost_bps",
                "cost_bps",
                default=calculate_bps(
                    cost,
                    starting,
                ),
            )
        ),
        "slippage_bps": safe_float(
            row.get("slippage_bps")
        ),
        "price_impact_bps": safe_float(
            row.get("price_impact_bps")
        ),
        "network_fee_usd": safe_float(
            row.get("network_fee_usd")
        ),
        "dex_fee_usd": safe_float(
            row.get("dex_fee_usd")
        ),
        "slippage_cost_usd": safe_float(
            row.get("slippage_cost_usd")
        ),
        "liquidity_usd": safe_float(
            first_present(
                row,
                "liquidity_usd",
                default=0.0,
            )
        ),
        "volume_24h_usd": safe_float(
            first_present(
                row,
                "volume_24h_usd",
                default=0.0,
            )
        ),
        "volume_liquidity_ratio": safe_float(
            row.get("volume_liquidity_ratio")
        ),
        "buy_route": normalized_text(
            first_present(
                row,
                "buy_route",
                default="UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "sell_route": normalized_text(
            first_present(
                row,
                "sell_route",
                default="UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "route_pair": normalized_text(
            first_present(
                row,
                "route_pair",
                default=(
                    f"{normalized_text(row.get('buy_route'), 'UNKNOWN')}"
                    f"->{normalized_text(row.get('sell_route'), 'UNKNOWN')}"
                ),
            )
        ),
        "route_hops": safe_int(
            row.get("route_hops")
        ),
        "dex_count": safe_int(
            row.get("dex_count")
        ),
        "market_score": safe_float(
            row.get("market_score")
        ),
        "liquidity_score": safe_float(
            row.get("liquidity_score")
        ),
        "volume_score": safe_float(
            row.get("volume_score")
        ),
        "pair_score": safe_float(
            row.get("pair_score")
        ),
        "intelligence_score": safe_float(
            first_present(
                row,
                "intelligence_score",
                "ai_opportunity_score",
                default=0.0,
            )
        ),
        "ai_priority": safe_float(
            row.get("ai_priority")
        ),
        "opportunity_probability": safe_float(
            row.get("opportunity_probability")
        ),
        "expected_profit_usd": safe_float(
            row.get("expected_profit_usd")
        ),
        "combined_confidence": safe_float(
            row.get("combined_confidence")
        ),
        "prediction_confidence": safe_float(
            row.get("prediction_confidence")
        ),
        "downside_risk": safe_float(
            first_present(
                row,
                "downside_risk",
                "downside_risk_score",
                default=0.0,
            )
        ),
        "trend_score": safe_float(
            row.get("trend_score")
        ),
        "stability_score": safe_float(
            row.get("stability_score")
        ),
        "score_mean": safe_float(
            row.get("score_mean")
        ),
        "score_std": safe_float(
            row.get("score_std")
        ),
        "score_min": safe_float(
            row.get("score_min")
        ),
        "score_max": safe_float(
            row.get("score_max")
        ),
        "score_range": safe_float(
            row.get("score_range")
        ),
        "quote_latency_ms": safe_float(
            row.get("quote_latency_ms")
        ),
        "quote_age_ms": safe_float(
            row.get("quote_age_ms")
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
        "enrichment_quality_score": safe_float(
            row.get("enrichment_quality_score")
        ),
        "cycle_position": safe_int(
            row.get("cycle_position")
        ),
        "cycle_size": safe_int(
            row.get("cycle_size")
        ),
        "cycle_elapsed_seconds": safe_float(
            row.get("cycle_elapsed_seconds")
        ),
        "scanner_speed_tokens_per_minute": safe_float(
            row.get("scanner_speed_tokens_per_minute")
        ),
        "raw_source_payload": json.dumps(
            dict(row),
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        ),
    }

    return canonical


def normalize_verified_live_row(
    row: Mapping[str, Any],
    position: int,
) -> dict[str, Any]:
    starting = safe_float(
        row.get("starting_amount_usd")
    )
    ending = safe_float(
        row.get("ending_amount_usd")
    )
    gross = safe_float(
        row.get("gross_profit_usd")
    )
    cost = safe_float(
        row.get("estimated_cost_usd")
    )
    net = safe_float(
        row.get("net_profit_usd")
    )

    quote_successful = safe_bool(
        row.get("quote_successful")
    )

    token = normalized_text(
        row.get("token"),
        "UNKNOWN",
    )

    cycle_id = normalized_text(
        row.get("cycle_id"),
        f"VERIFIED-CYCLE-{position:08d}",
    )

    event_time = canonical_timestamp(
        first_present(
            row,
            "scan_time",
            "logged_at",
            default=None,
        )
    )

    canonical = {
        "institutional_event_id": (
            f"VERIFIED_LIVE:{cycle_id}:"
            f"{safe_int(row.get('id'), position):08d}"
        ),
        "institutional_schema_version": SCHEMA_VERSION,
        "source_type": "VERIFIED_LIVE",
        "source_dataset": "verified_live_features",
        "source_schema_version": normalized_text(
            first_present(
                row,
                "live_feature_schema_version",
                "enrichment_schema_version",
                default="UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "source_row_id": first_present(
            row,
            "id",
            default=position,
        ),
        "source_event_id": first_present(
            row,
            "source_event_id",
            "id",
            default=position,
        ),
        "source_cycle_id": cycle_id,
        "source_cycle_number": safe_int(
            row.get("cycle_number")
        ),
        "event_time": event_time,
        "scan_time": event_time,
        "token": token,
        "token_key": normalized_text(
            first_present(
                row,
                "token_key",
                default=token.upper(),
            ),
            token.upper(),
        ),
        "mint": text_or_none(
            row.get("mint")
        ),
        "asset_key": derive_asset_key(row),
        "decision": normalize_decision(
            row.get("decision")
        ),
        "outcome": normalize_outcome(
            quote_successful,
            net,
        ),
        "quote_successful": quote_successful,
        "eligible": safe_bool(
            row.get("eligible")
        ),
        "research_eligible": safe_bool(
            row.get("research_eligible")
        ),
        "validation_status": normalized_text(
            row.get("validation_status"),
            "UNKNOWN",
        ),
        "validation_quality_score": safe_float(
            row.get("validation_quality_score")
        ),
        "validation_errors": safe_int(
            row.get("validation_errors")
        ),
        "validation_warnings": safe_int(
            row.get("validation_warnings")
        ),
        "starting_amount_usd": starting,
        "ending_amount_usd": ending,
        "gross_profit_usd": gross,
        "estimated_cost_usd": cost,
        "net_profit_usd": net,
        "gross_edge_bps": safe_float(
            row.get("gross_edge_bps")
        ),
        "net_edge_bps": safe_float(
            row.get("net_edge_bps")
        ),
        "total_cost_bps": safe_float(
            row.get("total_cost_bps")
        ),
        "slippage_bps": safe_float(
            row.get("slippage_bps")
        ),
        "price_impact_bps": safe_float(
            row.get("price_impact_bps")
        ),
        "network_fee_usd": safe_float(
            row.get("network_fee_usd")
        ),
        "dex_fee_usd": safe_float(
            row.get("dex_fee_usd")
        ),
        "slippage_cost_usd": safe_float(
            row.get("slippage_cost_usd")
        ),
        "liquidity_usd": safe_float(
            row.get("liquidity_usd")
        ),
        "volume_24h_usd": safe_float(
            row.get("volume_24h_usd")
        ),
        "volume_liquidity_ratio": safe_float(
            row.get("volume_liquidity_ratio")
        ),
        "buy_route": normalized_text(
            row.get("buy_route"),
            "UNKNOWN",
        ),
        "sell_route": normalized_text(
            row.get("sell_route"),
            "UNKNOWN",
        ),
        "route_pair": normalized_text(
            row.get("route_pair"),
            "UNKNOWN->UNKNOWN",
        ),
        "route_hops": safe_int(
            row.get("route_hops")
        ),
        "dex_count": safe_int(
            row.get("dex_count")
        ),
        "market_score": safe_float(
            row.get("market_score")
        ),
        "liquidity_score": safe_float(
            row.get("liquidity_score")
        ),
        "volume_score": safe_float(
            row.get("volume_score")
        ),
        "pair_score": safe_float(
            row.get("pair_score")
        ),
        "intelligence_score": safe_float(
            row.get("intelligence_score")
        ),
        "ai_priority": safe_float(
            row.get("ai_priority")
        ),
        "opportunity_probability": safe_float(
            row.get("opportunity_probability")
        ),
        "expected_profit_usd": safe_float(
            row.get("expected_profit_usd")
        ),
        "combined_confidence": safe_float(
            row.get("combined_confidence")
        ),
        "prediction_confidence": safe_float(
            row.get("prediction_confidence")
        ),
        "downside_risk": safe_float(
            row.get("downside_risk")
        ),
        "trend_score": safe_float(
            row.get("trend_score")
        ),
        "stability_score": safe_float(
            row.get("stability_score")
        ),
        "score_mean": safe_float(
            row.get("score_mean")
        ),
        "score_std": safe_float(
            row.get("score_std")
        ),
        "score_min": safe_float(
            row.get("score_min")
        ),
        "score_max": safe_float(
            row.get("score_max")
        ),
        "score_range": safe_float(
            row.get("score_range")
        ),
        "quote_latency_ms": safe_float(
            row.get("quote_latency_ms")
        ),
        "quote_age_ms": safe_float(
            row.get("quote_age_ms")
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
        "enrichment_quality_score": safe_float(
            row.get("enrichment_quality_score")
        ),
        "cycle_position": safe_int(
            row.get("cycle_position")
        ),
        "cycle_size": safe_int(
            row.get("cycle_size")
        ),
        "cycle_elapsed_seconds": safe_float(
            row.get("cycle_elapsed_seconds")
        ),
        "scanner_speed_tokens_per_minute": safe_float(
            row.get("scanner_speed_tokens_per_minute")
        ),
        "raw_source_payload": json.dumps(
            dict(row),
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        ),
    }

    return canonical


class InstitutionalDatasetMerger:
    def __init__(
        self,
        configuration: MergerConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or MergerConfiguration()
        )
        self.configuration.validate()

    def build(
        self,
    ) -> tuple[
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
        DatasetSummary,
        tuple[ValidationCheck, ...],
        dict[str, Any],
    ]:
        historical_source_rows: list[dict[str, Any]] = []
        live_source_rows: list[dict[str, Any]] = []

        if self.configuration.include_historical:
            historical_source_rows = load_jsonl(
                self.configuration.historical_jsonl
            )

        if self.configuration.include_verified_live:
            live_source_rows = load_jsonl(
                self.configuration.verified_live_jsonl
            )

        demo_rows_excluded = 0
        invalid_rows_excluded = 0

        historical_rows = [
            normalize_historical_row(
                row,
                position,
            )
            for position, row in enumerate(
                historical_source_rows,
                start=1,
            )
            if not self._is_demo_row(row)
        ]

        demo_rows_excluded += (
            len(historical_source_rows)
            - len(historical_rows)
        )

        verified_live_rows: list[dict[str, Any]] = []

        for position, row in enumerate(
            live_source_rows,
            start=1,
        ):
            if self._is_demo_row(row):
                demo_rows_excluded += 1
                continue

            if not self._is_valid_verified_live_row(row):
                invalid_rows_excluded += 1
                continue

            verified_live_rows.append(
                normalize_verified_live_row(
                    row,
                    position,
                )
            )

        merged_rows = (
            historical_rows
            + verified_live_rows
        )

        (
            deduplicated_rows,
            duplicate_rows_removed,
        ) = self._deduplicate(
            merged_rows
        )

        deduplicated_rows.sort(
            key=self._chronology_sort_key
        )

        cycle_rows = self._build_cycle_rows(
            deduplicated_rows
        )

        field_catalog = self._build_field_catalog(
            historical_source_rows,
            live_source_rows,
        )

        summary = self._build_summary(
            deduplicated_rows,
            cycle_rows,
            duplicate_rows_removed=(
                duplicate_rows_removed
            ),
            demo_rows_excluded=(
                demo_rows_excluded
            ),
            invalid_rows_excluded=(
                invalid_rows_excluded
            ),
        )

        checks = self._validate_dataset(
            deduplicated_rows,
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
            tuple(deduplicated_rows),
            tuple(cycle_rows),
            summary,
            tuple(checks),
            field_catalog,
        )

    @staticmethod
    def _is_demo_row(
        row: Mapping[str, Any],
    ) -> bool:
        token = normalized_text(
            first_present(
                row,
                "token",
                "symbol",
                default="",
            )
        ).upper()

        decision = normalized_text(
            row.get("decision")
        ).upper()

        source_type = normalized_text(
            row.get("source_type")
        ).upper()

        return (
            token.startswith("DEMO")
            or decision == "DEMO_EXECUTE"
            or source_type == "DEMO"
            or safe_bool(
                row.get("is_demo")
            )
        )

    @staticmethod
    def _is_valid_verified_live_row(
        row: Mapping[str, Any],
    ) -> bool:
        status = normalized_text(
            row.get("validation_status")
        ).upper()

        return (
            safe_bool(
                row.get("research_eligible")
            )
            and status
            in {
                "VERIFIED",
                "VERIFIED_WITH_WARNING",
            }
        )

    @staticmethod
    def _deduplicate(
        rows: Sequence[dict[str, Any]],
    ) -> tuple[
        list[dict[str, Any]],
        int,
    ]:
        selected: dict[
            tuple[Any, ...],
            dict[str, Any],
        ] = {}

        duplicate_count = 0

        for row in rows:
            key = (
                row["source_type"],
                row["source_cycle_id"],
                row["source_event_id"],
                row["token"],
                row["scan_time"],
            )

            if key in selected:
                duplicate_count += 1
                continue

            selected[key] = row

        return (
            list(selected.values()),
            duplicate_count,
        )

    @staticmethod
    def _chronology_sort_key(
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

    @staticmethod
    def _build_cycle_rows(
        rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped: dict[
            tuple[str, str, int],
            list[Mapping[str, Any]],
        ] = defaultdict(list)

        for row in rows:
            key = (
                normalized_text(
                    row.get("source_type")
                ),
                normalized_text(
                    row.get("source_cycle_id")
                ),
                safe_int(
                    row.get("source_cycle_number")
                ),
            )

            grouped[key].append(row)

        cycle_rows: list[dict[str, Any]] = []

        for (
            source_type,
            cycle_id,
            cycle_number,
        ), cycle_events in grouped.items():
            successful = [
                row
                for row in cycle_events
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

            statuses = Counter(
                normalized_text(
                    row.get(
                        "validation_status"
                    ),
                    "UNKNOWN",
                )
                for row in cycle_events
            )

            event_times = [
                normalized_text(
                    row.get("event_time")
                )
                for row in cycle_events
                if normalized_text(
                    row.get("event_time")
                )
            ]

            cycle_rows.append(
                {
                    "institutional_schema_version": (
                        SCHEMA_VERSION
                    ),
                    "source_type": source_type,
                    "source_cycle_id": cycle_id,
                    "source_cycle_number": (
                        cycle_number
                    ),
                    "rows": len(cycle_events),
                    "successful_quotes": len(
                        successful
                    ),
                    "quote_errors": (
                        len(cycle_events)
                        - len(successful)
                    ),
                    "eligible_observations": sum(
                        safe_bool(
                            row.get("eligible")
                        )
                        for row in cycle_events
                    ),
                    "profitable_observations": sum(
                        safe_float(
                            row.get(
                                "net_profit_usd"
                            )
                        ) > 0
                        and safe_bool(
                            row.get(
                                "quote_successful"
                            )
                        )
                        for row in cycle_events
                    ),
                    "average_net_profit_usd": (
                        sum(profits)
                        / len(profits)
                        if profits
                        else 0.0
                    ),
                    "best_net_profit_usd": (
                        max(profits)
                        if profits
                        else 0.0
                    ),
                    "worst_net_profit_usd": (
                        min(profits)
                        if profits
                        else 0.0
                    ),
                    "average_total_cost_bps": (
                        sum(
                            safe_float(
                                row.get(
                                    "total_cost_bps"
                                )
                            )
                            for row in successful
                        )
                        / len(successful)
                        if successful
                        else 0.0
                    ),
                    "average_quote_latency_ms": (
                        sum(
                            safe_float(
                                row.get(
                                    "quote_latency_ms"
                                )
                            )
                            for row in successful
                        )
                        / len(successful)
                        if successful
                        else 0.0
                    ),
                    "validation_status_counts": (
                        json.dumps(
                            dict(statuses),
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                    ),
                    "first_event_time": (
                        min(event_times)
                        if event_times
                        else None
                    ),
                    "last_event_time": (
                        max(event_times)
                        if event_times
                        else None
                    ),
                }
            )

        cycle_rows.sort(
            key=lambda row: (
                parse_timestamp(
                    row.get("first_event_time")
                )
                or datetime.max.replace(
                    tzinfo=timezone.utc
                ),
                row["source_type"],
                row["source_cycle_id"],
            )
        )

        return cycle_rows

    @staticmethod
    def _build_field_catalog(
        historical_rows: Sequence[Mapping[str, Any]],
        live_rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        historical_fields = sorted(
            {
                key
                for row in historical_rows
                for key in row.keys()
            }
        )

        live_fields = sorted(
            {
                key
                for row in live_rows
                for key in row.keys()
            }
        )

        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": utc_now_text(),
            "canonical_fields": list(
                CANONICAL_FIELD_ORDER
            ),
            "historical_source_fields": (
                historical_fields
            ),
            "verified_live_source_fields": (
                live_fields
            ),
            "shared_source_fields": sorted(
                set(historical_fields)
                & set(live_fields)
            ),
            "historical_only_fields": sorted(
                set(historical_fields)
                - set(live_fields)
            ),
            "verified_live_only_fields": sorted(
                set(live_fields)
                - set(historical_fields)
            ),
        }

    @staticmethod
    def _build_summary(
        rows: Sequence[Mapping[str, Any]],
        cycle_rows: Sequence[Mapping[str, Any]],
        *,
        duplicate_rows_removed: int,
        demo_rows_excluded: int,
        invalid_rows_excluded: int,
    ) -> DatasetSummary:
        historical = [
            row
            for row in rows
            if row["source_type"]
            == "HISTORICAL"
        ]

        verified_live = [
            row
            for row in rows
            if row["source_type"]
            == "VERIFIED_LIVE"
        ]

        successful = [
            row
            for row in rows
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

        event_times = [
            normalized_text(
                row.get("event_time")
            )
            for row in rows
            if normalized_text(
                row.get("event_time")
            )
        ]

        return DatasetSummary(
            generated_at=utc_now_text(),
            schema_version=SCHEMA_VERSION,
            total_rows=len(rows),
            total_cycles=len(cycle_rows),
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
            historical_rows=len(
                historical
            ),
            historical_cycles=len(
                {
                    (
                        row["source_cycle_id"],
                        row["source_cycle_number"],
                    )
                    for row in historical
                }
            ),
            verified_live_rows=len(
                verified_live
            ),
            verified_live_cycles=len(
                {
                    (
                        row["source_cycle_id"],
                        row["source_cycle_number"],
                    )
                    for row in verified_live
                }
            ),
            verified_rows=sum(
                row["validation_status"]
                == "VERIFIED"
                for row in rows
            ),
            verified_with_warning_rows=sum(
                row["validation_status"]
                == "VERIFIED_WITH_WARNING"
                for row in rows
            ),
            unvalidated_historical_rows=sum(
                row["validation_status"]
                == "HISTORICAL_UNVALIDATED"
                for row in rows
            ),
            successful_quotes=len(
                successful
            ),
            quote_errors=(
                len(rows) - len(successful)
            ),
            eligible_observations=sum(
                safe_bool(
                    row.get("eligible")
                )
                for row in rows
            ),
            profitable_observations=sum(
                safe_float(
                    row.get("net_profit_usd")
                ) > 0
                and safe_bool(
                    row.get("quote_successful")
                )
                for row in rows
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
                sum(
                    safe_float(
                        row.get(
                            "total_cost_bps"
                        )
                    )
                    for row in successful
                )
                / len(successful)
                if successful
                else 0.0
            ),
            average_quote_latency_ms=(
                sum(
                    safe_float(
                        row.get(
                            "quote_latency_ms"
                        )
                    )
                    for row in successful
                )
                / len(successful)
                if successful
                else 0.0
            ),
            duplicate_rows_removed=(
                duplicate_rows_removed
            ),
            demo_rows_excluded=(
                demo_rows_excluded
            ),
            invalid_rows_excluded=(
                invalid_rows_excluded
            ),
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

    def _validate_dataset(
        self,
        rows: Sequence[Mapping[str, Any]],
        cycle_rows: Sequence[Mapping[str, Any]],
        summary: DatasetSummary,
    ) -> list[ValidationCheck]:
        checks: list[
            ValidationCheck
        ] = []

        checks.append(
            ValidationCheck(
                name="minimum_total_rows",
                passed=(
                    len(rows)
                    >= self.configuration.minimum_total_rows
                ),
                observed=len(rows),
                expected=(
                    f">= {self.configuration.minimum_total_rows}"
                ),
                details="Canonical row minimum.",
            )
        )

        checks.append(
            ValidationCheck(
                name="minimum_total_cycles",
                passed=(
                    len(cycle_rows)
                    >= self.configuration.minimum_total_cycles
                ),
                observed=len(cycle_rows),
                expected=(
                    f">= {self.configuration.minimum_total_cycles}"
                ),
                details="Canonical cycle minimum.",
            )
        )

        checks.append(
            ValidationCheck(
                name="minimum_verified_live_rows",
                passed=(
                    summary.verified_live_rows
                    >= self.configuration.minimum_verified_live_rows
                ),
                observed=(
                    summary.verified_live_rows
                ),
                expected=(
                    f">= {self.configuration.minimum_verified_live_rows}"
                ),
                details="Verified-live row minimum.",
            )
        )

        checks.append(
            ValidationCheck(
                name="minimum_verified_live_cycles",
                passed=(
                    summary.verified_live_cycles
                    >= self.configuration.minimum_verified_live_cycles
                ),
                observed=(
                    summary.verified_live_cycles
                ),
                expected=(
                    f">= {self.configuration.minimum_verified_live_cycles}"
                ),
                details="Verified-live cycle minimum.",
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
                name="unique_institutional_event_ids",
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
                    "Every canonical row must have asset_key."
                ),
            )
        )

        invalid_live_rows = sum(
            row["source_type"]
            == "VERIFIED_LIVE"
            and (
                not safe_bool(
                    row.get(
                        "research_eligible"
                    )
                )
                or row[
                    "validation_status"
                ]
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
                    "Verified-live rows must pass "
                    "research eligibility and status gates."
                ),
            )
        )

        demo_rows = sum(
            self._is_demo_row(row)
            for row in rows
        )

        checks.append(
            ValidationCheck(
                name="demo_trade_exclusion",
                passed=demo_rows == 0,
                observed=demo_rows,
                expected=0,
                details=(
                    "Demo/synthetic rows are prohibited."
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
                name="chronological_order",
                passed=(
                    chronology_errors == 0
                ),
                observed=chronology_errors,
                expected=0,
                details=(
                    "Canonical rows must be chronological."
                ),
            )
        )

        arithmetic_errors = 0
        bps_errors = 0

        for row in rows:
            if not safe_bool(
                row.get("quote_successful")
            ):
                continue

            starting = safe_float(
                row.get("starting_amount_usd")
            )
            ending = safe_float(
                row.get("ending_amount_usd")
            )
            gross = safe_float(
                row.get("gross_profit_usd")
            )
            cost = safe_float(
                row.get("estimated_cost_usd")
            )
            net = safe_float(
                row.get("net_profit_usd")
            )

            if abs(
                gross - (ending - starting)
            ) > self.configuration.arithmetic_tolerance_usd:
                arithmetic_errors += 1

            if abs(
                net - (gross - cost)
            ) > self.configuration.arithmetic_tolerance_usd:
                arithmetic_errors += 1

            if starting > 0:
                expected_net_bps = (
                    net
                    / starting
                    * 10_000.0
                )

                if abs(
                    safe_float(
                        row.get("net_edge_bps")
                    )
                    - expected_net_bps
                ) > self.configuration.bps_tolerance:
                    bps_errors += 1

        checks.append(
            ValidationCheck(
                name="profit_arithmetic",
                passed=(
                    arithmetic_errors == 0
                ),
                observed=arithmetic_errors,
                expected=0,
                details=(
                    "Gross and net profit must reconcile."
                ),
            )
        )

        checks.append(
            ValidationCheck(
                name="net_bps_arithmetic",
                passed=bps_errors == 0,
                observed=bps_errors,
                expected=0,
                details=(
                    "net_edge_bps must reconcile."
                ),
            )
        )

        cycle_identity_from_events = {
            (
                row["source_type"],
                row["source_cycle_id"],
                row["source_cycle_number"],
            )
            for row in rows
        }

        cycle_identity_from_cycles = {
            (
                row["source_type"],
                row["source_cycle_id"],
                row["source_cycle_number"],
            )
            for row in cycle_rows
        }

        checks.append(
            ValidationCheck(
                name="cycle_reconciliation",
                passed=(
                    cycle_identity_from_events
                    == cycle_identity_from_cycles
                ),
                observed=len(
                    cycle_identity_from_events
                ),
                expected=len(
                    cycle_identity_from_cycles
                ),
                details=(
                    "Event and cycle identities must match."
                ),
            )
        )

        return checks


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    preferred_order: Sequence[str] | None = None,
) -> None:
    if not rows:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    fields: list[str] = []
    seen: set[str] = set()

    if preferred_order:
        for field in preferred_order:
            if field not in seen:
                seen.add(field)
                fields.append(field)

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


def export_institutional_dataset(
    *,
    rows: Sequence[Mapping[str, Any]],
    cycle_rows: Sequence[Mapping[str, Any]],
    summary: DatasetSummary,
    checks: Sequence[ValidationCheck],
    field_catalog: Mapping[str, Any],
    configuration: MergerConfiguration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    events_csv = output / EVENTS_CSV
    events_jsonl = output / EVENTS_JSONL
    cycles_csv = output / CYCLES_CSV
    manifest_json = output / MANIFEST_JSON
    validation_json = output / VALIDATION_JSON
    field_catalog_json = (
        output
        / FIELD_CATALOG_JSON
    )

    destinations = (
        events_csv,
        events_jsonl,
        cycles_csv,
        manifest_json,
        validation_json,
        field_catalog_json,
    )

    if not configuration.overwrite:
        existing = [
            path
            for path in destinations
            if path.exists()
        ]

        if existing:
            raise InstitutionalDatasetError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    write_csv(
        events_csv,
        rows,
        preferred_order=(
            CANONICAL_FIELD_ORDER
        ),
    )

    write_jsonl(
        events_jsonl,
        rows,
    )

    write_csv(
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

    field_catalog_json.write_text(
        json.dumps(
            dict(field_catalog),
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    files = {}

    for path, row_count in (
        (events_csv, len(rows)),
        (events_jsonl, len(rows)),
        (cycles_csv, len(cycle_rows)),
        (validation_json, len(checks)),
        (field_catalog_json, None),
    ):
        files[path.name] = {
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
                "sources": {
                    "historical": {
                        "enabled": (
                            configuration.include_historical
                        ),
                        "path": str(
                            configuration.historical_jsonl
                        ),
                    },
                    "verified_live": {
                        "enabled": (
                            configuration.include_verified_live
                        ),
                        "path": str(
                            configuration.verified_live_jsonl
                        ),
                        "required_statuses": [
                            "VERIFIED",
                            "VERIFIED_WITH_WARNING",
                        ],
                        "required_research_eligible": True,
                    },
                },
                "governance": {
                    "demo_trades_included": False,
                    "invalid_live_rows_included": False,
                    "legacy_live_rows_included": False,
                    "historical_rows_preserved_as": (
                        "HISTORICAL_UNVALIDATED"
                    ),
                    "source_payload_preserved": True,
                    "canonical_rows_sorted_chronologically": True,
                },
                "files": files,
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
            "Merge historical and verified-live data "
            "into the institutional research dataset."
        )
    )

    parser.add_argument(
        "--historical-jsonl",
        default=str(
            DEFAULT_HISTORICAL_JSONL
        ),
    )

    parser.add_argument(
        "--verified-live-jsonl",
        default=str(
            DEFAULT_VERIFIED_LIVE_JSONL
        ),
    )

    parser.add_argument(
        "--output-directory",
        default=str(
            DEFAULT_OUTPUT_DIRECTORY
        ),
    )

    parser.add_argument(
        "--exclude-historical",
        action="store_true",
    )

    parser.add_argument(
        "--exclude-verified-live",
        action="store_true",
    )

    parser.add_argument(
        "--minimum-total-rows",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--minimum-total-cycles",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--minimum-verified-live-rows",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--minimum-verified-live-cycles",
        type=int,
        default=0,
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

    configuration = MergerConfiguration(
        historical_jsonl=Path(
            args.historical_jsonl
        ),
        verified_live_jsonl=Path(
            args.verified_live_jsonl
        ),
        output_directory=Path(
            args.output_directory
        ),
        overwrite=(
            not args.no_overwrite
        ),
        include_historical=(
            not args.exclude_historical
        ),
        include_verified_live=(
            not args.exclude_verified_live
        ),
        minimum_total_rows=(
            args.minimum_total_rows
        ),
        minimum_total_cycles=(
            args.minimum_total_cycles
        ),
        minimum_verified_live_rows=(
            args.minimum_verified_live_rows
        ),
        minimum_verified_live_cycles=(
            args.minimum_verified_live_cycles
        ),
    )

    try:
        (
            rows,
            cycle_rows,
            summary,
            checks,
            field_catalog,
        ) = InstitutionalDatasetMerger(
            configuration
        ).build()

        output_paths = (
            export_institutional_dataset(
                rows=rows,
                cycle_rows=cycle_rows,
                summary=summary,
                checks=checks,
                field_catalog=field_catalog,
                configuration=configuration,
            )
        )

    except (
        InstitutionalDatasetError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 13B — "
        "Institutional Dataset Merger"
    )
    print("=" * 80)

    print("Dataset")
    print("-" * 80)
    print(
        f"Total rows: {summary.total_rows}"
    )
    print(
        f"Total cycles: {summary.total_cycles}"
    )
    print(
        f"Unique assets: {summary.unique_assets}"
    )
    print(
        f"Unique tokens: {summary.unique_tokens}"
    )
    print()

    print("Source Composition")
    print("-" * 80)
    print(
        "Historical rows / cycles: "
        f"{summary.historical_rows} / "
        f"{summary.historical_cycles}"
    )
    print(
        "Verified-live rows / cycles: "
        f"{summary.verified_live_rows} / "
        f"{summary.verified_live_cycles}"
    )
    print(
        "VERIFIED / VERIFIED_WITH_WARNING rows: "
        f"{summary.verified_rows} / "
        f"{summary.verified_with_warning_rows}"
    )
    print(
        "Historical unvalidated rows: "
        f"{summary.unvalidated_historical_rows}"
    )
    print()

    print("Performance Labels")
    print("-" * 80)
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
        "Best / worst net profit: "
        f"${summary.best_net_profit_usd:.6f} / "
        f"${summary.worst_net_profit_usd:.6f}"
    )
    print(
        "Average total cost: "
        f"{summary.average_total_cost_bps:.4f} bps"
    )
    print(
        "Average quote latency: "
        f"{summary.average_quote_latency_ms:.2f} ms"
    )
    print()

    print("Exclusions")
    print("-" * 80)
    print(
        "Duplicates removed: "
        f"{summary.duplicate_rows_removed}"
    )
    print(
        "Demo rows excluded: "
        f"{summary.demo_rows_excluded}"
    )
    print(
        "Invalid verified-live rows excluded: "
        f"{summary.invalid_rows_excluded}"
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

    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(path)

    return 0 if summary.valid else 2


if __name__ == "__main__":
    raise SystemExit(main())