"""
Phase 12A — Scanner Data Enrichment

Adds richer, strictly pre-decision research fields to scanner observations.

Design goals
------------
- Never change scanner trade decisions.
- Never execute trades.
- Never require a wallet.
- Preserve original scanner results.
- Add only information available at or before quote time.
- Save enriched observations in a separate SQLite table.
- Fail safely: enrichment errors never crash the scanner.

Typical integration
-------------------
    from execution.scanner_data_enrichment import enrich_scan_result

    enriched = enrich_scan_result(
        result,
        cycle_context={
            "cycle_id": cycle_id,
            "cycle_number": cycle_number,
            "cycle_started_at": cycle_started_at,
            "scanner_speed": scanner_speed,
        },
    )

The returned dictionary contains the original fields plus enrichment fields.

Optional persistence:
    from execution.scanner_data_enrichment import save_enriched_result

    save_enriched_result(enriched)
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence


LOGGER = logging.getLogger(__name__)

DATABASE = Path(__file__).resolve().parent.parent / "database" / "trades.db"
ENRICHMENT_SCHEMA_VERSION = "12A.1.0"


class ScannerEnrichmentError(RuntimeError):
    """Base exception for scanner-enrichment failures."""


@dataclass(frozen=True, slots=True)
class EnrichmentConfiguration:
    database_path: Path = DATABASE
    quote_stale_after_ms: float = 2_000.0
    high_latency_after_ms: float = 1_000.0
    high_price_impact_bps: float = 100.0
    high_cost_bps: float = 50.0
    low_liquidity_usd: float = 25_000.0
    minimum_volume_liquidity_ratio: float = 0.05

    def validate(self) -> None:
        numeric_fields = (
            "quote_stale_after_ms",
            "high_latency_after_ms",
            "high_price_impact_bps",
            "high_cost_bps",
            "low_liquidity_usd",
            "minimum_volume_liquidity_ratio",
        )

        for name in numeric_fields:
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ScannerEnrichmentError(
                    f"{name} must be finite and non-negative."
                )


@dataclass(frozen=True, slots=True)
class EnrichmentSummary:
    total_rows: int
    quote_errors: int
    stale_quotes: int
    high_latency_quotes: int
    low_liquidity_quotes: int
    high_price_impact_quotes: int
    average_quote_latency_ms: float
    average_quote_age_ms: float
    average_price_impact_bps: float
    average_total_cost_bps: float
    updated_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_text() -> str:
    return utc_now().isoformat(timespec="milliseconds")


def safe_float(
    value: Any,
    default: float = 0.0,
) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    if not math.isfinite(numeric):
        return default

    return numeric


def safe_int(
    value: Any,
    default: int = 0,
) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_bool(
    value: Any,
) -> bool:
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


def parse_timestamp(
    value: Any,
) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(
                float(value),
                tz=timezone.utc,
            )
        except (OSError, OverflowError, ValueError):
            return None
    elif isinstance(value, str):
        text = value.strip()

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
                    parsed = datetime.strptime(text, pattern)
                    break
                except ValueError:
                    continue
            else:
                return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def elapsed_ms(
    started: datetime | None,
    ended: datetime | None,
) -> float:
    if started is None or ended is None:
        return 0.0

    return max(
        0.0,
        (ended - started).total_seconds() * 1_000.0,
    )


def calculate_bps(
    numerator: float,
    denominator: float,
) -> float:
    if denominator <= 0:
        return 0.0

    return numerator / denominator * 10_000.0


def calculate_percentage(
    numerator: float,
    denominator: float,
) -> float:
    if denominator <= 0:
        return 0.0

    return numerator / denominator * 100.0


def normalize_route_name(
    value: Any,
) -> str:
    if value is None:
        return "UNKNOWN"

    text = str(value).strip()

    return text if text else "UNKNOWN"


def normalize_symbol(
    result: Mapping[str, Any],
) -> str:
    value = first_present(
        result,
        "token",
        "symbol",
        "token_symbol",
        "name",
        default="UNKNOWN",
    )

    text = str(value).strip()

    return text if text else "UNKNOWN"


def quote_successful(
    result: Mapping[str, Any],
) -> bool:
    explicit = first_present(
        result,
        "quote_successful",
        "quote_success",
        default=None,
    )

    if explicit is not None:
        return safe_bool(explicit)

    decision = str(
        first_present(
            result,
            "decision",
            default="",
        )
    ).upper()

    error = first_present(
        result,
        "error",
        "quote_error",
        "error_message",
        default=None,
    )

    return (
        "QUOTE ERROR" not in decision
        and not error
    )


def enrich_scan_result(
    result: Mapping[str, Any],
    *,
    cycle_context: Mapping[str, Any] | None = None,
    configuration: EnrichmentConfiguration | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """
    Return a copy of `result` with pre-decision enrichment fields.

    This function is deliberately defensive. If a source field is absent, a
    neutral default is used and `enrichment_missing_fields` records the gap.
    """

    config = configuration or EnrichmentConfiguration()
    config.validate()

    cycle = cycle_context or {}
    now = observed_at or utc_now()

    enriched: dict[str, Any] = dict(result)
    missing: list[str] = []

    symbol = normalize_symbol(result)

    mint = first_present(
        result,
        "mint",
        "token_mint",
        "mint_address",
        "address",
        default=None,
    )

    token_key = str(
        first_present(
            result,
            "token_key",
            default=symbol,
        )
    ).strip().upper()

    source_asset_key = first_present(
        result,
        "asset_key",
        default=None,
    )

    if source_asset_key is not None and str(source_asset_key).strip():
        asset_key = str(source_asset_key).strip()
    elif mint is not None and str(mint).strip():
        asset_key = f"mint:{str(mint).strip()}"
    else:
        asset_key = f"symbol:{token_key}"

    quote_started_at = parse_timestamp(
        first_present(
            result,
            "quote_started_at",
            "request_started_at",
            "quote_request_time",
        )
    )
    quote_received_at = parse_timestamp(
        first_present(
            result,
            "quote_received_at",
            "quote_completed_at",
            "quote_time",
            "scan_time",
        )
    )

    if quote_received_at is None:
        quote_received_at = now
        missing.append("quote_received_at")

    quote_source_time = parse_timestamp(
        first_present(
            result,
            "quote_source_time",
            "provider_timestamp",
            "route_timestamp",
        )
    )

    quote_latency_ms = safe_float(
        first_present(
            result,
            "quote_latency_ms",
            "latency_ms",
            default=None,
        ),
        default=elapsed_ms(
            quote_started_at,
            quote_received_at,
        ),
    )

    quote_age_ms = (
        elapsed_ms(
            quote_source_time,
            quote_received_at,
        )
        if quote_source_time is not None
        else 0.0
    )

    starting_amount_usd = safe_float(
        first_present(
            result,
            "starting_amount_usd",
            "starting_amount",
            "trade_amount_usd",
            "trade_amount",
            "amount_usd",
            "input_amount_usd",
            "input_amount",
        )
    )

    ending_amount_usd = safe_float(
        first_present(
            result,
            "ending_amount_usd",
            "ending_amount",
            "output_amount_usd",
            "output_amount",
            "final_amount_usd",
            "final_amount",
        )
    )

    gross_profit_usd = safe_float(
        first_present(
            result,
            "gross_profit",
            "gross_profit_usd",
            "quoted_profit",
            "quoted_profit_usd",
            default=(
                ending_amount_usd - starting_amount_usd
                if ending_amount_usd > 0
                else 0.0
            ),
        )
    )

    network_fee_usd = safe_float(
        first_present(
            result,
            "network_fee_usd",
            "gas_fee_usd",
            "transaction_fee_usd",
            "priority_fee_usd",
        )
    )

    dex_fee_usd = safe_float(
        first_present(
            result,
            "dex_fee_usd",
            "swap_fee_usd",
            "route_fee_usd",
        )
    )

    slippage_cost_usd = safe_float(
        first_present(
            result,
            "slippage_cost_usd",
            "estimated_slippage_usd",
        )
    )

    explicit_cost_value = first_present(
        result,
        "estimated_cost_usd",
        "estimated_cost",
        "all_in_cost_usd",
        "all_in_cost",
        "corrected_all_in_cost_usd",
        "corrected_all_in_cost",
        "total_cost_usd",
        "total_cost",
        "total_fees_usd",
        "total_fees",
        "estimated_fees_usd",
        "estimated_fees",
        default=None,
    )

    net_profit_usd = safe_float(
        first_present(
            result,
            "net_profit",
            "net_profit_usd",
            "conservative_net_profit",
            "conservative_net_profit_usd",
            default=0.0,
        )
    )

    component_cost_usd = (
        network_fee_usd
        + dex_fee_usd
        + slippage_cost_usd
    )

    if explicit_cost_value is not None:
        estimated_cost_usd = safe_float(
            explicit_cost_value
        )
    elif component_cost_usd > 0:
        estimated_cost_usd = component_cost_usd
    elif (
        gross_profit_usd != 0.0
        or net_profit_usd != 0.0
    ):
        # The scanner commonly exposes starting_amount, ending_amount and
        # net_profit but no explicit all-in cost. In that schema the
        # fee-adjusted cost reconciles as gross profit minus net profit.
        estimated_cost_usd = max(
            0.0,
            gross_profit_usd - net_profit_usd,
        )
    else:
        estimated_cost_usd = 0.0

    if not any(
        key in result
        for key in (
            "net_profit",
            "net_profit_usd",
            "conservative_net_profit",
            "conservative_net_profit_usd",
        )
    ):
        net_profit_usd = (
            gross_profit_usd
            - estimated_cost_usd
        )

    price_impact_pct = safe_float(
        first_present(
            result,
            "price_impact_pct",
            "price_impact_percent",
        )
    )

    price_impact_bps = safe_float(
        first_present(
            result,
            "price_impact_bps",
            default=price_impact_pct * 100.0,
        )
    )

    slippage_bps = safe_float(
        first_present(
            result,
            "slippage_bps",
            default=calculate_bps(
                slippage_cost_usd,
                starting_amount_usd,
            ),
        )
    )

    total_cost_bps = calculate_bps(
        estimated_cost_usd,
        starting_amount_usd,
    )

    gross_edge_bps = calculate_bps(
        gross_profit_usd,
        starting_amount_usd,
    )

    net_edge_bps = calculate_bps(
        net_profit_usd,
        starting_amount_usd,
    )

    liquidity_usd = safe_float(
        first_present(
            result,
            "liquidity_usd",
            "liquidity",
            "pool_liquidity_usd",
            "largest_pool_liquidity_usd",
        )
    )

    volume_24h_usd = safe_float(
        first_present(
            result,
            "volume_24h_usd",
            "volume_usd",
            "daily_volume_usd",
            "volume_24h",
        )
    )

    volume_liquidity_ratio = (
        volume_24h_usd / liquidity_usd
        if liquidity_usd > 0
        else 0.0
    )

    buy_route = normalize_route_name(
        first_present(
            result,
            "buy_route",
            "buy_dex",
            "source_dex",
        )
    )

    sell_route = normalize_route_name(
        first_present(
            result,
            "sell_route",
            "sell_dex",
            "destination_dex",
        )
    )

    route_pair = f"{buy_route}->{sell_route}"

    route_hops = safe_int(
        first_present(
            result,
            "route_hops",
            "hop_count",
            "route_length",
        )
    )

    dex_count = safe_int(
        first_present(
            result,
            "dex_count",
            "route_dex_count",
        ),
        default=len(
            {
                route
                for route in (buy_route, sell_route)
                if route != "UNKNOWN"
            }
        ),
    )

    market_score = safe_float(
        first_present(
            result,
            "market_score",
        )
    )

    liquidity_score = safe_float(
        first_present(
            result,
            "liquidity_score",
        )
    )

    volume_score = safe_float(
        first_present(
            result,
            "volume_score",
        )
    )

    pair_score = safe_float(
        first_present(
            result,
            "pair_score",
        )
    )

    intelligence_score = safe_float(
        first_present(
            result,
            "intelligence_score",
            "ai_opportunity_score",
            "ai_score",
        )
    )

    score_values = [
        market_score,
        liquidity_score,
        volume_score,
        pair_score,
        intelligence_score,
    ]

    nonzero_scores = [
        value
        for value in score_values
        if value != 0.0
    ]

    score_mean = (
        statistics.fmean(nonzero_scores)
        if nonzero_scores
        else 0.0
    )

    score_std = (
        statistics.pstdev(nonzero_scores)
        if len(nonzero_scores) > 1
        else 0.0
    )

    score_min = (
        min(nonzero_scores)
        if nonzero_scores
        else 0.0
    )

    score_max = (
        max(nonzero_scores)
        if nonzero_scores
        else 0.0
    )

    quote_ok = quote_successful(result)

    is_stale_quote = (
        quote_age_ms
        >= config.quote_stale_after_ms
    )

    is_high_latency = (
        quote_latency_ms
        >= config.high_latency_after_ms
    )

    is_high_price_impact = (
        price_impact_bps
        >= config.high_price_impact_bps
    )

    is_high_cost = (
        total_cost_bps
        >= config.high_cost_bps
    )

    is_low_liquidity = (
        liquidity_usd > 0
        and liquidity_usd
        <= config.low_liquidity_usd
    )

    is_low_activity = (
        liquidity_usd > 0
        and volume_liquidity_ratio
        < config.minimum_volume_liquidity_ratio
    )

    enrichment_quality_score = 100.0

    enrichment_quality_score -= min(
        30.0,
        len(missing) * 5.0,
    )
    enrichment_quality_score -= (
        20.0 if not quote_ok else 0.0
    )
    enrichment_quality_score -= (
        10.0 if is_stale_quote else 0.0
    )
    enrichment_quality_score -= (
        10.0 if is_high_latency else 0.0
    )

    enrichment_quality_score = max(
        0.0,
        min(100.0, enrichment_quality_score),
    )

    enriched.update(
        {
            "enrichment_schema_version": ENRICHMENT_SCHEMA_VERSION,
            "enriched_at": now.isoformat(timespec="milliseconds"),
            "symbol_normalized": symbol,
            "token_key": token_key,
            "mint": mint,
            "asset_key": asset_key,
            "cycle_id": first_present(
                cycle,
                "cycle_id",
                default=first_present(
                    result,
                    "cycle_id",
                    default=None,
                ),
            ),
            "cycle_number": safe_int(
                first_present(
                    cycle,
                    "cycle_number",
                    default=first_present(
                        result,
                        "cycle_number",
                        default=0,
                    ),
                )
            ),
            "cycle_started_at": first_present(
                cycle,
                "cycle_started_at",
                default=None,
            ),
            "scanner_speed_tokens_per_minute": safe_float(
                first_present(
                    cycle,
                    "scanner_speed",
                    "scanner_speed_tokens_per_minute",
                    default=0.0,
                )
            ),
            "quote_started_at": (
                quote_started_at.isoformat(timespec="milliseconds")
                if quote_started_at
                else None
            ),
            "quote_received_at": (
                quote_received_at.isoformat(timespec="milliseconds")
                if quote_received_at
                else None
            ),
            "quote_source_time": (
                quote_source_time.isoformat(timespec="milliseconds")
                if quote_source_time
                else None
            ),
            "quote_latency_ms": quote_latency_ms,
            "quote_age_ms": quote_age_ms,
            "quote_successful": quote_ok,
            "is_stale_quote": is_stale_quote,
            "is_high_latency": is_high_latency,
            "starting_amount_usd": starting_amount_usd,
            "ending_amount_usd": ending_amount_usd,
            "gross_profit_usd": gross_profit_usd,
            "estimated_cost_usd": estimated_cost_usd,
            "network_fee_usd": network_fee_usd,
            "dex_fee_usd": dex_fee_usd,
            "slippage_cost_usd": slippage_cost_usd,
            "net_profit_usd": net_profit_usd,
            "gross_edge_bps": gross_edge_bps,
            "net_edge_bps": net_edge_bps,
            "total_cost_bps": total_cost_bps,
            "slippage_bps": slippage_bps,
            "price_impact_bps": price_impact_bps,
            "is_high_price_impact": is_high_price_impact,
            "is_high_cost": is_high_cost,
            "liquidity_usd_enriched": liquidity_usd,
            "volume_24h_usd_enriched": volume_24h_usd,
            "volume_liquidity_ratio": volume_liquidity_ratio,
            "is_low_liquidity": is_low_liquidity,
            "is_low_activity": is_low_activity,
            "buy_route_normalized": buy_route,
            "sell_route_normalized": sell_route,
            "route_pair_enriched": route_pair,
            "route_hops": route_hops,
            "dex_count": dex_count,
            "score_mean": score_mean,
            "score_std": score_std,
            "score_min": score_min,
            "score_max": score_max,
            "score_range": score_max - score_min,
            "enrichment_quality_score": enrichment_quality_score,
            "enrichment_missing_fields": missing,
        }
    )

    return enriched


def initialize_enrichment_table(
    database_path: str | Path = DATABASE,
) -> None:
    path = Path(database_path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = sqlite3.connect(path)

    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scanner_enrichment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_event_id INTEGER,
                cycle_id TEXT,
                cycle_number INTEGER,
                scan_time TEXT,
                token TEXT,
                mint TEXT,
                asset_key TEXT,
                quote_successful INTEGER,
                decision TEXT,
                eligible INTEGER,
                quote_latency_ms REAL,
                quote_age_ms REAL,
                is_stale_quote INTEGER,
                is_high_latency INTEGER,
                starting_amount_usd REAL,
                ending_amount_usd REAL,
                gross_profit_usd REAL,
                estimated_cost_usd REAL,
                net_profit_usd REAL,
                gross_edge_bps REAL,
                net_edge_bps REAL,
                total_cost_bps REAL,
                slippage_bps REAL,
                price_impact_bps REAL,
                liquidity_usd REAL,
                volume_24h_usd REAL,
                volume_liquidity_ratio REAL,
                buy_route TEXT,
                sell_route TEXT,
                route_pair TEXT,
                route_hops INTEGER,
                dex_count INTEGER,
                market_score REAL,
                liquidity_score REAL,
                volume_score REAL,
                pair_score REAL,
                intelligence_score REAL,
                score_mean REAL,
                score_std REAL,
                score_min REAL,
                score_max REAL,
                score_range REAL,
                is_high_price_impact INTEGER,
                is_high_cost INTEGER,
                is_low_liquidity INTEGER,
                is_low_activity INTEGER,
                enrichment_quality_score REAL,
                enrichment_missing_fields TEXT,
                enrichment_schema_version TEXT,
                raw_payload_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_scanner_enrichment_cycle
            ON scanner_enrichment(cycle_number, scan_time)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_scanner_enrichment_token
            ON scanner_enrichment(token, scan_time)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_scanner_enrichment_profit
            ON scanner_enrichment(net_profit_usd)
            """
        )

        connection.commit()

    finally:
        connection.close()


def save_enriched_result(
    enriched: Mapping[str, Any],
    *,
    database_path: str | Path = DATABASE,
) -> int:
    initialize_enrichment_table(
        database_path
    )

    connection = sqlite3.connect(
        Path(database_path)
    )

    try:
        cursor = connection.execute(
            """
            INSERT INTO scanner_enrichment (
                source_event_id,
                cycle_id,
                cycle_number,
                scan_time,
                token,
                mint,
                asset_key,
                quote_successful,
                decision,
                eligible,
                quote_latency_ms,
                quote_age_ms,
                is_stale_quote,
                is_high_latency,
                starting_amount_usd,
                ending_amount_usd,
                gross_profit_usd,
                estimated_cost_usd,
                net_profit_usd,
                gross_edge_bps,
                net_edge_bps,
                total_cost_bps,
                slippage_bps,
                price_impact_bps,
                liquidity_usd,
                volume_24h_usd,
                volume_liquidity_ratio,
                buy_route,
                sell_route,
                route_pair,
                route_hops,
                dex_count,
                market_score,
                liquidity_score,
                volume_score,
                pair_score,
                intelligence_score,
                score_mean,
                score_std,
                score_min,
                score_max,
                score_range,
                is_high_price_impact,
                is_high_cost,
                is_low_liquidity,
                is_low_activity,
                enrichment_quality_score,
                enrichment_missing_fields,
                enrichment_schema_version,
                raw_payload_json,
                created_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                first_present(
                    enriched,
                    "source_event_id",
                    "event_id",
                    "id",
                ),
                enriched.get("cycle_id"),
                safe_int(
                    enriched.get("cycle_number")
                ),
                first_present(
                    enriched,
                    "scan_time",
                    "quote_received_at",
                    "enriched_at",
                ),
                normalize_symbol(enriched),
                first_present(
                    enriched,
                    "mint",
                    "token_mint",
                ),
                (
                    str(
                        first_present(
                            enriched,
                            "asset_key",
                            default="",
                        )
                    ).strip()
                    or (
                        f"mint:{str(first_present(enriched, 'mint', 'token_mint')).strip()}"
                        if first_present(
                            enriched,
                            "mint",
                            "token_mint",
                            default=None,
                        )
                        else f"symbol:{normalize_symbol(enriched).upper()}"
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "quote_successful"
                        )
                    )
                ),
                str(
                    enriched.get(
                        "decision",
                        "",
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "eligible"
                        )
                    )
                ),
                safe_float(
                    enriched.get(
                        "quote_latency_ms"
                    )
                ),
                safe_float(
                    enriched.get(
                        "quote_age_ms"
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "is_stale_quote"
                        )
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "is_high_latency"
                        )
                    )
                ),
                safe_float(
                    enriched.get(
                        "starting_amount_usd"
                    )
                ),
                safe_float(
                    enriched.get(
                        "ending_amount_usd"
                    )
                ),
                safe_float(
                    enriched.get(
                        "gross_profit_usd"
                    )
                ),
                safe_float(
                    enriched.get(
                        "estimated_cost_usd"
                    )
                ),
                safe_float(
                    enriched.get(
                        "net_profit_usd"
                    )
                ),
                safe_float(
                    enriched.get(
                        "gross_edge_bps"
                    )
                ),
                safe_float(
                    enriched.get(
                        "net_edge_bps"
                    )
                ),
                safe_float(
                    enriched.get(
                        "total_cost_bps"
                    )
                ),
                safe_float(
                    enriched.get(
                        "slippage_bps"
                    )
                ),
                safe_float(
                    enriched.get(
                        "price_impact_bps"
                    )
                ),
                safe_float(
                    enriched.get(
                        "liquidity_usd_enriched"
                    )
                ),
                safe_float(
                    enriched.get(
                        "volume_24h_usd_enriched"
                    )
                ),
                safe_float(
                    enriched.get(
                        "volume_liquidity_ratio"
                    )
                ),
                enriched.get(
                    "buy_route_normalized"
                ),
                enriched.get(
                    "sell_route_normalized"
                ),
                enriched.get(
                    "route_pair_enriched"
                ),
                safe_int(
                    enriched.get(
                        "route_hops"
                    )
                ),
                safe_int(
                    enriched.get(
                        "dex_count"
                    )
                ),
                safe_float(
                    enriched.get(
                        "market_score"
                    )
                ),
                safe_float(
                    enriched.get(
                        "liquidity_score"
                    )
                ),
                safe_float(
                    enriched.get(
                        "volume_score"
                    )
                ),
                safe_float(
                    enriched.get(
                        "pair_score"
                    )
                ),
                safe_float(
                    first_present(
                        enriched,
                        "intelligence_score",
                        "ai_opportunity_score",
                    )
                ),
                safe_float(
                    enriched.get(
                        "score_mean"
                    )
                ),
                safe_float(
                    enriched.get(
                        "score_std"
                    )
                ),
                safe_float(
                    enriched.get(
                        "score_min"
                    )
                ),
                safe_float(
                    enriched.get(
                        "score_max"
                    )
                ),
                safe_float(
                    enriched.get(
                        "score_range"
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "is_high_price_impact"
                        )
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "is_high_cost"
                        )
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "is_low_liquidity"
                        )
                    )
                ),
                int(
                    safe_bool(
                        enriched.get(
                            "is_low_activity"
                        )
                    )
                ),
                safe_float(
                    enriched.get(
                        "enrichment_quality_score"
                    )
                ),
                json.dumps(
                    enriched.get(
                        "enrichment_missing_fields",
                        [],
                    ),
                    ensure_ascii=False,
                ),
                str(
                    enriched.get(
                        "enrichment_schema_version",
                        ENRICHMENT_SCHEMA_VERSION,
                    )
                ),
                json.dumps(
                    dict(enriched),
                    ensure_ascii=False,
                    default=str,
                    separators=(",", ":"),
                ),
                utc_now_text(),
            ),
        )

        connection.commit()

        return int(cursor.lastrowid)

    finally:
        connection.close()


def enrich_and_save_scan_result(
    result: Mapping[str, Any],
    *,
    cycle_context: Mapping[str, Any] | None = None,
    configuration: EnrichmentConfiguration | None = None,
) -> dict[str, Any]:
    """
    Scanner-safe convenience wrapper.

    Persistence failure is logged but the enriched result is still returned so
    the scanner can continue operating.
    """

    config = configuration or EnrichmentConfiguration()

    try:
        enriched = enrich_scan_result(
            result,
            cycle_context=cycle_context,
            configuration=config,
        )
    except Exception as error:
        LOGGER.exception(
            "Scanner enrichment failed: %s",
            error,
        )

        fallback = dict(result)
        fallback.update(
            {
                "enrichment_schema_version": (
                    ENRICHMENT_SCHEMA_VERSION
                ),
                "enrichment_error": str(error),
                "enrichment_quality_score": 0.0,
                "enrichment_missing_fields": [],
            }
        )

        return fallback

    try:
        row_id = save_enriched_result(
            enriched,
            database_path=config.database_path,
        )
        enriched["scanner_enrichment_id"] = row_id

    except Exception as error:
        LOGGER.exception(
            "Could not persist enriched scanner row: %s",
            error,
        )
        enriched["enrichment_persistence_error"] = str(
            error
        )

    return enriched


def save_enriched_results(
    results: Sequence[Mapping[str, Any]],
    *,
    cycle_context: Mapping[str, Any] | None = None,
    configuration: EnrichmentConfiguration | None = None,
) -> list[dict[str, Any]]:
    return [
        enrich_and_save_scan_result(
            result,
            cycle_context=cycle_context,
            configuration=configuration,
        )
        for result in results
    ]


def get_enrichment_summary(
    database_path: str | Path = DATABASE,
) -> EnrichmentSummary:
    initialize_enrichment_table(
        database_path
    )

    connection = sqlite3.connect(
        Path(database_path)
    )
    connection.row_factory = sqlite3.Row

    try:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN quote_successful = 0 THEN 1 ELSE 0 END)
                    AS quote_errors,
                SUM(CASE WHEN is_stale_quote = 1 THEN 1 ELSE 0 END)
                    AS stale_quotes,
                SUM(CASE WHEN is_high_latency = 1 THEN 1 ELSE 0 END)
                    AS high_latency_quotes,
                SUM(CASE WHEN is_low_liquidity = 1 THEN 1 ELSE 0 END)
                    AS low_liquidity_quotes,
                SUM(CASE WHEN is_high_price_impact = 1 THEN 1 ELSE 0 END)
                    AS high_price_impact_quotes,
                AVG(quote_latency_ms) AS average_quote_latency_ms,
                AVG(quote_age_ms) AS average_quote_age_ms,
                AVG(price_impact_bps) AS average_price_impact_bps,
                AVG(total_cost_bps) AS average_total_cost_bps,
                MAX(created_at) AS updated_at
            FROM scanner_enrichment
            """
        ).fetchone()

        return EnrichmentSummary(
            total_rows=safe_int(
                row["total_rows"]
            ),
            quote_errors=safe_int(
                row["quote_errors"]
            ),
            stale_quotes=safe_int(
                row["stale_quotes"]
            ),
            high_latency_quotes=safe_int(
                row["high_latency_quotes"]
            ),
            low_liquidity_quotes=safe_int(
                row["low_liquidity_quotes"]
            ),
            high_price_impact_quotes=safe_int(
                row["high_price_impact_quotes"]
            ),
            average_quote_latency_ms=safe_float(
                row["average_quote_latency_ms"]
            ),
            average_quote_age_ms=safe_float(
                row["average_quote_age_ms"]
            ),
            average_price_impact_bps=safe_float(
                row["average_price_impact_bps"]
            ),
            average_total_cost_bps=safe_float(
                row["average_total_cost_bps"]
            ),
            updated_at=row["updated_at"],
        )

    finally:
        connection.close()


def _demo_result() -> dict[str, Any]:
    now = utc_now()
    started = now.timestamp() - 0.350

    return {
        "source_event_id": 1,
        "token": "DEMO",
        "mint": "DemoMint",
        "asset_key": "mint:DemoMint",
        "decision": "🔴 SKIP",
        "eligible": False,
        "quote_started_at": started,
        "quote_received_at": now.isoformat(),
        "starting_amount_usd": 100.0,
        "ending_amount_usd": 99.92,
        "estimated_cost_usd": 0.03,
        "network_fee_usd": 0.01,
        "dex_fee_usd": 0.01,
        "slippage_cost_usd": 0.01,
        "price_impact_pct": 0.15,
        "liquidity_usd": 150_000.0,
        "volume_24h_usd": 75_000.0,
        "buy_route": "Jupiter",
        "sell_route": "Orca",
        "route_hops": 2,
        "market_score": 70.0,
        "liquidity_score": 80.0,
        "volume_score": 65.0,
        "pair_score": 72.0,
        "intelligence_score": 68.0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 12A scanner data-enrichment utilities."
        )
    )

    parser.add_argument(
        "--database",
        default=str(DATABASE),
    )

    parser.add_argument(
        "--initialize",
        action="store_true",
    )

    parser.add_argument(
        "--demo",
        action="store_true",
    )

    parser.add_argument(
        "--summary",
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

    database_path = Path(
        args.database
    )

    configuration = EnrichmentConfiguration(
        database_path=database_path
    )

    try:
        if args.initialize:
            initialize_enrichment_table(
                database_path
            )
            print(
                "scanner_enrichment table initialized."
            )

        if args.demo:
            enriched = enrich_and_save_scan_result(
                _demo_result(),
                cycle_context={
                    "cycle_id": "DEMO-CYCLE",
                    "cycle_number": 1,
                    "scanner_speed": 25.0,
                },
                configuration=configuration,
            )

            print(
                json.dumps(
                    enriched,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )
            )

        if args.summary:
            summary = get_enrichment_summary(
                database_path
            )

            print(
                "\nPhase 12A — "
                "Scanner Data Enrichment"
            )
            print("=" * 80)
            print(
                f"Rows: {summary.total_rows}"
            )
            print(
                "Quote errors: "
                f"{summary.quote_errors}"
            )
            print(
                "Stale quotes: "
                f"{summary.stale_quotes}"
            )
            print(
                "High-latency quotes: "
                f"{summary.high_latency_quotes}"
            )
            print(
                "Low-liquidity quotes: "
                f"{summary.low_liquidity_quotes}"
            )
            print(
                "High-impact quotes: "
                f"{summary.high_price_impact_quotes}"
            )
            print(
                "Average quote latency: "
                f"{summary.average_quote_latency_ms:.2f} ms"
            )
            print(
                "Average quote age: "
                f"{summary.average_quote_age_ms:.2f} ms"
            )
            print(
                "Average price impact: "
                f"{summary.average_price_impact_bps:.4f} bps"
            )
            print(
                "Average total cost: "
                f"{summary.average_total_cost_bps:.4f} bps"
            )
            print(
                f"Updated: {summary.updated_at}"
            )

        if not (
            args.initialize
            or args.demo
            or args.summary
        ):
            print(
                "Use --initialize, --demo, or --summary."
            )

    except (
        ScannerEnrichmentError,
        sqlite3.Error,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())