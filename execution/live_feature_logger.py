"""
Phase 12B — Live Scanner Feature Logging

Persists enriched live paper-scanner observations into a dedicated, append-only
SQLite feature log.

This module is research-only:
- It never changes scanner decisions.
- It never executes trades.
- It never requires a wallet.
- It never promotes models.
- It never modifies risk limits.
- Logging failure never stops the scanner.

Recommended integration
-----------------------
At the end of one completed scanner cycle:

    from execution.live_feature_logger import log_scanner_cycle

    results = log_scanner_cycle(
        results,
        cycle_context={
            "cycle_id": cycle_id,
            "cycle_number": cycle_number,
            "cycle_started_at": cycle_started_at,
            "cycle_finished_at": cycle_finished_at,
            "elapsed_seconds": elapsed_seconds,
            "scanner_speed": scanner_speed,
        },
    )

The returned list contains the original result dictionaries plus:
    live_feature_log_id
    live_feature_logged_at
    live_feature_logging_error   (only when persistence fails)
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import statistics
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

try:
    from execution.scanner_data_enrichment import (
        DATABASE,
        ENRICHMENT_SCHEMA_VERSION,
        EnrichmentConfiguration,
        ScannerEnrichmentError,
        enrich_scan_result,
        initialize_enrichment_table,
        save_enriched_result,
        safe_bool,
        safe_float,
        safe_int,
        utc_now,
        utc_now_text,
    )
except ModuleNotFoundError:
    from scanner_data_enrichment import (  # type: ignore
        DATABASE,
        ENRICHMENT_SCHEMA_VERSION,
        EnrichmentConfiguration,
        ScannerEnrichmentError,
        enrich_scan_result,
        initialize_enrichment_table,
        save_enriched_result,
        safe_bool,
        safe_float,
        safe_int,
        utc_now,
        utc_now_text,
    )


LOGGER = logging.getLogger(__name__)

LIVE_FEATURE_SCHEMA_VERSION = "12B.1.0"


class LiveFeatureLoggingError(RuntimeError):
    """Base exception for live feature logging failures."""


@dataclass(frozen=True, slots=True)
class LiveFeatureLoggerConfiguration:
    database_path: Path = DATABASE
    persist_scanner_enrichment: bool = True
    persist_cycle_summary: bool = True
    append_only: bool = True
    fail_open: bool = True

    def validate(self) -> None:
        if not str(self.database_path).strip():
            raise LiveFeatureLoggingError(
                "database_path cannot be empty."
            )


@dataclass(frozen=True, slots=True)
class CycleLoggingSummary:
    cycle_log_id: int | None
    cycle_id: str
    cycle_number: int
    observations_received: int
    observations_logged: int
    observations_failed: int
    quote_successes: int
    quote_errors: int
    eligible_observations: int
    profitable_observations: int
    average_net_profit_usd: float
    average_total_cost_bps: float
    average_quote_latency_ms: float
    average_quote_age_ms: float
    average_enrichment_quality_score: float
    started_at: str | None
    finished_at: str
    elapsed_seconds: float
    scanner_speed_tokens_per_minute: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def initialize_live_feature_logging(
    database_path: str | Path = DATABASE,
) -> None:
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    initialize_enrichment_table(path)

    connection = sqlite3.connect(path)

    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS live_scanner_features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                live_feature_schema_version TEXT NOT NULL,
                enrichment_schema_version TEXT NOT NULL,

                source_event_id INTEGER,
                scanner_enrichment_id INTEGER,

                cycle_id TEXT NOT NULL,
                cycle_number INTEGER NOT NULL,
                cycle_position INTEGER,
                cycle_size INTEGER,

                scan_time TEXT,
                logged_at TEXT NOT NULL,

                token TEXT,
                token_key TEXT,
                mint TEXT,
                asset_key TEXT,

                decision TEXT,
                eligible INTEGER,
                quote_successful INTEGER,

                selection_reason TEXT,
                ai_priority REAL,
                opportunity_probability REAL,
                expected_profit_usd REAL,
                combined_confidence REAL,
                prediction_confidence REAL,
                downside_risk REAL,
                trend_score REAL,
                stability_score REAL,

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

                network_fee_usd REAL,
                dex_fee_usd REAL,
                slippage_cost_usd REAL,

                liquidity_usd REAL,
                volume_24h_usd REAL,
                volume_liquidity_ratio REAL,

                buy_route TEXT,
                sell_route TEXT,
                route_pair TEXT,
                route_hops INTEGER,
                dex_count INTEGER,

                quote_latency_ms REAL,
                quote_age_ms REAL,

                is_stale_quote INTEGER,
                is_high_latency INTEGER,
                is_high_price_impact INTEGER,
                is_high_cost INTEGER,
                is_low_liquidity INTEGER,
                is_low_activity INTEGER,

                enrichment_quality_score REAL,
                enrichment_missing_fields TEXT,

                cycle_elapsed_seconds REAL,
                scanner_speed_tokens_per_minute REAL,

                raw_payload_json TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS live_scanner_cycles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                live_feature_schema_version TEXT NOT NULL,
                cycle_id TEXT NOT NULL,
                cycle_number INTEGER NOT NULL,
                started_at TEXT,
                finished_at TEXT NOT NULL,
                elapsed_seconds REAL,
                scanner_speed_tokens_per_minute REAL,

                observations_received INTEGER NOT NULL,
                observations_logged INTEGER NOT NULL,
                observations_failed INTEGER NOT NULL,
                quote_successes INTEGER NOT NULL,
                quote_errors INTEGER NOT NULL,
                eligible_observations INTEGER NOT NULL,
                profitable_observations INTEGER NOT NULL,

                average_net_profit_usd REAL,
                best_net_profit_usd REAL,
                worst_net_profit_usd REAL,
                average_total_cost_bps REAL,
                average_quote_latency_ms REAL,
                average_quote_age_ms REAL,
                average_enrichment_quality_score REAL,

                created_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_live_scanner_features_event_cycle
            ON live_scanner_features(
                cycle_id,
                source_event_id,
                token,
                scan_time
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_live_scanner_features_cycle
            ON live_scanner_features(
                cycle_number,
                cycle_position
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_live_scanner_features_token
            ON live_scanner_features(
                token,
                scan_time
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_live_scanner_features_profit
            ON live_scanner_features(
                net_profit_usd
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_live_scanner_features_quality
            ON live_scanner_features(
                enrichment_quality_score
            )
            """
        )

        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_live_scanner_cycles_identity
            ON live_scanner_cycles(
                cycle_id,
                cycle_number
            )
            """
        )

        connection.commit()

    finally:
        connection.close()


def _first_present(
    source: Mapping[str, Any],
    *keys: str,
    default: Any = None,
) -> Any:
    for key in keys:
        if key in source and source[key] is not None:
            return source[key]

    return default


def _text(
    value: Any,
    default: str = "",
) -> str:
    if value is None:
        return default

    result = str(value).strip()
    return result if result else default


def _json_text(
    value: Any,
) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )


def _cycle_identity(
    cycle_context: Mapping[str, Any] | None,
) -> tuple[str, int]:
    cycle = cycle_context or {}

    cycle_number = safe_int(
        _first_present(
            cycle,
            "cycle_number",
            default=0,
        )
    )

    cycle_id = _text(
        _first_present(
            cycle,
            "cycle_id",
            default="",
        )
    )

    if not cycle_id:
        cycle_id = (
            f"LIVE-CYCLE-{cycle_number:06d}-"
            f"{uuid.uuid4().hex[:8]}"
        )

    return cycle_id, cycle_number


def _prepare_enriched_result(
    result: Mapping[str, Any],
    *,
    cycle_context: Mapping[str, Any],
    configuration: LiveFeatureLoggerConfiguration,
    cycle_position: int,
    cycle_size: int,
) -> dict[str, Any]:
    enrichment_configuration = EnrichmentConfiguration(
        database_path=configuration.database_path
    )

    enriched = enrich_scan_result(
        result,
        cycle_context=cycle_context,
        configuration=enrichment_configuration,
    )

    enriched["cycle_position"] = cycle_position
    enriched["cycle_size"] = cycle_size

    if configuration.persist_scanner_enrichment:
        try:
            scanner_enrichment_id = save_enriched_result(
                enriched,
                database_path=configuration.database_path,
            )
            enriched["scanner_enrichment_id"] = (
                scanner_enrichment_id
            )
        except Exception as error:
            LOGGER.exception(
                "Could not save scanner_enrichment row: %s",
                error,
            )
            enriched["scanner_enrichment_persistence_error"] = str(
                error
            )

    return enriched


def _insert_live_feature_row(
    connection: sqlite3.Connection,
    enriched: Mapping[str, Any],
    *,
    cycle_context: Mapping[str, Any],
) -> int:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO live_scanner_features (
            live_feature_schema_version,
            enrichment_schema_version,
            source_event_id,
            scanner_enrichment_id,
            cycle_id,
            cycle_number,
            cycle_position,
            cycle_size,
            scan_time,
            logged_at,
            token,
            token_key,
            mint,
            asset_key,
            decision,
            eligible,
            quote_successful,
            selection_reason,
            ai_priority,
            opportunity_probability,
            expected_profit_usd,
            combined_confidence,
            prediction_confidence,
            downside_risk,
            trend_score,
            stability_score,
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
            network_fee_usd,
            dex_fee_usd,
            slippage_cost_usd,
            liquidity_usd,
            volume_24h_usd,
            volume_liquidity_ratio,
            buy_route,
            sell_route,
            route_pair,
            route_hops,
            dex_count,
            quote_latency_ms,
            quote_age_ms,
            is_stale_quote,
            is_high_latency,
            is_high_price_impact,
            is_high_cost,
            is_low_liquidity,
            is_low_activity,
            enrichment_quality_score,
            enrichment_missing_fields,
            cycle_elapsed_seconds,
            scanner_speed_tokens_per_minute,
            raw_payload_json
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            LIVE_FEATURE_SCHEMA_VERSION,
            _text(
                enriched.get(
                    "enrichment_schema_version"
                ),
                ENRICHMENT_SCHEMA_VERSION,
            ),
            _first_present(
                enriched,
                "source_event_id",
                "event_id",
                "id",
            ),
            enriched.get("scanner_enrichment_id"),
            _text(enriched.get("cycle_id")),
            safe_int(enriched.get("cycle_number")),
            safe_int(enriched.get("cycle_position")),
            safe_int(enriched.get("cycle_size")),
            _first_present(
                enriched,
                "scan_time",
                "quote_received_at",
                "enriched_at",
            ),
            utc_now_text(),
            _text(
                _first_present(
                    enriched,
                    "token",
                    "symbol",
                    "token_symbol",
                    default="UNKNOWN",
                ),
                "UNKNOWN",
            ),
            _text(
                _first_present(
                    enriched,
                    "token_key",
                    default="",
                )
            ),
            _first_present(
                enriched,
                "mint",
                "token_mint",
            ),
            _text(
                _first_present(
                    enriched,
                    "asset_key",
                    default="",
                )
            ),
            _text(enriched.get("decision")),
            int(safe_bool(enriched.get("eligible"))),
            int(
                safe_bool(
                    enriched.get("quote_successful")
                )
            ),
            _text(
                _first_present(
                    enriched,
                    "selection",
                    "selection_reason",
                    "selection_mode",
                )
            ),
            safe_float(
                _first_present(
                    enriched,
                    "ai_priority",
                    "ai_opportunity",
                    "ai_opportunity_score",
                )
            ),
            safe_float(
                _first_present(
                    enriched,
                    "opportunity_probability",
                    "prediction_probability",
                )
            ),
            safe_float(
                _first_present(
                    enriched,
                    "expected_profit",
                    "expected_profit_usd",
                )
            ),
            safe_float(
                enriched.get("combined_confidence")
            ),
            safe_float(
                enriched.get("prediction_confidence")
            ),
            safe_float(
                _first_present(
                    enriched,
                    "downside_risk",
                    "downside_risk_score",
                )
            ),
            safe_float(
                _first_present(
                    enriched,
                    "trend",
                    "trend_score",
                )
            ),
            safe_float(
                _first_present(
                    enriched,
                    "stability",
                    "stability_score",
                )
            ),
            safe_float(enriched.get("market_score")),
            safe_float(enriched.get("liquidity_score")),
            safe_float(enriched.get("volume_score")),
            safe_float(enriched.get("pair_score")),
            safe_float(
                _first_present(
                    enriched,
                    "intelligence_score",
                    "ai_opportunity_score",
                )
            ),
            safe_float(enriched.get("score_mean")),
            safe_float(enriched.get("score_std")),
            safe_float(enriched.get("score_min")),
            safe_float(enriched.get("score_max")),
            safe_float(enriched.get("score_range")),
            safe_float(
                enriched.get("starting_amount_usd")
            ),
            safe_float(
                enriched.get("ending_amount_usd")
            ),
            safe_float(
                enriched.get("gross_profit_usd")
            ),
            safe_float(
                enriched.get("estimated_cost_usd")
            ),
            safe_float(
                enriched.get("net_profit_usd")
            ),
            safe_float(
                enriched.get("gross_edge_bps")
            ),
            safe_float(
                enriched.get("net_edge_bps")
            ),
            safe_float(
                enriched.get("total_cost_bps")
            ),
            safe_float(
                enriched.get("slippage_bps")
            ),
            safe_float(
                enriched.get("price_impact_bps")
            ),
            safe_float(
                enriched.get("network_fee_usd")
            ),
            safe_float(
                enriched.get("dex_fee_usd")
            ),
            safe_float(
                enriched.get("slippage_cost_usd")
            ),
            safe_float(
                enriched.get("liquidity_usd_enriched")
            ),
            safe_float(
                enriched.get("volume_24h_usd_enriched")
            ),
            safe_float(
                enriched.get("volume_liquidity_ratio")
            ),
            _text(
                enriched.get("buy_route_normalized"),
                "UNKNOWN",
            ),
            _text(
                enriched.get("sell_route_normalized"),
                "UNKNOWN",
            ),
            _text(
                enriched.get("route_pair_enriched"),
                "UNKNOWN->UNKNOWN",
            ),
            safe_int(enriched.get("route_hops")),
            safe_int(enriched.get("dex_count")),
            safe_float(
                enriched.get("quote_latency_ms")
            ),
            safe_float(
                enriched.get("quote_age_ms")
            ),
            int(
                safe_bool(
                    enriched.get("is_stale_quote")
                )
            ),
            int(
                safe_bool(
                    enriched.get("is_high_latency")
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
                    enriched.get("is_high_cost")
                )
            ),
            int(
                safe_bool(
                    enriched.get("is_low_liquidity")
                )
            ),
            int(
                safe_bool(
                    enriched.get("is_low_activity")
                )
            ),
            safe_float(
                enriched.get(
                    "enrichment_quality_score"
                )
            ),
            _json_text(
                enriched.get(
                    "enrichment_missing_fields",
                    [],
                )
            ),
            safe_float(
                _first_present(
                    cycle_context,
                    "elapsed_seconds",
                    "cycle_elapsed_seconds",
                )
            ),
            safe_float(
                _first_present(
                    cycle_context,
                    "scanner_speed",
                    "scanner_speed_tokens_per_minute",
                )
            ),
            _json_text(dict(enriched)),
        ),
    )

    if cursor.rowcount == 0:
        row = connection.execute(
            """
            SELECT id
            FROM live_scanner_features
            WHERE cycle_id = ?
              AND source_event_id IS ?
              AND token = ?
              AND scan_time IS ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                _text(enriched.get("cycle_id")),
                _first_present(
                    enriched,
                    "source_event_id",
                    "event_id",
                    "id",
                ),
                _text(
                    _first_present(
                        enriched,
                        "token",
                        "symbol",
                        default="UNKNOWN",
                    ),
                    "UNKNOWN",
                ),
                _first_present(
                    enriched,
                    "scan_time",
                    "quote_received_at",
                    "enriched_at",
                ),
            ),
        ).fetchone()

        return int(row[0]) if row else 0

    return int(cursor.lastrowid)


def _cycle_summary_values(
    enriched_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    successful = [
        item
        for item in enriched_results
        if safe_bool(
            item.get("quote_successful")
        )
    ]

    profits = [
        safe_float(item.get("net_profit_usd"))
        for item in successful
    ]

    costs = [
        safe_float(item.get("total_cost_bps"))
        for item in successful
    ]

    latencies = [
        safe_float(item.get("quote_latency_ms"))
        for item in successful
    ]

    ages = [
        safe_float(item.get("quote_age_ms"))
        for item in successful
    ]

    qualities = [
        safe_float(
            item.get("enrichment_quality_score")
        )
        for item in enriched_results
    ]

    return {
        "quote_successes": len(successful),
        "quote_errors": (
            len(enriched_results) - len(successful)
        ),
        "eligible_observations": sum(
            safe_bool(item.get("eligible"))
            for item in enriched_results
        ),
        "profitable_observations": sum(
            safe_float(
                item.get("net_profit_usd")
            ) > 0
            for item in successful
        ),
        "average_net_profit_usd": (
            statistics.fmean(profits)
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
            statistics.fmean(costs)
            if costs
            else 0.0
        ),
        "average_quote_latency_ms": (
            statistics.fmean(latencies)
            if latencies
            else 0.0
        ),
        "average_quote_age_ms": (
            statistics.fmean(ages)
            if ages
            else 0.0
        ),
        "average_enrichment_quality_score": (
            statistics.fmean(qualities)
            if qualities
            else 0.0
        ),
    }


def _save_cycle_summary(
    connection: sqlite3.Connection,
    *,
    cycle_context: Mapping[str, Any],
    cycle_id: str,
    cycle_number: int,
    observations_received: int,
    observations_logged: int,
    observations_failed: int,
    enriched_results: Sequence[Mapping[str, Any]],
) -> int:
    values = _cycle_summary_values(
        enriched_results
    )

    cursor = connection.execute(
        """
        INSERT INTO live_scanner_cycles (
            live_feature_schema_version,
            cycle_id,
            cycle_number,
            started_at,
            finished_at,
            elapsed_seconds,
            scanner_speed_tokens_per_minute,
            observations_received,
            observations_logged,
            observations_failed,
            quote_successes,
            quote_errors,
            eligible_observations,
            profitable_observations,
            average_net_profit_usd,
            best_net_profit_usd,
            worst_net_profit_usd,
            average_total_cost_bps,
            average_quote_latency_ms,
            average_quote_age_ms,
            average_enrichment_quality_score,
            created_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?
        )
        ON CONFLICT(cycle_id, cycle_number)
        DO UPDATE SET
            finished_at = excluded.finished_at,
            elapsed_seconds = excluded.elapsed_seconds,
            scanner_speed_tokens_per_minute =
                excluded.scanner_speed_tokens_per_minute,
            observations_received = excluded.observations_received,
            observations_logged = excluded.observations_logged,
            observations_failed = excluded.observations_failed,
            quote_successes = excluded.quote_successes,
            quote_errors = excluded.quote_errors,
            eligible_observations = excluded.eligible_observations,
            profitable_observations = excluded.profitable_observations,
            average_net_profit_usd =
                excluded.average_net_profit_usd,
            best_net_profit_usd =
                excluded.best_net_profit_usd,
            worst_net_profit_usd =
                excluded.worst_net_profit_usd,
            average_total_cost_bps =
                excluded.average_total_cost_bps,
            average_quote_latency_ms =
                excluded.average_quote_latency_ms,
            average_quote_age_ms =
                excluded.average_quote_age_ms,
            average_enrichment_quality_score =
                excluded.average_enrichment_quality_score
        """,
        (
            LIVE_FEATURE_SCHEMA_VERSION,
            cycle_id,
            cycle_number,
            _first_present(
                cycle_context,
                "cycle_started_at",
                "started_at",
            ),
            _first_present(
                cycle_context,
                "cycle_finished_at",
                "finished_at",
                default=utc_now_text(),
            ),
            safe_float(
                _first_present(
                    cycle_context,
                    "elapsed_seconds",
                    "cycle_elapsed_seconds",
                )
            ),
            safe_float(
                _first_present(
                    cycle_context,
                    "scanner_speed",
                    "scanner_speed_tokens_per_minute",
                )
            ),
            observations_received,
            observations_logged,
            observations_failed,
            values["quote_successes"],
            values["quote_errors"],
            values["eligible_observations"],
            values["profitable_observations"],
            values["average_net_profit_usd"],
            values["best_net_profit_usd"],
            values["worst_net_profit_usd"],
            values["average_total_cost_bps"],
            values["average_quote_latency_ms"],
            values["average_quote_age_ms"],
            values["average_enrichment_quality_score"],
            utc_now_text(),
        ),
    )

    row = connection.execute(
        """
        SELECT id
        FROM live_scanner_cycles
        WHERE cycle_id = ?
          AND cycle_number = ?
        LIMIT 1
        """,
        (
            cycle_id,
            cycle_number,
        ),
    ).fetchone()

    return int(row[0]) if row else int(cursor.lastrowid or 0)


def log_scanner_cycle(
    results: Sequence[Mapping[str, Any]],
    *,
    cycle_context: Mapping[str, Any] | None = None,
    configuration: LiveFeatureLoggerConfiguration | None = None,
) -> list[dict[str, Any]]:
    """
    Enrich and persist one completed scanner cycle.

    Enrichment rows are saved before the live-feature transaction begins.
    This prevents SQLite writer contention caused by opening a second write
    connection while the live-feature connection already owns a transaction.

    The function is fail-open by default. Any individual logging error is
    recorded on that result and the remaining observations continue.
    """

    config = (
        configuration
        or LiveFeatureLoggerConfiguration()
    )
    config.validate()

    initialize_live_feature_logging(
        config.database_path
    )

    cycle_id, cycle_number = _cycle_identity(
        cycle_context
    )

    context = dict(cycle_context or {})
    context["cycle_id"] = cycle_id
    context["cycle_number"] = cycle_number

    cycle_size = len(results)

    prepared_results: list[dict[str, Any]] = []
    preparation_failures = 0

    # IMPORTANT:
    # Complete scanner_enrichment writes before opening the live-feature
    # transaction. SQLite allows only one writer at a time.
    for position, result in enumerate(
        results,
        start=1,
    ):
        try:
            enriched = _prepare_enriched_result(
                result,
                cycle_context=context,
                configuration=config,
                cycle_position=position,
                cycle_size=cycle_size,
            )
            prepared_results.append(enriched)

        except Exception as error:
            preparation_failures += 1
            LOGGER.exception(
                "Feature preparation failed for row %s: %s",
                position,
                error,
            )

            fallback = dict(result)
            fallback.update(
                {
                    "cycle_id": cycle_id,
                    "cycle_number": cycle_number,
                    "cycle_position": position,
                    "cycle_size": cycle_size,
                    "live_feature_logging_error": str(error),
                }
            )
            prepared_results.append(fallback)

            if not config.fail_open:
                raise

    connection = sqlite3.connect(
        config.database_path,
        timeout=30.0,
    )

    # WAL improves reader/writer coexistence. busy_timeout gives another
    # process time to finish a short transaction instead of failing at once.
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=30000")

    logged_results: list[dict[str, Any]] = []
    insertion_failures = 0

    try:
        connection.execute("BEGIN IMMEDIATE")

        for position, prepared in enumerate(
            prepared_results,
            start=1,
        ):
            if "live_feature_logging_error" in prepared:
                logged_results.append(prepared)
                continue

            try:
                row_id = _insert_live_feature_row(
                    connection,
                    prepared,
                    cycle_context=context,
                )

                prepared["live_feature_log_id"] = row_id
                prepared["live_feature_logged_at"] = utc_now_text()
                logged_results.append(prepared)

            except Exception as error:
                insertion_failures += 1
                LOGGER.exception(
                    "Live feature insertion failed for row %s: %s",
                    position,
                    error,
                )

                fallback = dict(prepared)
                fallback["live_feature_logging_error"] = str(error)
                logged_results.append(fallback)

                if not config.fail_open:
                    raise

        total_failures = (
            preparation_failures
            + insertion_failures
        )

        if config.persist_cycle_summary:
            cycle_log_id = _save_cycle_summary(
                connection,
                cycle_context=context,
                cycle_id=cycle_id,
                cycle_number=cycle_number,
                observations_received=cycle_size,
                observations_logged=(
                    cycle_size - total_failures
                ),
                observations_failed=total_failures,
                enriched_results=logged_results,
            )

            for item in logged_results:
                item["live_cycle_log_id"] = (
                    cycle_log_id
                )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return logged_results


def get_live_feature_summary(
    database_path: str | Path = DATABASE,
) -> dict[str, Any]:
    initialize_live_feature_logging(
        database_path
    )

    connection = sqlite3.connect(
        Path(database_path)
    )
    connection.row_factory = sqlite3.Row

    try:
        feature = connection.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT cycle_id) AS cycles,
                COUNT(DISTINCT asset_key) AS assets,
                SUM(
                    CASE WHEN quote_successful = 1
                    THEN 1 ELSE 0 END
                ) AS successful_quotes,
                SUM(
                    CASE WHEN quote_successful = 0
                    THEN 1 ELSE 0 END
                ) AS quote_errors,
                SUM(
                    CASE WHEN eligible = 1
                    THEN 1 ELSE 0 END
                ) AS eligible_observations,
                SUM(
                    CASE WHEN net_profit_usd > 0
                    THEN 1 ELSE 0 END
                ) AS profitable_observations,
                AVG(net_profit_usd) AS average_net_profit_usd,
                MAX(net_profit_usd) AS best_net_profit_usd,
                MIN(net_profit_usd) AS worst_net_profit_usd,
                AVG(total_cost_bps) AS average_total_cost_bps,
                AVG(quote_latency_ms) AS average_quote_latency_ms,
                AVG(quote_age_ms) AS average_quote_age_ms,
                AVG(enrichment_quality_score)
                    AS average_enrichment_quality_score,
                MAX(logged_at) AS updated_at
            FROM live_scanner_features
            """
        ).fetchone()

        cycles = connection.execute(
            """
            SELECT
                COUNT(*) AS cycle_rows,
                SUM(observations_received)
                    AS observations_received,
                SUM(observations_logged)
                    AS observations_logged,
                SUM(observations_failed)
                    AS observations_failed,
                AVG(scanner_speed_tokens_per_minute)
                    AS average_scanner_speed,
                AVG(elapsed_seconds)
                    AS average_elapsed_seconds
            FROM live_scanner_cycles
            """
        ).fetchone()

        return {
            "schema_version": (
                LIVE_FEATURE_SCHEMA_VERSION
            ),
            "rows": safe_int(feature["rows"]),
            "cycles": safe_int(feature["cycles"]),
            "assets": safe_int(feature["assets"]),
            "successful_quotes": safe_int(
                feature["successful_quotes"]
            ),
            "quote_errors": safe_int(
                feature["quote_errors"]
            ),
            "eligible_observations": safe_int(
                feature["eligible_observations"]
            ),
            "profitable_observations": safe_int(
                feature["profitable_observations"]
            ),
            "average_net_profit_usd": safe_float(
                feature["average_net_profit_usd"]
            ),
            "best_net_profit_usd": safe_float(
                feature["best_net_profit_usd"]
            ),
            "worst_net_profit_usd": safe_float(
                feature["worst_net_profit_usd"]
            ),
            "average_total_cost_bps": safe_float(
                feature["average_total_cost_bps"]
            ),
            "average_quote_latency_ms": safe_float(
                feature["average_quote_latency_ms"]
            ),
            "average_quote_age_ms": safe_float(
                feature["average_quote_age_ms"]
            ),
            "average_enrichment_quality_score": (
                safe_float(
                    feature[
                        "average_enrichment_quality_score"
                    ]
                )
            ),
            "cycle_rows": safe_int(
                cycles["cycle_rows"]
            ),
            "observations_received": safe_int(
                cycles["observations_received"]
            ),
            "observations_logged": safe_int(
                cycles["observations_logged"]
            ),
            "observations_failed": safe_int(
                cycles["observations_failed"]
            ),
            "average_scanner_speed": safe_float(
                cycles["average_scanner_speed"]
            ),
            "average_elapsed_seconds": safe_float(
                cycles["average_elapsed_seconds"]
            ),
            "updated_at": feature["updated_at"],
        }

    finally:
        connection.close()


def _demo_results() -> list[dict[str, Any]]:
    now = utc_now()

    return [
        {
            "source_event_id": 10_001,
            "token": "DEMO-A",
            "mint": "DemoMintA",
            "asset_key": "mint:DemoMintA",
            "decision": "🔴 SKIP",
            "eligible": False,
            "selection": "demo",
            "quote_received_at": now.isoformat(),
            "starting_amount_usd": 100.0,
            "ending_amount_usd": 99.95,
            "estimated_cost_usd": 0.02,
            "net_profit_usd": -0.07,
            "liquidity_usd": 200_000.0,
            "volume_24h_usd": 80_000.0,
            "market_score": 75.0,
            "liquidity_score": 82.0,
            "volume_score": 70.0,
            "pair_score": 74.0,
            "intelligence_score": 71.0,
        },
        {
            "source_event_id": 10_002,
            "token": "DEMO-B",
            "mint": "DemoMintB",
            "asset_key": "mint:DemoMintB",
            "decision": "🟡 WATCH",
            "eligible": False,
            "selection": "demo",
            "quote_received_at": now.isoformat(),
            "starting_amount_usd": 100.0,
            "ending_amount_usd": 100.01,
            "estimated_cost_usd": 0.02,
            "net_profit_usd": -0.01,
            "liquidity_usd": 500_000.0,
            "volume_24h_usd": 200_000.0,
            "market_score": 85.0,
            "liquidity_score": 88.0,
            "volume_score": 81.0,
            "pair_score": 80.0,
            "intelligence_score": 79.0,
        },
    ]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 12B live scanner feature logging."
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

    database_path = Path(args.database)

    configuration = LiveFeatureLoggerConfiguration(
        database_path=database_path
    )

    try:
        if args.initialize:
            initialize_live_feature_logging(
                database_path
            )
            print(
                "Live scanner feature tables initialized."
            )

        if args.demo:
            results = log_scanner_cycle(
                _demo_results(),
                cycle_context={
                    "cycle_id": "DEMO-LIVE-CYCLE",
                    "cycle_number": 1,
                    "cycle_started_at": utc_now_text(),
                    "cycle_finished_at": utc_now_text(),
                    "elapsed_seconds": 5.0,
                    "scanner_speed": 24.0,
                },
                configuration=configuration,
            )

            print(
                json.dumps(
                    results,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )
            )

        if args.summary:
            summary = get_live_feature_summary(
                database_path
            )

            print(
                "\nPhase 12B — "
                "Live Scanner Feature Logging"
            )
            print("=" * 80)
            print(
                f"Schema: {summary['schema_version']}"
            )
            print(
                f"Rows: {summary['rows']}"
            )
            print(
                f"Cycles: {summary['cycles']}"
            )
            print(
                f"Assets: {summary['assets']}"
            )
            print(
                "Successful quotes / errors: "
                f"{summary['successful_quotes']} / "
                f"{summary['quote_errors']}"
            )
            print(
                "Eligible / profitable: "
                f"{summary['eligible_observations']} / "
                f"{summary['profitable_observations']}"
            )
            print(
                "Average net profit: "
                f"${summary['average_net_profit_usd']:.6f}"
            )
            print(
                "Average total cost: "
                f"{summary['average_total_cost_bps']:.4f} bps"
            )
            print(
                "Average latency: "
                f"{summary['average_quote_latency_ms']:.2f} ms"
            )
            print(
                "Average quality score: "
                f"{summary['average_enrichment_quality_score']:.2f}/100"
            )
            print(
                "Logged / failed observations: "
                f"{summary['observations_logged']} / "
                f"{summary['observations_failed']}"
            )
            print(
                f"Updated: {summary['updated_at']}"
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
        LiveFeatureLoggingError,
        ScannerEnrichmentError,
        sqlite3.Error,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())