"""
Phase 14E — Execution-Aware Edge Filter and Shadow Revalidation

Derives an execution-aware minimum quoted edge from Phase 14A and 14B evidence,
filters fee-dominated opportunities, and revalidates the surviving source
trades under the same shadow-execution model.

Inputs
------
execution/shadow_results/shadow_execution_attempts.csv
execution/shadow_results/shadow_execution_trades.csv
execution/shadow_results/shadow_execution_report.json
execution/shadow_diagnostics/shadow_execution_diagnostics_report.json
research/institutional_walk_forward/walk_forward_trades.csv
research/institutional_walk_forward/institutional_walk_forward_report.json
research/institutional_promotion_gate/institutional_promotion_decision.json

Outputs
-------
execution/execution_aware_filter/
    execution_aware_candidates.csv
    execution_aware_rejections.csv
    execution_aware_thresholds.csv
    execution_aware_shadow_attempts.csv
    execution_aware_shadow_trades.csv
    execution_aware_gate_checks.csv
    execution_aware_report.json
    execution_aware_manifest.json

Safety
------
Research only. No wallet connection, signing, broadcasting, live execution,
or automatic promotion.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import random
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "14E.1.0"
OPERATING_MODE = "EXECUTION_AWARE_RESEARCH_ONLY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_SHADOW_ATTEMPTS = (
    PROJECT_ROOT / "execution" / "shadow_results" / "shadow_execution_attempts.csv"
)
DEFAULT_SHADOW_TRADES = (
    PROJECT_ROOT / "execution" / "shadow_results" / "shadow_execution_trades.csv"
)
DEFAULT_SHADOW_REPORT = (
    PROJECT_ROOT / "execution" / "shadow_results" / "shadow_execution_report.json"
)
DEFAULT_DIAGNOSTICS_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "shadow_diagnostics"
    / "shadow_execution_diagnostics_report.json"
)
DEFAULT_WALK_FORWARD_TRADES = (
    PROJECT_ROOT
    / "research"
    / "institutional_walk_forward"
    / "walk_forward_trades.csv"
)
DEFAULT_WALK_FORWARD_REPORT = (
    PROJECT_ROOT
    / "research"
    / "institutional_walk_forward"
    / "institutional_walk_forward_report.json"
)
DEFAULT_PROMOTION_DECISION = (
    PROJECT_ROOT
    / "research"
    / "institutional_promotion_gate"
    / "institutional_promotion_decision.json"
)
DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT / "execution" / "execution_aware_filter"
)

CANDIDATES_CSV = "execution_aware_candidates.csv"
REJECTIONS_CSV = "execution_aware_rejections.csv"
THRESHOLDS_CSV = "execution_aware_thresholds.csv"
ATTEMPTS_CSV = "execution_aware_shadow_attempts.csv"
TRADES_CSV = "execution_aware_shadow_trades.csv"
GATES_CSV = "execution_aware_gate_checks.csv"
REPORT_JSON = "execution_aware_report.json"
MANIFEST_JSON = "execution_aware_manifest.json"


class ExecutionAwareFilterError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Configuration:
    shadow_attempts: Path = DEFAULT_SHADOW_ATTEMPTS
    shadow_trades: Path = DEFAULT_SHADOW_TRADES
    shadow_report: Path = DEFAULT_SHADOW_REPORT
    diagnostics_report: Path = DEFAULT_DIAGNOSTICS_REPORT
    walk_forward_trades: Path = DEFAULT_WALK_FORWARD_TRADES
    walk_forward_report: Path = DEFAULT_WALK_FORWARD_REPORT
    promotion_decision: Path = DEFAULT_PROMOTION_DECISION
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    random_seed: int = 14_005
    attempts_per_trade: int = 250

    safety_multiplier: float = 1.50
    minimum_absolute_edge_usd: float = 0.005
    minimum_net_edge_bps: float = 5.0
    minimum_historical_profitable_rate: float = 0.50
    maximum_historical_fee_share: float = 0.80
    maximum_historical_penalty_share: float = 0.80

    base_network_fee_usd: float = 0.0005
    priority_fee_min_usd: float = 0.0002
    priority_fee_max_usd: float = 0.0050
    additional_slippage_min_bps: float = 0.0
    additional_slippage_max_bps: float = 15.0
    adverse_move_min_bps: float = 0.0
    adverse_move_max_bps: float = 10.0
    miss_probability: float = 0.05
    confirmation_probability: float = 0.985

    minimum_filtered_trades: int = 3
    minimum_confirmed_attempts: int = 100
    minimum_confirmation_rate: float = 0.90
    minimum_profitable_confirmation_rate: float = 0.60
    minimum_median_realized_profit_usd: float = 0.0
    maximum_single_trade_profit_concentration: float = 0.60
    maximum_failure_rate: float = 0.15

    def validate(self) -> None:
        for name in (
            "attempts_per_trade",
            "minimum_filtered_trades",
            "minimum_confirmed_attempts",
        ):
            if int(getattr(self, name)) <= 0:
                raise ExecutionAwareFilterError(f"{name} must be positive.")

        for name in (
            "safety_multiplier",
            "minimum_absolute_edge_usd",
            "minimum_net_edge_bps",
            "minimum_historical_profitable_rate",
            "maximum_historical_fee_share",
            "maximum_historical_penalty_share",
            "base_network_fee_usd",
            "priority_fee_min_usd",
            "priority_fee_max_usd",
            "additional_slippage_min_bps",
            "additional_slippage_max_bps",
            "adverse_move_min_bps",
            "adverse_move_max_bps",
            "miss_probability",
            "confirmation_probability",
            "minimum_confirmation_rate",
            "minimum_profitable_confirmation_rate",
            "minimum_median_realized_profit_usd",
            "maximum_single_trade_profit_concentration",
            "maximum_failure_rate",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise ExecutionAwareFilterError(f"{name} must be finite.")


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    observed: float
    comparison: str
    required: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    result = str(value).strip()
    return result or default


def load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ExecutionAwareFilterError(f"Required CSV missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ExecutionAwareFilterError(f"Required JSON missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ExecutionAwareFilterError(f"Expected JSON object: {path}")
    return payload


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    weight = position - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def inferred_amount(trade: Mapping[str, Any]) -> float:
    explicit = safe_float(trade.get("starting_amount_usd"))
    if explicit > 0:
        return explicit

    profit = abs(safe_float(trade.get("net_profit_usd")))
    edge_bps = abs(safe_float(trade.get("net_edge_bps")))
    if edge_bps > 0:
        inferred = profit / edge_bps * 10_000.0
        if inferred > 0:
            return inferred

    return 1.0


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ExecutionAwareFilterEngine:
    def __init__(self, configuration: Configuration) -> None:
        self.config = configuration
        self.config.validate()

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[GateCheck],
    ]:
        shadow_attempts = load_csv(self.config.shadow_attempts)
        shadow_trades = load_csv(self.config.shadow_trades)
        shadow_report = load_json(self.config.shadow_report)
        diagnostics_report = load_json(self.config.diagnostics_report)
        source_trades = load_csv(self.config.walk_forward_trades)
        walk_forward_report = load_json(self.config.walk_forward_report)
        promotion_decision = load_json(self.config.promotion_decision)

        if not source_trades:
            raise ExecutionAwareFilterError(
                "No walk-forward source trades are available."
            )

        thresholds = self._derive_thresholds(
            shadow_attempts,
            diagnostics_report,
        )

        historical_by_event = self._historical_profiles(
            shadow_attempts
        )

        candidates: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []

        for trade_index, trade in enumerate(source_trades, start=1):
            event_id = text(trade.get("event_id"))
            profile = historical_by_event.get(event_id, {})

            quoted_profit = safe_float(trade.get("net_profit_usd"))
            net_edge_bps = safe_float(trade.get("net_edge_bps"))

            reasons: list[str] = []

            if quoted_profit < thresholds["minimum_quoted_profit_usd"]:
                reasons.append("QUOTED_PROFIT_BELOW_EXECUTION_AWARE_THRESHOLD")

            if net_edge_bps < self.config.minimum_net_edge_bps:
                reasons.append("NET_EDGE_BPS_BELOW_MINIMUM")

            historical_profitable_rate = safe_float(
                profile.get("profitable_confirmation_rate")
            )
            historical_fee_share = safe_float(
                profile.get("median_fee_share")
            )
            historical_penalty_share = safe_float(
                profile.get("median_penalty_share")
            )

            if (
                profile
                and historical_profitable_rate
                < self.config.minimum_historical_profitable_rate
            ):
                reasons.append("HISTORICAL_PROFITABLE_RATE_TOO_LOW")

            if (
                profile
                and historical_fee_share
                > self.config.maximum_historical_fee_share
            ):
                reasons.append("HISTORICAL_FEE_SHARE_TOO_HIGH")

            if (
                profile
                and historical_penalty_share
                > self.config.maximum_historical_penalty_share
            ):
                reasons.append("HISTORICAL_EXECUTION_PENALTY_TOO_HIGH")

            row = {
                "trade_index": trade_index,
                **dict(trade),
                "historical_confirmed_attempts": safe_int(
                    profile.get("confirmed_attempts")
                ),
                "historical_profitable_confirmation_rate": (
                    historical_profitable_rate
                ),
                "historical_median_fee_share": historical_fee_share,
                "historical_median_penalty_share": historical_penalty_share,
                "execution_aware_minimum_profit_usd": (
                    thresholds["minimum_quoted_profit_usd"]
                ),
                "execution_aware_minimum_edge_bps": (
                    self.config.minimum_net_edge_bps
                ),
                "accepted": not reasons,
                "rejection_reasons": ";".join(reasons),
            }

            if reasons:
                rejections.append(row)
            else:
                candidates.append(row)

        (
            revalidation_attempts,
            revalidation_trade_rows,
        ) = self._shadow_revalidate(candidates)

        confirmed = [
            row
            for row in revalidation_attempts
            if safe_bool(row["confirmed"])
        ]
        realized = [
            safe_float(row["realized_profit_usd"])
            for row in confirmed
        ]

        confirmation_rate = safe_ratio(
            len(confirmed),
            len(revalidation_attempts),
        )
        failure_rate = safe_ratio(
            len(revalidation_attempts) - len(confirmed),
            len(revalidation_attempts),
        )
        profitable_confirmation_rate = safe_ratio(
            sum(value > 0 for value in realized),
            len(realized),
        )
        median_profit = percentile(realized, 0.50)

        positive_by_trade = [
            max(0.0, safe_float(row["total_realized_profit_usd"]))
            for row in revalidation_trade_rows
        ]
        total_positive = sum(positive_by_trade)
        concentration = safe_ratio(
            max(positive_by_trade, default=0.0),
            total_positive,
        )

        gates = [
            GateCheck(
                "FILTERED_SOURCE_TRADES",
                len(candidates) >= self.config.minimum_filtered_trades,
                float(len(candidates)),
                ">=",
                float(self.config.minimum_filtered_trades),
                "Too few source trades survive the execution-aware filter.",
            ),
            GateCheck(
                "CONFIRMED_REVALIDATION_ATTEMPTS",
                len(confirmed) >= self.config.minimum_confirmed_attempts,
                float(len(confirmed)),
                ">=",
                float(self.config.minimum_confirmed_attempts),
                "Too few filtered shadow attempts confirmed.",
            ),
            GateCheck(
                "CONFIRMATION_RATE",
                confirmation_rate >= self.config.minimum_confirmation_rate,
                confirmation_rate,
                ">=",
                self.config.minimum_confirmation_rate,
                "Filtered confirmation rate is below the gate.",
            ),
            GateCheck(
                "FAILURE_RATE",
                failure_rate <= self.config.maximum_failure_rate,
                failure_rate,
                "<=",
                self.config.maximum_failure_rate,
                "Filtered failure rate exceeds the gate.",
            ),
            GateCheck(
                "PROFITABLE_CONFIRMATION_RATE",
                profitable_confirmation_rate
                >= self.config.minimum_profitable_confirmation_rate,
                profitable_confirmation_rate,
                ">=",
                self.config.minimum_profitable_confirmation_rate,
                "Too few filtered confirmations remain profitable.",
            ),
            GateCheck(
                "MEDIAN_REALIZED_PROFIT",
                median_profit
                >= self.config.minimum_median_realized_profit_usd,
                median_profit,
                ">=",
                self.config.minimum_median_realized_profit_usd,
                "Filtered median realized profit remains negative.",
            ),
            GateCheck(
                "SINGLE_TRADE_CONCENTRATION",
                concentration
                <= self.config.maximum_single_trade_profit_concentration,
                concentration,
                "<=",
                self.config.maximum_single_trade_profit_concentration,
                "Filtered profits remain too concentrated.",
            ),
        ]

        filter_passed = all(gate.passed for gate in gates)

        shadow_summary = shadow_report.get("summary", {})
        diagnostics_summary = diagnostics_report.get("summary", {})
        walk_summary = walk_forward_report.get("summary", {})

        blocking_reasons: list[str] = []

        if not filter_passed:
            blocking_reasons.append(
                "One or more execution-aware filter gates failed."
            )

        if len(source_trades) < 30:
            blocking_reasons.append(
                "Fewer than 30 independent out-of-sample source trades."
            )

        if not safe_bool(walk_summary.get("promotion_allowed")):
            blocking_reasons.append(
                "Phase 13D walk-forward promotion remains blocked."
            )

        if not safe_bool(
            promotion_decision.get("live_readiness_allowed")
        ):
            blocking_reasons.append(
                "Phase 13F live readiness remains blocked."
            )

        summary = {
            "generated_at": utc_now(),
            "schema_version": SCHEMA_VERSION,
            "operating_mode": OPERATING_MODE,
            "source_trades": len(source_trades),
            "accepted_trades": len(candidates),
            "rejected_trades": len(rejections),
            "acceptance_rate": safe_ratio(
                len(candidates),
                len(source_trades),
            ),
            "derived_minimum_quoted_profit_usd": (
                thresholds["minimum_quoted_profit_usd"]
            ),
            "derived_median_network_fee_usd": (
                thresholds["median_network_fee_usd"]
            ),
            "derived_median_execution_penalty_usd": (
                thresholds["median_execution_penalty_usd"]
            ),
            "derived_median_total_degradation_usd": (
                thresholds["median_total_degradation_usd"]
            ),
            "revalidation_attempts": len(revalidation_attempts),
            "confirmed_revalidation_attempts": len(confirmed),
            "confirmation_rate": confirmation_rate,
            "failure_rate": failure_rate,
            "profitable_confirmation_rate": profitable_confirmation_rate,
            "median_realized_profit_usd": median_profit,
            "mean_realized_profit_usd": (
                statistics.fmean(realized) if realized else 0.0
            ),
            "total_realized_profit_usd": sum(realized),
            "single_trade_profit_concentration": concentration,
            "filter_passed": filter_passed,
            "final_decision": (
                "EXECUTION_AWARE_FILTER_VALIDATED"
                if filter_passed and not blocking_reasons
                else "BLOCK_LIVE_EXECUTION"
            ),
            "blocking_reasons": blocking_reasons,
            "upstream": {
                "phase_14a_operational_gate_passed": safe_bool(
                    shadow_summary.get("operational_gate_passed")
                ),
                "phase_14b_diagnostics_passed": safe_bool(
                    diagnostics_summary.get("diagnostics_passed")
                ),
                "phase_13d_promotion_allowed": safe_bool(
                    walk_summary.get("promotion_allowed")
                ),
                "phase_13f_live_readiness_allowed": safe_bool(
                    promotion_decision.get("live_readiness_allowed")
                ),
            },
            "safety": {
                "wallet_connected": False,
                "transaction_signing_enabled": False,
                "transaction_broadcasting_enabled": False,
                "live_execution_enabled": False,
                "automatic_promotion_enabled": False,
            },
            "valid": True,
        }

        threshold_rows = [
            {
                "threshold": key,
                "value": value,
            }
            for key, value in thresholds.items()
        ]

        return (
            summary,
            candidates,
            rejections,
            threshold_rows,
            revalidation_attempts,
            revalidation_trade_rows,
            gates,
        )

    def _derive_thresholds(
        self,
        attempts: Sequence[Mapping[str, Any]],
        diagnostics_report: Mapping[str, Any],
    ) -> dict[str, float]:
        confirmed = [
            row
            for row in attempts
            if safe_bool(row.get("confirmed"))
        ]

        network_fees = [
            safe_float(row.get("total_network_fee_usd"))
            for row in confirmed
        ]
        execution_penalties = [
            safe_float(row.get("execution_penalty_usd"))
            for row in confirmed
        ]
        degradations = [
            safe_float(row.get("profit_degradation_usd"))
            for row in confirmed
        ]

        median_network_fee = percentile(network_fees, 0.50)
        median_execution_penalty = percentile(
            execution_penalties,
            0.50,
        )
        median_total_degradation = percentile(
            degradations,
            0.50,
        )

        baseline = max(
            self.config.minimum_absolute_edge_usd,
            median_network_fee + median_execution_penalty,
            median_total_degradation,
        )

        minimum_quoted_profit = (
            baseline * self.config.safety_multiplier
        )

        return {
            "median_network_fee_usd": median_network_fee,
            "median_execution_penalty_usd": median_execution_penalty,
            "median_total_degradation_usd": median_total_degradation,
            "safety_multiplier": self.config.safety_multiplier,
            "minimum_absolute_edge_usd": self.config.minimum_absolute_edge_usd,
            "minimum_quoted_profit_usd": minimum_quoted_profit,
            "minimum_net_edge_bps": self.config.minimum_net_edge_bps,
        }

    @staticmethod
    def _historical_profiles(
        attempts: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        grouped: dict[str, list[Mapping[str, Any]]] = {}

        for row in attempts:
            event_id = text(row.get("event_id"))
            grouped.setdefault(event_id, []).append(row)

        output: dict[str, dict[str, Any]] = {}

        for event_id, rows in grouped.items():
            confirmed = [
                row
                for row in rows
                if safe_bool(row.get("confirmed"))
            ]

            realized = [
                safe_float(row.get("realized_net_profit_usd"))
                for row in confirmed
            ]

            fee_shares = []
            penalty_shares = []

            for row in confirmed:
                quoted = abs(
                    safe_float(row.get("quoted_net_profit_usd"))
                )

                if quoted <= 0:
                    continue

                fee_shares.append(
                    safe_float(row.get("total_network_fee_usd"))
                    / quoted
                )

                penalty_shares.append(
                    safe_float(row.get("execution_penalty_usd"))
                    / quoted
                )

            output[event_id] = {
                "confirmed_attempts": len(confirmed),
                "profitable_confirmation_rate": safe_ratio(
                    sum(value > 0 for value in realized),
                    len(realized),
                ),
                "median_fee_share": percentile(fee_shares, 0.50),
                "median_penalty_share": percentile(
                    penalty_shares,
                    0.50,
                ),
            }

        return output

    def _shadow_revalidate(
        self,
        candidates: Sequence[Mapping[str, Any]],
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        rng = random.Random(self.config.random_seed)

        attempts: list[dict[str, Any]] = []
        trade_rows: list[dict[str, Any]] = []

        for candidate_index, trade in enumerate(candidates, start=1):
            trade_attempts: list[dict[str, Any]] = []

            quoted_profit = safe_float(trade.get("net_profit_usd"))
            amount = inferred_amount(trade)

            for attempt_number in range(
                1,
                self.config.attempts_per_trade + 1,
            ):
                missed = rng.random() < self.config.miss_probability
                confirmed = (
                    not missed
                    and rng.random() < self.config.confirmation_probability
                )

                priority_fee = rng.uniform(
                    self.config.priority_fee_min_usd,
                    self.config.priority_fee_max_usd,
                )

                total_fee = (
                    self.config.base_network_fee_usd
                    + priority_fee
                )

                slippage_bps = rng.uniform(
                    self.config.additional_slippage_min_bps,
                    self.config.additional_slippage_max_bps,
                )

                adverse_bps = rng.uniform(
                    self.config.adverse_move_min_bps,
                    self.config.adverse_move_max_bps,
                )

                execution_penalty = (
                    amount
                    * (slippage_bps + adverse_bps)
                    / 10_000.0
                )

                realized_profit = (
                    quoted_profit
                    - total_fee
                    - execution_penalty
                    if confirmed
                    else 0.0
                )

                row = {
                    "candidate_index": candidate_index,
                    "attempt_number": attempt_number,
                    "event_id": trade.get("event_id"),
                    "cycle_id": trade.get("cycle_id"),
                    "token": trade.get("token"),
                    "asset_key": trade.get("asset_key"),
                    "quoted_profit_usd": quoted_profit,
                    "starting_amount_usd": amount,
                    "priority_fee_usd": priority_fee,
                    "total_network_fee_usd": total_fee,
                    "slippage_bps": slippage_bps,
                    "adverse_move_bps": adverse_bps,
                    "execution_penalty_usd": execution_penalty,
                    "missed": missed,
                    "confirmed": confirmed,
                    "realized_profit_usd": realized_profit,
                }

                attempts.append(row)
                trade_attempts.append(row)

            confirmed_rows = [
                row
                for row in trade_attempts
                if safe_bool(row["confirmed"])
            ]

            realized = [
                safe_float(row["realized_profit_usd"])
                for row in confirmed_rows
            ]

            trade_rows.append(
                {
                    "candidate_index": candidate_index,
                    "event_id": trade.get("event_id"),
                    "cycle_id": trade.get("cycle_id"),
                    "token": trade.get("token"),
                    "asset_key": trade.get("asset_key"),
                    "quoted_profit_usd": quoted_profit,
                    "attempts": len(trade_attempts),
                    "confirmed": len(confirmed_rows),
                    "confirmation_rate": safe_ratio(
                        len(confirmed_rows),
                        len(trade_attempts),
                    ),
                    "profitable_confirmation_rate": safe_ratio(
                        sum(value > 0 for value in realized),
                        len(realized),
                    ),
                    "median_realized_profit_usd": percentile(
                        realized,
                        0.50,
                    ),
                    "mean_realized_profit_usd": (
                        statistics.fmean(realized)
                        if realized
                        else 0.0
                    ),
                    "total_realized_profit_usd": sum(realized),
                }
            )

        return attempts, trade_rows


def export_results(
    *,
    summary: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    rejections: Sequence[Mapping[str, Any]],
    thresholds: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
    trade_rows: Sequence[Mapping[str, Any]],
    gates: Sequence[GateCheck],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(parents=True, exist_ok=True)

    paths = {
        "candidates": output / CANDIDATES_CSV,
        "rejections": output / REJECTIONS_CSV,
        "thresholds": output / THRESHOLDS_CSV,
        "attempts": output / ATTEMPTS_CSV,
        "trades": output / TRADES_CSV,
        "gates": output / GATES_CSV,
        "report": output / REPORT_JSON,
        "manifest": output / MANIFEST_JSON,
    }

    if not configuration.overwrite:
        existing = [path for path in paths.values() if path.exists()]
        if existing:
            raise ExecutionAwareFilterError(
                "Refusing to overwrite: "
                + ", ".join(str(path) for path in existing)
            )

    write_csv(paths["candidates"], candidates)
    write_csv(paths["rejections"], rejections)
    write_csv(paths["thresholds"], thresholds)
    write_csv(paths["attempts"], attempts)
    write_csv(paths["trades"], trade_rows)
    write_csv(paths["gates"], [gate.to_dict() for gate in gates])

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(configuration),
                    "shadow_attempts": str(configuration.shadow_attempts),
                    "shadow_trades": str(configuration.shadow_trades),
                    "shadow_report": str(configuration.shadow_report),
                    "diagnostics_report": str(
                        configuration.diagnostics_report
                    ),
                    "walk_forward_trades": str(
                        configuration.walk_forward_trades
                    ),
                    "walk_forward_report": str(
                        configuration.walk_forward_report
                    ),
                    "promotion_decision": str(
                        configuration.promotion_decision
                    ),
                    "output_directory": str(
                        configuration.output_directory
                    ),
                },
                "gate_checks": [
                    gate.to_dict()
                    for gate in gates
                ],
                "governance": {
                    "research_only": True,
                    "wallet_connection_authorized": False,
                    "transaction_signing_enabled": False,
                    "transaction_broadcasting_enabled": False,
                    "live_execution_enabled": False,
                    "automatic_promotion_enabled": False,
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    row_counts = {
        "candidates": len(candidates),
        "rejections": len(rejections),
        "thresholds": len(thresholds),
        "attempts": len(attempts),
        "trades": len(trade_rows),
        "gates": len(gates),
        "report": None,
    }

    files: dict[str, Any] = {}

    for name, path in paths.items():
        if name == "manifest":
            continue

        files[path.name] = {
            "path": str(path),
            "rows": row_counts.get(name),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }

    paths["manifest"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": utc_now(),
                "summary": dict(summary),
                "inputs": {
                    "shadow_attempts": {
                        "path": str(configuration.shadow_attempts),
                        "sha256": sha256_file(configuration.shadow_attempts),
                    },
                    "diagnostics_report": {
                        "path": str(configuration.diagnostics_report),
                        "sha256": sha256_file(
                            configuration.diagnostics_report
                        ),
                    },
                    "walk_forward_trades": {
                        "path": str(configuration.walk_forward_trades),
                        "sha256": sha256_file(
                            configuration.walk_forward_trades
                        ),
                    },
                },
                "outputs": files,
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    return tuple(paths.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 14E execution-aware edge filtering "
            "and shadow revalidation."
        )
    )

    parser.add_argument(
        "--shadow-attempts",
        default=str(DEFAULT_SHADOW_ATTEMPTS),
    )
    parser.add_argument(
        "--shadow-trades",
        default=str(DEFAULT_SHADOW_TRADES),
    )
    parser.add_argument(
        "--shadow-report",
        default=str(DEFAULT_SHADOW_REPORT),
    )
    parser.add_argument(
        "--diagnostics-report",
        default=str(DEFAULT_DIAGNOSTICS_REPORT),
    )
    parser.add_argument(
        "--walk-forward-trades",
        default=str(DEFAULT_WALK_FORWARD_TRADES),
    )
    parser.add_argument(
        "--walk-forward-report",
        default=str(DEFAULT_WALK_FORWARD_REPORT),
    )
    parser.add_argument(
        "--promotion-decision",
        default=str(DEFAULT_PROMOTION_DECISION),
    )
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    parser.add_argument(
        "--attempts-per-trade",
        type=int,
        default=250,
    )
    parser.add_argument(
        "--safety-multiplier",
        type=float,
        default=1.50,
    )
    parser.add_argument(
        "--minimum-net-edge-bps",
        type=float,
        default=5.0,
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


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    configuration = Configuration(
        shadow_attempts=Path(args.shadow_attempts),
        shadow_trades=Path(args.shadow_trades),
        shadow_report=Path(args.shadow_report),
        diagnostics_report=Path(args.diagnostics_report),
        walk_forward_trades=Path(args.walk_forward_trades),
        walk_forward_report=Path(args.walk_forward_report),
        promotion_decision=Path(args.promotion_decision),
        output_directory=Path(args.output_directory),
        overwrite=not args.no_overwrite,
        attempts_per_trade=args.attempts_per_trade,
        safety_multiplier=args.safety_multiplier,
        minimum_net_edge_bps=args.minimum_net_edge_bps,
    )

    try:
        (
            summary,
            candidates,
            rejections,
            thresholds,
            attempts,
            trade_rows,
            gates,
        ) = ExecutionAwareFilterEngine(configuration).run()

        output_paths = export_results(
            summary=summary,
            candidates=candidates,
            rejections=rejections,
            thresholds=thresholds,
            attempts=attempts,
            trade_rows=trade_rows,
            gates=gates,
            configuration=configuration,
        )

    except (
        ExecutionAwareFilterError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print(
        "\nPhase 14E — Execution-Aware Edge Filter "
        "and Shadow Revalidation"
    )
    print("=" * 80)
    print(f"Operating mode: {summary['operating_mode']}")
    print()

    print("Filter Results")
    print("-" * 80)
    print(
        "Source / accepted / rejected trades: "
        f"{summary['source_trades']} / "
        f"{summary['accepted_trades']} / "
        f"{summary['rejected_trades']}"
    )
    print(
        "Acceptance rate: "
        f"{summary['acceptance_rate'] * 100:.2f}%"
    )
    print(
        "Derived minimum quoted profit: "
        f"${summary['derived_minimum_quoted_profit_usd']:.6f}"
    )
    print(
        "Median network fee / execution penalty: "
        f"${summary['derived_median_network_fee_usd']:.6f} / "
        f"${summary['derived_median_execution_penalty_usd']:.6f}"
    )
    print()

    print("Shadow Revalidation")
    print("-" * 80)
    print(
        "Attempts / confirmed: "
        f"{summary['revalidation_attempts']} / "
        f"{summary['confirmed_revalidation_attempts']}"
    )
    print(
        "Confirmation / failure rate: "
        f"{summary['confirmation_rate'] * 100:.2f}% / "
        f"{summary['failure_rate'] * 100:.2f}%"
    )
    print(
        "Profitable confirmation rate: "
        f"{summary['profitable_confirmation_rate'] * 100:.2f}%"
    )
    print(
        "Median / mean realized profit: "
        f"${summary['median_realized_profit_usd']:.6f} / "
        f"${summary['mean_realized_profit_usd']:.6f}"
    )
    print(
        "Total realized profit: "
        f"${summary['total_realized_profit_usd']:.6f}"
    )
    print(
        "Single-trade concentration: "
        f"{summary['single_trade_profit_concentration'] * 100:.2f}%"
    )
    print()

    print("Gate Checks")
    print("-" * 80)

    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:32} | "
            f"{gate.observed:.6f} "
            f"{gate.comparison} {gate.required:.6f}"
        )

        if not gate.passed:
            print(f"       {gate.message}")

    print()
    print(f"Filter passed: {summary['filter_passed']}")
    print(f"Final decision: {summary['final_decision']}")

    if summary["blocking_reasons"]:
        print("Blocking reasons:")
        for reason in summary["blocking_reasons"]:
            print(f"  - {reason}")

    print()
    print("Safety")
    print("-" * 80)
    print("Wallet connected: False")
    print("Transaction signing: False")
    print("Transaction broadcasting: False")
    print("Live execution: False")
    print()

    print("Output files")
    print("-" * 80)
    for path in output_paths:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())