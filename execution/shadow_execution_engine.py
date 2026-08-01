"""
Phase 14A — Production Shadow Execution Engine

Repository-integrated, research-only execution simulator for Solan-Arbitrage-Bot.

Integration points
------------------
Reads:
- research/institutional_walk_forward/walk_forward_trades.csv
- research/institutional_walk_forward/institutional_walk_forward_report.json
- research/institutional_robustness/institutional_robustness_report.json
- research/institutional_promotion_gate/institutional_promotion_decision.json
- database/trades.db (optional audit persistence only)

Writes:
- execution/shadow_results/shadow_execution_trades.csv
- execution/shadow_results/shadow_execution_attempts.csv
- execution/shadow_results/shadow_execution_equity_curve.csv
- execution/shadow_results/shadow_execution_gate_checks.csv
- execution/shadow_results/shadow_execution_report.json
- execution/shadow_results/shadow_execution_manifest.json
- database/trades.db:
    shadow_execution_runs
    shadow_execution_attempts
  unless --no-database-write is supplied.

Safety invariants
-----------------
- No wallet import.
- No private-key access.
- No signing.
- No RPC transaction submission.
- No transaction broadcasting.
- No live execution.
- No automatic strategy promotion.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import random
import sqlite3
import statistics
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "14A.2.0"
OPERATING_MODE = "SHADOW_EXECUTION_ONLY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_TRADES_CSV = (
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
DEFAULT_ROBUSTNESS_REPORT = (
    PROJECT_ROOT
    / "research"
    / "institutional_robustness"
    / "institutional_robustness_report.json"
)
DEFAULT_PROMOTION_DECISION = (
    PROJECT_ROOT
    / "research"
    / "institutional_promotion_gate"
    / "institutional_promotion_decision.json"
)
DEFAULT_DATABASE = PROJECT_ROOT / "database" / "trades.db"
DEFAULT_OUTPUT_DIRECTORY = PROJECT_ROOT / "execution" / "shadow_results"

OUTPUT_TRADES = "shadow_execution_trades.csv"
OUTPUT_ATTEMPTS = "shadow_execution_attempts.csv"
OUTPUT_EQUITY = "shadow_execution_equity_curve.csv"
OUTPUT_GATES = "shadow_execution_gate_checks.csv"
OUTPUT_REPORT = "shadow_execution_report.json"
OUTPUT_MANIFEST = "shadow_execution_manifest.json"


class ShadowExecutionError(RuntimeError):
    """Base exception for Phase 14A failures."""


@dataclass(frozen=True, slots=True)
class Configuration:
    trades_csv: Path = DEFAULT_TRADES_CSV
    walk_forward_report: Path = DEFAULT_WALK_FORWARD_REPORT
    robustness_report: Path = DEFAULT_ROBUSTNESS_REPORT
    promotion_decision: Path = DEFAULT_PROMOTION_DECISION
    database_path: Path = DEFAULT_DATABASE
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY

    overwrite: bool = True
    persist_database: bool = True
    random_seed: int = 14_002
    attempts_per_trade: int = 100
    initial_capital_usd: float = 1_000.0

    maximum_quote_age_ms: float = 1_500.0
    quote_to_build_min_ms: float = 20.0
    quote_to_build_max_ms: float = 150.0
    transaction_build_min_ms: float = 20.0
    transaction_build_max_ms: float = 120.0
    rpc_submission_min_ms: float = 20.0
    rpc_submission_max_ms: float = 250.0
    confirmation_min_ms: float = 250.0
    confirmation_max_ms: float = 2_500.0

    base_network_fee_usd: float = 0.0005
    priority_fee_min_usd: float = 0.0002
    priority_fee_max_usd: float = 0.0050

    base_confirmation_probability: float = 0.985
    stale_confirmation_multiplier: float = 0.30
    miss_probability: float = 0.05
    simulation_failure_probability: float = 0.005
    rpc_failure_probability: float = 0.010
    blockhash_expiry_probability: float = 0.005

    additional_slippage_min_bps: float = 0.0
    additional_slippage_max_bps: float = 15.0
    adverse_move_min_bps: float = 0.0
    adverse_move_max_bps: float = 10.0

    minimum_attempts: int = 500
    minimum_confirmed: int = 100
    minimum_confirmation_rate: float = 0.90
    maximum_stale_rate: float = 0.20
    maximum_failure_rate: float = 0.15
    minimum_median_realized_profit_usd: float = 0.0
    maximum_drawdown_percent: float = 10.0
    minimum_source_trades_for_promotion: int = 30

    def validate(self) -> None:
        positive_ints = (
            "attempts_per_trade",
            "minimum_attempts",
            "minimum_confirmed",
            "minimum_source_trades_for_promotion",
        )
        for name in positive_ints:
            if int(getattr(self, name)) <= 0:
                raise ShadowExecutionError(f"{name} must be positive.")

        finite_fields = (
            "initial_capital_usd",
            "maximum_quote_age_ms",
            "quote_to_build_min_ms",
            "quote_to_build_max_ms",
            "transaction_build_min_ms",
            "transaction_build_max_ms",
            "rpc_submission_min_ms",
            "rpc_submission_max_ms",
            "confirmation_min_ms",
            "confirmation_max_ms",
            "base_network_fee_usd",
            "priority_fee_min_usd",
            "priority_fee_max_usd",
            "base_confirmation_probability",
            "stale_confirmation_multiplier",
            "miss_probability",
            "simulation_failure_probability",
            "rpc_failure_probability",
            "blockhash_expiry_probability",
            "additional_slippage_min_bps",
            "additional_slippage_max_bps",
            "adverse_move_min_bps",
            "adverse_move_max_bps",
            "minimum_confirmation_rate",
            "maximum_stale_rate",
            "maximum_failure_rate",
            "minimum_median_realized_profit_usd",
            "maximum_drawdown_percent",
        )
        for name in finite_fields:
            if not math.isfinite(float(getattr(self, name))):
                raise ShadowExecutionError(f"{name} must be finite.")

        if self.initial_capital_usd <= 0:
            raise ShadowExecutionError("initial_capital_usd must be positive.")

        ranges = (
            ("quote_to_build_min_ms", "quote_to_build_max_ms"),
            ("transaction_build_min_ms", "transaction_build_max_ms"),
            ("rpc_submission_min_ms", "rpc_submission_max_ms"),
            ("confirmation_min_ms", "confirmation_max_ms"),
            ("priority_fee_min_usd", "priority_fee_max_usd"),
            ("additional_slippage_min_bps", "additional_slippage_max_bps"),
            ("adverse_move_min_bps", "adverse_move_max_bps"),
        )
        for low, high in ranges:
            if float(getattr(self, high)) < float(getattr(self, low)):
                raise ShadowExecutionError(f"{high} must be >= {low}.")

        probabilities = (
            "base_confirmation_probability",
            "stale_confirmation_multiplier",
            "miss_probability",
            "simulation_failure_probability",
            "rpc_failure_probability",
            "blockhash_expiry_probability",
            "minimum_confirmation_rate",
            "maximum_stale_rate",
            "maximum_failure_rate",
        )
        for name in probabilities:
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise ShadowExecutionError(f"{name} must be in [0, 1].")


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    blocking: bool
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


def normalized_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    result = str(value).strip()
    return result or default


def load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ShadowExecutionError(f"Required CSV does not exist: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ShadowExecutionError(f"Required JSON does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ShadowExecutionError(f"Expected JSON object: {path}")
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


def inferred_trade_amount(trade: Mapping[str, Any]) -> float:
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


class ShadowExecutionEngine:
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
        list[GateCheck],
    ]:
        source_trades = load_csv(self.config.trades_csv)
        walk_forward = load_json(self.config.walk_forward_report)
        robustness = load_json(self.config.robustness_report)
        promotion = load_json(self.config.promotion_decision)

        if not source_trades:
            raise ShadowExecutionError(
                "walk_forward_trades.csv contains no source trades."
            )

        rng = random.Random(self.config.random_seed)
        run_id = str(uuid.uuid4())
        started_at = utc_now()

        attempts: list[dict[str, Any]] = []
        trade_summaries: list[dict[str, Any]] = []
        confirmed_sequence: list[dict[str, Any]] = []

        for trade_index, trade in enumerate(source_trades, start=1):
            trade_attempts: list[dict[str, Any]] = []

            for attempt_number in range(1, self.config.attempts_per_trade + 1):
                attempt = self._simulate_attempt(
                    run_id=run_id,
                    trade=trade,
                    trade_index=trade_index,
                    attempt_number=attempt_number,
                    rng=rng,
                )
                attempts.append(attempt)
                trade_attempts.append(attempt)

                if safe_bool(attempt["confirmed"]):
                    confirmed_sequence.append(attempt)

            trade_summaries.append(
                self._summarize_trade(trade, trade_index, trade_attempts)
            )

        equity_curve = self._build_equity_curve(
            run_id,
            confirmed_sequence,
        )

        confirmed = [row for row in attempts if safe_bool(row["confirmed"])]
        failed = [
            row
            for row in attempts
            if row["status"] == "FAILED"
        ]
        missed = [
            row
            for row in attempts
            if row["status"] == "MISSED"
        ]
        stale = [row for row in attempts if safe_bool(row["quote_stale"])]
        realized = [
            safe_float(row["realized_net_profit_usd"])
            for row in confirmed
        ]

        total_attempts = len(attempts)
        confirmed_count = len(confirmed)
        failure_count = len(failed)
        missed_count = len(missed)
        stale_count = len(stale)

        confirmation_rate = (
            confirmed_count / total_attempts if total_attempts else 0.0
        )
        stale_rate = stale_count / total_attempts if total_attempts else 0.0
        failure_rate = (
            (failure_count + missed_count) / total_attempts
            if total_attempts
            else 1.0
        )

        ending_capital = (
            safe_float(equity_curve[-1]["capital_usd"])
            if equity_curve
            else self.config.initial_capital_usd
        )
        maximum_drawdown = max(
            (safe_float(row["drawdown_percent"]) for row in equity_curve),
            default=0.0,
        )
        median_profit = percentile(realized, 0.50)

        gates = self._build_gates(
            total_attempts=total_attempts,
            confirmed=confirmed_count,
            confirmation_rate=confirmation_rate,
            stale_rate=stale_rate,
            failure_rate=failure_rate,
            median_profit=median_profit,
            maximum_drawdown=maximum_drawdown,
            source_trade_count=len(source_trades),
            walk_forward=walk_forward,
            robustness=robustness,
            promotion=promotion,
        )

        operational_checks = {
            "SHADOW_ATTEMPTS",
            "CONFIRMED_EXECUTIONS",
            "CONFIRMATION_RATE",
            "STALE_QUOTE_RATE",
            "TOTAL_FAILURE_RATE",
            "MEDIAN_REALIZED_PROFIT",
            "MAXIMUM_DRAWDOWN",
        }
        operational_passed = all(
            gate.passed
            for gate in gates
            if gate.name in operational_checks
        )
        all_blocking_passed = all(
            gate.passed for gate in gates if gate.blocking
        )

        blocking_reasons = [
            gate.message
            for gate in gates
            if gate.blocking and not gate.passed
        ]

        summary = {
            "generated_at": utc_now(),
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "operating_mode": OPERATING_MODE,
            "source_trades": len(source_trades),
            "attempts_per_trade": self.config.attempts_per_trade,
            "shadow_attempts": total_attempts,
            "confirmed_executions": confirmed_count,
            "failed_executions": failure_count,
            "missed_executions": missed_count,
            "stale_quote_attempts": stale_count,
            "confirmation_rate": confirmation_rate,
            "stale_quote_rate": stale_rate,
            "total_failure_rate": failure_rate,
            "median_realized_profit_usd": median_profit,
            "mean_realized_profit_usd": (
                statistics.fmean(realized) if realized else 0.0
            ),
            "fifth_percentile_realized_profit_usd": percentile(realized, 0.05),
            "ninety_fifth_percentile_realized_profit_usd": percentile(
                realized, 0.95
            ),
            "total_realized_profit_usd": sum(realized),
            "profitable_confirmations": sum(value > 0 for value in realized),
            "losing_confirmations": sum(value < 0 for value in realized),
            "initial_capital_usd": self.config.initial_capital_usd,
            "ending_capital_usd": ending_capital,
            "maximum_drawdown_percent": maximum_drawdown,
            "operational_gate_passed": operational_passed,
            "all_blocking_gates_passed": all_blocking_passed,
            "shadow_promotion_allowed": all_blocking_passed,
            "final_decision": (
                "ELIGIBLE_FOR_PHASE_14B_REVIEW"
                if all_blocking_passed
                else "BLOCK_LIVE_EXECUTION"
            ),
            "blocking_reasons": blocking_reasons,
            "upstream": {
                "walk_forward_promotion_allowed": safe_bool(
                    walk_forward.get("summary", {}).get("promotion_allowed")
                ),
                "robustness_passed": safe_bool(
                    robustness.get("summary", {}).get("robustness_passed")
                ),
                "institutional_live_readiness_allowed": safe_bool(
                    promotion.get("live_readiness_allowed")
                ),
                "institutional_final_decision": promotion.get(
                    "final_decision", "UNKNOWN"
                ),
            },
            "safety": {
                "live_execution_enabled": False,
                "wallet_connection_authorized": False,
                "transaction_signing_enabled": False,
                "transaction_broadcasting_enabled": False,
                "automatic_promotion_enabled": False,
            },
            "valid": True,
        }

        if self.config.persist_database:
            self._persist_database(
                summary=summary,
                gates=gates,
                attempts=attempts,
                started_at=started_at,
            )

        return summary, trade_summaries, attempts, equity_curve, gates

    def _simulate_attempt(
        self,
        *,
        run_id: str,
        trade: Mapping[str, Any],
        trade_index: int,
        attempt_number: int,
        rng: random.Random,
    ) -> dict[str, Any]:
        quoted_profit = safe_float(trade.get("net_profit_usd"))
        amount = inferred_trade_amount(trade)
        source_quote_latency = max(
            0.0,
            safe_float(trade.get("quote_latency_ms")),
        )

        quote_to_build = rng.uniform(
            self.config.quote_to_build_min_ms,
            self.config.quote_to_build_max_ms,
        )
        transaction_build = rng.uniform(
            self.config.transaction_build_min_ms,
            self.config.transaction_build_max_ms,
        )
        rpc_submission = rng.uniform(
            self.config.rpc_submission_min_ms,
            self.config.rpc_submission_max_ms,
        )
        confirmation_latency = rng.uniform(
            self.config.confirmation_min_ms,
            self.config.confirmation_max_ms,
        )

        total_quote_age = (
            source_quote_latency
            + quote_to_build
            + transaction_build
            + rpc_submission
        )
        quote_stale = total_quote_age > self.config.maximum_quote_age_ms

        priority_fee = rng.uniform(
            self.config.priority_fee_min_usd,
            self.config.priority_fee_max_usd,
        )
        total_network_fee = self.config.base_network_fee_usd + priority_fee

        slippage_bps = rng.uniform(
            self.config.additional_slippage_min_bps,
            self.config.additional_slippage_max_bps,
        )
        adverse_move_bps = rng.uniform(
            self.config.adverse_move_min_bps,
            self.config.adverse_move_max_bps,
        )

        missed = rng.random() < self.config.miss_probability
        simulation_failed = (
            rng.random() < self.config.simulation_failure_probability
        )
        rpc_failed = rng.random() < self.config.rpc_failure_probability
        blockhash_expired = (
            rng.random() < self.config.blockhash_expiry_probability
        )

        confirmation_probability = self.config.base_confirmation_probability
        if quote_stale:
            confirmation_probability *= self.config.stale_confirmation_multiplier

        random_confirmation_failed = (
            rng.random() >= confirmation_probability
        )

        confirmed = not any(
            (
                missed,
                simulation_failed,
                rpc_failed,
                blockhash_expired,
                random_confirmation_failed,
            )
        )

        execution_penalty_usd = (
            amount * (slippage_bps + adverse_move_bps) / 10_000.0
        )

        realized_profit = (
            quoted_profit - execution_penalty_usd - total_network_fee
            if confirmed
            else 0.0
        )

        if missed:
            status = "MISSED"
            reason = "OPPORTUNITY_MISSED"
        elif simulation_failed:
            status = "FAILED"
            reason = "SIMULATION_FAILURE"
        elif rpc_failed:
            status = "FAILED"
            reason = "RPC_FAILURE"
        elif blockhash_expired:
            status = "FAILED"
            reason = "BLOCKHASH_EXPIRED"
        elif random_confirmation_failed:
            status = "FAILED"
            reason = (
                "STALE_QUOTE_REJECTION"
                if quote_stale
                else "CONFIRMATION_FAILURE"
            )
        else:
            status = "CONFIRMED"
            reason = None

        return {
            "run_id": run_id,
            "attempt_id": f"{run_id}:{trade_index}:{attempt_number}",
            "trade_index": trade_index,
            "attempt_number": attempt_number,
            "event_id": trade.get("event_id"),
            "cycle_id": trade.get("cycle_id"),
            "cycle_number": safe_int(trade.get("cycle_number")),
            "timestamp": trade.get("timestamp"),
            "token": trade.get("token"),
            "asset_key": trade.get("asset_key"),
            "source_type": trade.get("source_type"),
            "validation_status": trade.get("validation_status"),
            "rule": trade.get("rule"),
            "quoted_net_profit_usd": quoted_profit,
            "starting_amount_usd": amount,
            "source_quote_latency_ms": source_quote_latency,
            "quote_to_build_latency_ms": quote_to_build,
            "transaction_build_latency_ms": transaction_build,
            "rpc_submission_latency_ms": rpc_submission,
            "confirmation_latency_ms": confirmation_latency,
            "total_quote_age_ms": total_quote_age,
            "maximum_quote_age_ms": self.config.maximum_quote_age_ms,
            "quote_stale": quote_stale,
            "base_network_fee_usd": self.config.base_network_fee_usd,
            "priority_fee_usd": priority_fee,
            "total_network_fee_usd": total_network_fee,
            "additional_slippage_bps": slippage_bps,
            "adverse_price_move_bps": adverse_move_bps,
            "execution_penalty_usd": execution_penalty_usd,
            "confirmation_probability": confirmation_probability,
            "missed": missed,
            "simulation_failed": simulation_failed,
            "rpc_failed": rpc_failed,
            "blockhash_expired": blockhash_expired,
            "confirmed": confirmed,
            "status": status,
            "failure_reason": reason,
            "realized_net_profit_usd": realized_profit,
            "profit_degradation_usd": (
                quoted_profit - realized_profit
                if confirmed
                else quoted_profit
            ),
            "created_at": utc_now(),
        }

    @staticmethod
    def _summarize_trade(
        trade: Mapping[str, Any],
        trade_index: int,
        attempts: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        confirmed = [row for row in attempts if safe_bool(row["confirmed"])]
        realized = [
            safe_float(row["realized_net_profit_usd"])
            for row in confirmed
        ]

        return {
            "trade_index": trade_index,
            "event_id": trade.get("event_id"),
            "cycle_id": trade.get("cycle_id"),
            "cycle_number": safe_int(trade.get("cycle_number")),
            "token": trade.get("token"),
            "asset_key": trade.get("asset_key"),
            "source_type": trade.get("source_type"),
            "validation_status": trade.get("validation_status"),
            "rule": trade.get("rule"),
            "quoted_net_profit_usd": safe_float(trade.get("net_profit_usd")),
            "attempts": len(attempts),
            "confirmed": len(confirmed),
            "failed": sum(row["status"] == "FAILED" for row in attempts),
            "missed": sum(row["status"] == "MISSED" for row in attempts),
            "confirmation_rate": (
                len(confirmed) / len(attempts) if attempts else 0.0
            ),
            "stale_quote_rate": (
                sum(safe_bool(row["quote_stale"]) for row in attempts)
                / len(attempts)
                if attempts
                else 0.0
            ),
            "median_realized_profit_usd": percentile(realized, 0.50),
            "fifth_percentile_realized_profit_usd": percentile(realized, 0.05),
            "ninety_fifth_percentile_realized_profit_usd": percentile(
                realized, 0.95
            ),
            "mean_realized_profit_usd": (
                statistics.fmean(realized) if realized else 0.0
            ),
            "profitable_confirmation_rate": (
                sum(value > 0 for value in realized) / len(realized)
                if realized
                else 0.0
            ),
        }

    def _build_equity_curve(
        self,
        run_id: str,
        confirmed: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        capital = self.config.initial_capital_usd
        peak = capital
        rows: list[dict[str, Any]] = []

        for index, attempt in enumerate(confirmed, start=1):
            profit = safe_float(attempt["realized_net_profit_usd"])
            capital += profit
            peak = max(peak, capital)
            drawdown = (
                (peak - capital) / peak * 100.0
                if peak > 0
                else 0.0
            )
            rows.append(
                {
                    "run_id": run_id,
                    "execution_index": index,
                    "attempt_id": attempt["attempt_id"],
                    "event_id": attempt.get("event_id"),
                    "cycle_id": attempt.get("cycle_id"),
                    "token": attempt.get("token"),
                    "profit_usd": profit,
                    "capital_usd": capital,
                    "peak_capital_usd": peak,
                    "drawdown_percent": drawdown,
                }
            )

        return rows

    def _build_gates(
        self,
        *,
        total_attempts: int,
        confirmed: int,
        confirmation_rate: float,
        stale_rate: float,
        failure_rate: float,
        median_profit: float,
        maximum_drawdown: float,
        source_trade_count: int,
        walk_forward: Mapping[str, Any],
        robustness: Mapping[str, Any],
        promotion: Mapping[str, Any],
    ) -> list[GateCheck]:
        walk_summary = walk_forward.get("summary", {})
        robustness_summary = robustness.get("summary", {})

        return [
            GateCheck(
                "SHADOW_ATTEMPTS",
                total_attempts >= self.config.minimum_attempts,
                True,
                float(total_attempts),
                ">=",
                float(self.config.minimum_attempts),
                "Enough shadow attempts must complete.",
            ),
            GateCheck(
                "CONFIRMED_EXECUTIONS",
                confirmed >= self.config.minimum_confirmed,
                True,
                float(confirmed),
                ">=",
                float(self.config.minimum_confirmed),
                "Enough shadow executions must confirm.",
            ),
            GateCheck(
                "CONFIRMATION_RATE",
                confirmation_rate >= self.config.minimum_confirmation_rate,
                True,
                confirmation_rate,
                ">=",
                self.config.minimum_confirmation_rate,
                "Shadow confirmation rate is below the operational gate.",
            ),
            GateCheck(
                "STALE_QUOTE_RATE",
                stale_rate <= self.config.maximum_stale_rate,
                True,
                stale_rate,
                "<=",
                self.config.maximum_stale_rate,
                "Stale-quote exposure exceeds the operational gate.",
            ),
            GateCheck(
                "TOTAL_FAILURE_RATE",
                failure_rate <= self.config.maximum_failure_rate,
                True,
                failure_rate,
                "<=",
                self.config.maximum_failure_rate,
                "Combined failed and missed execution rate is too high.",
            ),
            GateCheck(
                "MEDIAN_REALIZED_PROFIT",
                median_profit
                >= self.config.minimum_median_realized_profit_usd,
                True,
                median_profit,
                ">=",
                self.config.minimum_median_realized_profit_usd,
                "Median shadow realized profit is negative.",
            ),
            GateCheck(
                "MAXIMUM_DRAWDOWN",
                maximum_drawdown
                <= self.config.maximum_drawdown_percent,
                True,
                maximum_drawdown,
                "<=",
                self.config.maximum_drawdown_percent,
                "Shadow equity drawdown exceeds the gate.",
            ),
            GateCheck(
                "SOURCE_TRADE_COUNT",
                source_trade_count
                >= self.config.minimum_source_trades_for_promotion,
                True,
                float(source_trade_count),
                ">=",
                float(self.config.minimum_source_trades_for_promotion),
                "Too few independent out-of-sample source trades.",
            ),
            GateCheck(
                "PHASE_13D_PROMOTION",
                safe_bool(walk_summary.get("promotion_allowed")),
                True,
                float(safe_bool(walk_summary.get("promotion_allowed"))),
                "==",
                1.0,
                "Phase 13D walk-forward promotion remains blocked.",
            ),
            GateCheck(
                "PHASE_13E_ROBUSTNESS",
                safe_bool(robustness_summary.get("robustness_passed")),
                True,
                float(safe_bool(robustness_summary.get("robustness_passed"))),
                "==",
                1.0,
                "Phase 13E robustness validation has not passed.",
            ),
            GateCheck(
                "PHASE_13F_LIVE_READINESS",
                safe_bool(promotion.get("live_readiness_allowed")),
                True,
                float(safe_bool(promotion.get("live_readiness_allowed"))),
                "==",
                1.0,
                "Phase 13F institutional live readiness remains blocked.",
            ),
        ]

    def _persist_database(
        self,
        *,
        summary: Mapping[str, Any],
        gates: Sequence[GateCheck],
        attempts: Sequence[Mapping[str, Any]],
        started_at: str,
    ) -> None:
        path = self.config.database_path
        path.parent.mkdir(parents=True, exist_ok=True)

        connection = sqlite3.connect(path, timeout=30.0)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA busy_timeout=30000")

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS shadow_execution_runs (
                    run_id TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    operating_mode TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    source_trades INTEGER NOT NULL,
                    shadow_attempts INTEGER NOT NULL,
                    confirmed_executions INTEGER NOT NULL,
                    failed_executions INTEGER NOT NULL,
                    missed_executions INTEGER NOT NULL,
                    confirmation_rate REAL NOT NULL,
                    stale_quote_rate REAL NOT NULL,
                    total_failure_rate REAL NOT NULL,
                    median_realized_profit_usd REAL NOT NULL,
                    total_realized_profit_usd REAL NOT NULL,
                    maximum_drawdown_percent REAL NOT NULL,
                    operational_gate_passed INTEGER NOT NULL,
                    shadow_promotion_allowed INTEGER NOT NULL,
                    final_decision TEXT NOT NULL,
                    result_json TEXT NOT NULL
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS shadow_execution_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL UNIQUE,
                    trade_index INTEGER NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    event_id TEXT,
                    cycle_id TEXT,
                    cycle_number INTEGER,
                    token TEXT,
                    asset_key TEXT,
                    status TEXT NOT NULL,
                    failure_reason TEXT,
                    quote_stale INTEGER NOT NULL,
                    confirmed INTEGER NOT NULL,
                    quoted_net_profit_usd REAL NOT NULL,
                    realized_net_profit_usd REAL NOT NULL,
                    total_quote_age_ms REAL NOT NULL,
                    total_network_fee_usd REAL NOT NULL,
                    additional_slippage_bps REAL NOT NULL,
                    adverse_price_move_bps REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES shadow_execution_runs(run_id)
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_shadow_execution_attempts_run_id
                ON shadow_execution_attempts(run_id)
                """
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO shadow_execution_runs (
                    run_id,
                    schema_version,
                    operating_mode,
                    started_at,
                    completed_at,
                    source_trades,
                    shadow_attempts,
                    confirmed_executions,
                    failed_executions,
                    missed_executions,
                    confirmation_rate,
                    stale_quote_rate,
                    total_failure_rate,
                    median_realized_profit_usd,
                    total_realized_profit_usd,
                    maximum_drawdown_percent,
                    operational_gate_passed,
                    shadow_promotion_allowed,
                    final_decision,
                    result_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary["run_id"],
                    SCHEMA_VERSION,
                    OPERATING_MODE,
                    started_at,
                    summary["generated_at"],
                    summary["source_trades"],
                    summary["shadow_attempts"],
                    summary["confirmed_executions"],
                    summary["failed_executions"],
                    summary["missed_executions"],
                    summary["confirmation_rate"],
                    summary["stale_quote_rate"],
                    summary["total_failure_rate"],
                    summary["median_realized_profit_usd"],
                    summary["total_realized_profit_usd"],
                    summary["maximum_drawdown_percent"],
                    int(summary["operational_gate_passed"]),
                    int(summary["shadow_promotion_allowed"]),
                    summary["final_decision"],
                    json.dumps(
                        {
                            "summary": dict(summary),
                            "gates": [gate.to_dict() for gate in gates],
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                ),
            )

            connection.executemany(
                """
                INSERT OR REPLACE INTO shadow_execution_attempts (
                    run_id,
                    attempt_id,
                    trade_index,
                    attempt_number,
                    event_id,
                    cycle_id,
                    cycle_number,
                    token,
                    asset_key,
                    status,
                    failure_reason,
                    quote_stale,
                    confirmed,
                    quoted_net_profit_usd,
                    realized_net_profit_usd,
                    total_quote_age_ms,
                    total_network_fee_usd,
                    additional_slippage_bps,
                    adverse_price_move_bps,
                    created_at,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["run_id"],
                        row["attempt_id"],
                        row["trade_index"],
                        row["attempt_number"],
                        row.get("event_id"),
                        row.get("cycle_id"),
                        row.get("cycle_number"),
                        row.get("token"),
                        row.get("asset_key"),
                        row["status"],
                        row.get("failure_reason"),
                        int(safe_bool(row["quote_stale"])),
                        int(safe_bool(row["confirmed"])),
                        row["quoted_net_profit_usd"],
                        row["realized_net_profit_usd"],
                        row["total_quote_age_ms"],
                        row["total_network_fee_usd"],
                        row["additional_slippage_bps"],
                        row["adverse_price_move_bps"],
                        row["created_at"],
                        json.dumps(dict(row), ensure_ascii=False, default=str),
                    )
                    for row in attempts
                ],
            )

            connection.commit()

        except sqlite3.Error as error:
            connection.rollback()
            raise ShadowExecutionError(
                f"Could not persist shadow execution audit: {error}"
            ) from error
        finally:
            connection.close()


def export_results(
    *,
    summary: Mapping[str, Any],
    trade_summaries: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
    equity_curve: Sequence[Mapping[str, Any]],
    gates: Sequence[GateCheck],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(parents=True, exist_ok=True)

    paths = {
        "trades": output / OUTPUT_TRADES,
        "attempts": output / OUTPUT_ATTEMPTS,
        "equity": output / OUTPUT_EQUITY,
        "gates": output / OUTPUT_GATES,
        "report": output / OUTPUT_REPORT,
        "manifest": output / OUTPUT_MANIFEST,
    }

    if not configuration.overwrite:
        existing = [path for path in paths.values() if path.exists()]
        if existing:
            raise ShadowExecutionError(
                "Refusing to overwrite: "
                + ", ".join(str(path) for path in existing)
            )

    write_csv(paths["trades"], trade_summaries)
    write_csv(paths["attempts"], attempts)
    write_csv(paths["equity"], equity_curve)
    write_csv(paths["gates"], [gate.to_dict() for gate in gates])

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(configuration),
                    "trades_csv": str(configuration.trades_csv),
                    "walk_forward_report": str(
                        configuration.walk_forward_report
                    ),
                    "robustness_report": str(configuration.robustness_report),
                    "promotion_decision": str(
                        configuration.promotion_decision
                    ),
                    "database_path": str(configuration.database_path),
                    "output_directory": str(configuration.output_directory),
                },
                "gate_checks": [gate.to_dict() for gate in gates],
                "governance": {
                    "live_execution_enabled": False,
                    "wallet_connection_authorized": False,
                    "transaction_signing_enabled": False,
                    "transaction_broadcasting_enabled": False,
                    "automatic_promotion_enabled": False,
                    "database_writes_are_audit_only": (
                        configuration.persist_database
                    ),
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    files: dict[str, Any] = {}
    row_counts = {
        "trades": len(trade_summaries),
        "attempts": len(attempts),
        "equity": len(equity_curve),
        "gates": len(gates),
        "report": None,
    }

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
                "run_id": summary["run_id"],
                "summary": dict(summary),
                "inputs": {
                    "walk_forward_trades": {
                        "path": str(configuration.trades_csv),
                        "sha256": sha256_file(configuration.trades_csv),
                    },
                    "walk_forward_report": {
                        "path": str(configuration.walk_forward_report),
                        "sha256": sha256_file(
                            configuration.walk_forward_report
                        ),
                    },
                    "robustness_report": {
                        "path": str(configuration.robustness_report),
                        "sha256": sha256_file(
                            configuration.robustness_report
                        ),
                    },
                    "promotion_decision": {
                        "path": str(configuration.promotion_decision),
                        "sha256": sha256_file(
                            configuration.promotion_decision
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
        description="Run Phase 14A production shadow execution."
    )
    parser.add_argument("--trades-csv", default=str(DEFAULT_TRADES_CSV))
    parser.add_argument(
        "--walk-forward-report",
        default=str(DEFAULT_WALK_FORWARD_REPORT),
    )
    parser.add_argument(
        "--robustness-report",
        default=str(DEFAULT_ROBUSTNESS_REPORT),
    )
    parser.add_argument(
        "--promotion-decision",
        default=str(DEFAULT_PROMOTION_DECISION),
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE))
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    parser.add_argument("--attempts-per-trade", type=int, default=100)
    parser.add_argument("--initial-capital", type=float, default=1_000.0)
    parser.add_argument("--seed", type=int, default=14_002)
    parser.add_argument("--maximum-quote-age-ms", type=float, default=1_500.0)
    parser.add_argument("--no-database-write", action="store_true")
    parser.add_argument("--no-overwrite", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        ),
    )

    configuration = Configuration(
        trades_csv=Path(args.trades_csv),
        walk_forward_report=Path(args.walk_forward_report),
        robustness_report=Path(args.robustness_report),
        promotion_decision=Path(args.promotion_decision),
        database_path=Path(args.database),
        output_directory=Path(args.output_directory),
        overwrite=not args.no_overwrite,
        persist_database=not args.no_database_write,
        random_seed=args.seed,
        attempts_per_trade=args.attempts_per_trade,
        initial_capital_usd=args.initial_capital,
        maximum_quote_age_ms=args.maximum_quote_age_ms,
    )

    try:
        (
            summary,
            trade_summaries,
            attempts,
            equity_curve,
            gates,
        ) = ShadowExecutionEngine(configuration).run()

        output_paths = export_results(
            summary=summary,
            trade_summaries=trade_summaries,
            attempts=attempts,
            equity_curve=equity_curve,
            gates=gates,
            configuration=configuration,
        )

    except (
        ShadowExecutionError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nPhase 14A — Production Shadow Execution Engine")
    print("=" * 80)
    print(f"Run ID: {summary['run_id']}")
    print(f"Operating mode: {summary['operating_mode']}")
    print()

    print("Execution Evidence")
    print("-" * 80)
    print(
        "Source trades / attempts per trade: "
        f"{summary['source_trades']} / {summary['attempts_per_trade']}"
    )
    print(f"Shadow attempts: {summary['shadow_attempts']}")
    print(
        "Confirmed / failed / missed: "
        f"{summary['confirmed_executions']} / "
        f"{summary['failed_executions']} / "
        f"{summary['missed_executions']}"
    )
    print(f"Confirmation rate: {summary['confirmation_rate'] * 100:.2f}%")
    print(f"Stale quote rate: {summary['stale_quote_rate'] * 100:.2f}%")
    print(f"Total failure rate: {summary['total_failure_rate'] * 100:.2f}%")
    print(
        "Median / mean realized profit: "
        f"${summary['median_realized_profit_usd']:.6f} / "
        f"${summary['mean_realized_profit_usd']:.6f}"
    )
    print(
        "5th / 95th percentile realized profit: "
        f"${summary['fifth_percentile_realized_profit_usd']:.6f} / "
        f"${summary['ninety_fifth_percentile_realized_profit_usd']:.6f}"
    )
    print(f"Total realized profit: ${summary['total_realized_profit_usd']:.6f}")
    print(
        "Initial / ending capital: "
        f"${summary['initial_capital_usd']:.2f} / "
        f"${summary['ending_capital_usd']:.2f}"
    )
    print(
        f"Maximum drawdown: {summary['maximum_drawdown_percent']:.6f}%"
    )
    print()

    print("Gate Checks")
    print("-" * 80)
    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:30} | "
            f"{gate.observed:.6f} "
            f"{gate.comparison} {gate.required:.6f}"
        )
        if not gate.passed:
            print(f"       {gate.message}")

    print()
    print(
        "Operational gate passed: "
        f"{summary['operational_gate_passed']}"
    )
    print(
        "All blocking gates passed: "
        f"{summary['all_blocking_gates_passed']}"
    )
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

    if configuration.persist_database:
        print()
        print(
            "Audit database: "
            f"{configuration.database_path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())