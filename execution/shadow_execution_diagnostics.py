"""
Phase 14B — Shadow Execution Diagnostics and Edge Decomposition

Analyzes Phase 14A shadow execution outputs and decomposes quoted edge into:

- network fees
- priority fees
- slippage
- adverse price movement
- quote staleness
- failed execution opportunity cost
- missed opportunity cost
- per-trade and per-token edge degradation
- confirmed execution profitability distribution

Inputs
------
execution/shadow_results/shadow_execution_attempts.csv
execution/shadow_results/shadow_execution_trades.csv
execution/shadow_results/shadow_execution_report.json

Outputs
-------
execution/shadow_diagnostics/
    edge_decomposition.csv
    trade_diagnostics.csv
    token_diagnostics.csv
    failure_diagnostics.csv
    shadow_diagnostics_gate_checks.csv
    shadow_execution_diagnostics_report.json
    shadow_execution_diagnostics_manifest.json

Safety
------
Research and diagnostics only. No wallet, signing, transaction submission,
broadcasting, execution changes, or automatic promotion.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = "14B.1.0"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_ATTEMPTS_CSV = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_attempts.csv"
)
DEFAULT_TRADES_CSV = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_trades.csv"
)
DEFAULT_SHADOW_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_report.json"
)
DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "execution"
    / "shadow_diagnostics"
)

EDGE_OUTPUT = "edge_decomposition.csv"
TRADE_OUTPUT = "trade_diagnostics.csv"
TOKEN_OUTPUT = "token_diagnostics.csv"
FAILURE_OUTPUT = "failure_diagnostics.csv"
GATES_OUTPUT = "shadow_diagnostics_gate_checks.csv"
REPORT_OUTPUT = "shadow_execution_diagnostics_report.json"
MANIFEST_OUTPUT = "shadow_execution_diagnostics_manifest.json"


class ShadowDiagnosticsError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Configuration:
    attempts_csv: Path = DEFAULT_ATTEMPTS_CSV
    trades_csv: Path = DEFAULT_TRADES_CSV
    shadow_report: Path = DEFAULT_SHADOW_REPORT
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    minimum_confirmed_attempts: int = 100
    maximum_median_fee_share_of_edge: float = 1.00
    maximum_median_execution_penalty_share: float = 1.00
    minimum_profitable_confirmation_rate: float = 0.50
    maximum_single_trade_profit_concentration: float = 0.60
    maximum_failure_opportunity_cost_share: float = 0.50

    def validate(self) -> None:
        if self.minimum_confirmed_attempts <= 0:
            raise ShadowDiagnosticsError(
                "minimum_confirmed_attempts must be positive."
            )

        for name in (
            "maximum_median_fee_share_of_edge",
            "maximum_median_execution_penalty_share",
            "minimum_profitable_confirmation_rate",
            "maximum_single_trade_profit_concentration",
            "maximum_failure_opportunity_cost_share",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise ShadowDiagnosticsError(f"{name} must be finite.")


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
        raise ShadowDiagnosticsError(f"Required CSV missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ShadowDiagnosticsError(f"Required JSON missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ShadowDiagnosticsError(f"Expected JSON object: {path}")
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
    if denominator == 0:
        return 0.0
    return numerator / denominator


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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ShadowDiagnosticsEngine:
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
        list[GateCheck],
    ]:
        attempts = load_csv(self.config.attempts_csv)
        trades = load_csv(self.config.trades_csv)
        shadow_report = load_json(self.config.shadow_report)

        if not attempts:
            raise ShadowDiagnosticsError(
                "No shadow execution attempts are available."
            )

        edge_rows = self._edge_decomposition(attempts)
        trade_rows = self._group_diagnostics(edge_rows, key_name="trade_index")
        token_rows = self._group_diagnostics(edge_rows, key_name="token")
        failure_rows = self._failure_diagnostics(attempts)

        confirmed = [row for row in edge_rows if safe_bool(row["confirmed"])]
        realized = [
            safe_float(row["realized_net_profit_usd"])
            for row in confirmed
        ]
        quoted = [
            safe_float(row["quoted_net_profit_usd"])
            for row in confirmed
        ]
        fees = [
            safe_float(row["total_network_fee_usd"])
            for row in confirmed
        ]
        execution_penalties = [
            safe_float(row["execution_penalty_usd"])
            for row in confirmed
        ]
        fee_shares = [
            safe_float(row["fee_share_of_quoted_edge"])
            for row in confirmed
            if abs(safe_float(row["quoted_net_profit_usd"])) > 0
        ]
        penalty_shares = [
            safe_float(row["execution_penalty_share_of_quoted_edge"])
            for row in confirmed
            if abs(safe_float(row["quoted_net_profit_usd"])) > 0
        ]

        profitable_confirmation_rate = safe_ratio(
            sum(value > 0 for value in realized),
            len(realized),
        )

        positive_trade_profit = [
            max(0.0, safe_float(row["realized_profit_total_usd"]))
            for row in trade_rows
        ]
        total_positive_trade_profit = sum(positive_trade_profit)
        max_trade_profit = max(positive_trade_profit, default=0.0)
        single_trade_concentration = safe_ratio(
            max_trade_profit,
            total_positive_trade_profit,
        )

        failed_or_missed = [
            row
            for row in attempts
            if not safe_bool(row.get("confirmed"))
        ]
        failure_opportunity_cost = sum(
            max(0.0, safe_float(row.get("quoted_net_profit_usd")))
            for row in failed_or_missed
        )
        total_positive_quoted_edge = sum(
            max(0.0, safe_float(row.get("quoted_net_profit_usd")))
            for row in attempts
        )
        failure_opportunity_cost_share = safe_ratio(
            failure_opportunity_cost,
            total_positive_quoted_edge,
        )

        median_fee_share = percentile(fee_shares, 0.50)
        median_penalty_share = percentile(penalty_shares, 0.50)

        gates = [
            GateCheck(
                "CONFIRMED_ATTEMPTS",
                len(confirmed) >= self.config.minimum_confirmed_attempts,
                float(len(confirmed)),
                ">=",
                float(self.config.minimum_confirmed_attempts),
                "Enough confirmed attempts are required for diagnostics.",
            ),
            GateCheck(
                "MEDIAN_FEE_SHARE",
                median_fee_share
                <= self.config.maximum_median_fee_share_of_edge,
                median_fee_share,
                "<=",
                self.config.maximum_median_fee_share_of_edge,
                "Median fee share consumes too much quoted edge.",
            ),
            GateCheck(
                "MEDIAN_EXECUTION_PENALTY_SHARE",
                median_penalty_share
                <= self.config.maximum_median_execution_penalty_share,
                median_penalty_share,
                "<=",
                self.config.maximum_median_execution_penalty_share,
                "Median execution penalty consumes too much quoted edge.",
            ),
            GateCheck(
                "PROFITABLE_CONFIRMATION_RATE",
                profitable_confirmation_rate
                >= self.config.minimum_profitable_confirmation_rate,
                profitable_confirmation_rate,
                ">=",
                self.config.minimum_profitable_confirmation_rate,
                "Too few confirmed executions remain profitable.",
            ),
            GateCheck(
                "SINGLE_TRADE_PROFIT_CONCENTRATION",
                single_trade_concentration
                <= self.config.maximum_single_trade_profit_concentration,
                single_trade_concentration,
                "<=",
                self.config.maximum_single_trade_profit_concentration,
                "Shadow profits are too concentrated in one source trade.",
            ),
            GateCheck(
                "FAILURE_OPPORTUNITY_COST_SHARE",
                failure_opportunity_cost_share
                <= self.config.maximum_failure_opportunity_cost_share,
                failure_opportunity_cost_share,
                "<=",
                self.config.maximum_failure_opportunity_cost_share,
                "Failed and missed executions consume too much positive edge.",
            ),
        ]

        diagnostics_passed = all(gate.passed for gate in gates)

        largest_cost_driver = max(
            (
                ("network_fees", sum(fees)),
                ("execution_penalties", sum(execution_penalties)),
                ("failure_opportunity_cost", failure_opportunity_cost),
            ),
            key=lambda item: item[1],
        )

        summary = {
            "generated_at": utc_now(),
            "schema_version": SCHEMA_VERSION,
            "operating_mode": "SHADOW_DIAGNOSTICS_ONLY",
            "attempts": len(attempts),
            "confirmed_attempts": len(confirmed),
            "source_trades": len(trades),
            "unique_tokens": len(
                {text(row.get("token")) for row in attempts if text(row.get("token"))}
            ),
            "quoted_profit_total_usd": sum(quoted),
            "realized_profit_total_usd": sum(realized),
            "profit_degradation_total_usd": sum(
                safe_float(row["profit_degradation_usd"])
                for row in confirmed
            ),
            "network_fee_total_usd": sum(fees),
            "execution_penalty_total_usd": sum(execution_penalties),
            "failure_opportunity_cost_usd": failure_opportunity_cost,
            "failure_opportunity_cost_share": failure_opportunity_cost_share,
            "median_realized_profit_usd": percentile(realized, 0.50),
            "mean_realized_profit_usd": (
                statistics.fmean(realized) if realized else 0.0
            ),
            "profitable_confirmation_rate": profitable_confirmation_rate,
            "median_fee_share_of_quoted_edge": median_fee_share,
            "median_execution_penalty_share_of_quoted_edge": median_penalty_share,
            "single_trade_profit_concentration": single_trade_concentration,
            "largest_cost_driver": largest_cost_driver[0],
            "largest_cost_driver_usd": largest_cost_driver[1],
            "negative_median_profit_confirmed": (
                percentile(realized, 0.50) < 0
            ),
            "diagnostics_passed": diagnostics_passed,
            "final_decision": (
                "EDGE_DECOMPOSITION_ACCEPTABLE"
                if diagnostics_passed
                else "EDGE_DECOMPOSITION_FAILED"
            ),
            "upstream_shadow_decision": shadow_report.get(
                "summary", {}
            ).get("final_decision", "UNKNOWN"),
            "live_execution_enabled": False,
            "wallet_connection_authorized": False,
            "transaction_signing_enabled": False,
            "transaction_broadcasting_enabled": False,
            "valid": True,
        }

        return (
            summary,
            edge_rows,
            trade_rows,
            token_rows,
            failure_rows,
            gates,
        )

    @staticmethod
    def _edge_decomposition(
        attempts: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for attempt in attempts:
            quoted = safe_float(attempt.get("quoted_net_profit_usd"))
            realized = safe_float(attempt.get("realized_net_profit_usd"))
            base_fee = safe_float(attempt.get("base_network_fee_usd"))
            priority_fee = safe_float(attempt.get("priority_fee_usd"))
            total_fee = safe_float(attempt.get("total_network_fee_usd"))
            execution_penalty = safe_float(
                attempt.get("execution_penalty_usd")
            )
            slippage_bps = safe_float(
                attempt.get("additional_slippage_bps")
            )
            adverse_bps = safe_float(
                attempt.get("adverse_price_move_bps")
            )
            amount = safe_float(attempt.get("starting_amount_usd"))

            slippage_cost = amount * slippage_bps / 10_000.0
            adverse_move_cost = amount * adverse_bps / 10_000.0
            degradation = (
                quoted - realized
                if safe_bool(attempt.get("confirmed"))
                else quoted
            )

            denominator = abs(quoted)

            rows.append(
                {
                    **dict(attempt),
                    "base_fee_component_usd": base_fee,
                    "priority_fee_component_usd": priority_fee,
                    "slippage_component_usd": slippage_cost,
                    "adverse_move_component_usd": adverse_move_cost,
                    "execution_penalty_component_usd": execution_penalty,
                    "fee_share_of_quoted_edge": (
                        total_fee / denominator
                        if denominator > 0
                        else 0.0
                    ),
                    "execution_penalty_share_of_quoted_edge": (
                        execution_penalty / denominator
                        if denominator > 0
                        else 0.0
                    ),
                    "total_degradation_share_of_quoted_edge": (
                        degradation / denominator
                        if denominator > 0
                        else 0.0
                    ),
                    "edge_survived": (
                        safe_bool(attempt.get("confirmed"))
                        and realized > 0
                    ),
                }
            )

        return rows

    @staticmethod
    def _group_diagnostics(
        rows: Sequence[Mapping[str, Any]],
        *,
        key_name: str,
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)

        for row in rows:
            key = text(row.get(key_name), "UNKNOWN")
            grouped[key].append(row)

        output: list[dict[str, Any]] = []

        for key, group in grouped.items():
            confirmed = [row for row in group if safe_bool(row["confirmed"])]
            realized = [
                safe_float(row["realized_net_profit_usd"])
                for row in confirmed
            ]

            output.append(
                {
                    key_name: key,
                    "token": text(group[0].get("token")),
                    "event_id": group[0].get("event_id"),
                    "attempts": len(group),
                    "confirmed": len(confirmed),
                    "failed": sum(row["status"] == "FAILED" for row in group),
                    "missed": sum(row["status"] == "MISSED" for row in group),
                    "confirmation_rate": safe_ratio(
                        len(confirmed),
                        len(group),
                    ),
                    "stale_quote_rate": safe_ratio(
                        sum(safe_bool(row["quote_stale"]) for row in group),
                        len(group),
                    ),
                    "quoted_profit_total_usd": sum(
                        safe_float(row["quoted_net_profit_usd"])
                        for row in confirmed
                    ),
                    "realized_profit_total_usd": sum(realized),
                    "median_realized_profit_usd": percentile(realized, 0.50),
                    "mean_realized_profit_usd": (
                        statistics.fmean(realized) if realized else 0.0
                    ),
                    "profitable_confirmation_rate": safe_ratio(
                        sum(value > 0 for value in realized),
                        len(realized),
                    ),
                    "network_fee_total_usd": sum(
                        safe_float(row["total_network_fee_usd"])
                        for row in confirmed
                    ),
                    "execution_penalty_total_usd": sum(
                        safe_float(row["execution_penalty_usd"])
                        for row in confirmed
                    ),
                    "profit_degradation_total_usd": sum(
                        safe_float(row["profit_degradation_usd"])
                        for row in confirmed
                    ),
                }
            )

        output.sort(
            key=lambda row: safe_float(row["realized_profit_total_usd"]),
            reverse=True,
        )
        return output

    @staticmethod
    def _failure_diagnostics(
        attempts: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)

        for row in attempts:
            if safe_bool(row.get("confirmed")):
                continue
            reason = text(row.get("failure_reason"), "UNKNOWN")
            grouped[reason].append(row)

        total_failures = sum(len(group) for group in grouped.values())
        output: list[dict[str, Any]] = []

        for reason, group in sorted(
            grouped.items(),
            key=lambda item: len(item[1]),
            reverse=True,
        ):
            opportunity_cost = sum(
                max(0.0, safe_float(row.get("quoted_net_profit_usd")))
                for row in group
            )
            output.append(
                {
                    "failure_reason": reason,
                    "count": len(group),
                    "share_of_failures": safe_ratio(
                        len(group),
                        total_failures,
                    ),
                    "positive_edge_opportunity_cost_usd": opportunity_cost,
                    "average_quote_age_ms": (
                        statistics.fmean(
                            safe_float(row.get("total_quote_age_ms"))
                            for row in group
                        )
                        if group
                        else 0.0
                    ),
                    "stale_quote_count": sum(
                        safe_bool(row.get("quote_stale"))
                        for row in group
                    ),
                }
            )

        return output


def export_results(
    *,
    summary: Mapping[str, Any],
    edge_rows: Sequence[Mapping[str, Any]],
    trade_rows: Sequence[Mapping[str, Any]],
    token_rows: Sequence[Mapping[str, Any]],
    failure_rows: Sequence[Mapping[str, Any]],
    gates: Sequence[GateCheck],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(parents=True, exist_ok=True)

    paths = {
        "edge": output / EDGE_OUTPUT,
        "trade": output / TRADE_OUTPUT,
        "token": output / TOKEN_OUTPUT,
        "failure": output / FAILURE_OUTPUT,
        "gates": output / GATES_OUTPUT,
        "report": output / REPORT_OUTPUT,
        "manifest": output / MANIFEST_OUTPUT,
    }

    if not configuration.overwrite:
        existing = [path for path in paths.values() if path.exists()]
        if existing:
            raise ShadowDiagnosticsError(
                "Refusing to overwrite: "
                + ", ".join(str(path) for path in existing)
            )

    write_csv(paths["edge"], edge_rows)
    write_csv(paths["trade"], trade_rows)
    write_csv(paths["token"], token_rows)
    write_csv(paths["failure"], failure_rows)
    write_csv(paths["gates"], [gate.to_dict() for gate in gates])

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(configuration),
                    "attempts_csv": str(configuration.attempts_csv),
                    "trades_csv": str(configuration.trades_csv),
                    "shadow_report": str(configuration.shadow_report),
                    "output_directory": str(configuration.output_directory),
                },
                "gate_checks": [gate.to_dict() for gate in gates],
                "governance": {
                    "research_only": True,
                    "live_execution_enabled": False,
                    "wallet_connection_authorized": False,
                    "transaction_signing_enabled": False,
                    "transaction_broadcasting_enabled": False,
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
        "edge": len(edge_rows),
        "trade": len(trade_rows),
        "token": len(token_rows),
        "failure": len(failure_rows),
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
                    "attempts_csv": {
                        "path": str(configuration.attempts_csv),
                        "sha256": sha256_file(configuration.attempts_csv),
                    },
                    "trades_csv": {
                        "path": str(configuration.trades_csv),
                        "sha256": sha256_file(configuration.trades_csv),
                    },
                    "shadow_report": {
                        "path": str(configuration.shadow_report),
                        "sha256": sha256_file(configuration.shadow_report),
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
            "Run Phase 14B shadow execution diagnostics "
            "and edge decomposition."
        )
    )
    parser.add_argument(
        "--attempts-csv",
        default=str(DEFAULT_ATTEMPTS_CSV),
    )
    parser.add_argument(
        "--trades-csv",
        default=str(DEFAULT_TRADES_CSV),
    )
    parser.add_argument(
        "--shadow-report",
        default=str(DEFAULT_SHADOW_REPORT),
    )
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    parser.add_argument("--no-overwrite", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    configuration = Configuration(
        attempts_csv=Path(args.attempts_csv),
        trades_csv=Path(args.trades_csv),
        shadow_report=Path(args.shadow_report),
        output_directory=Path(args.output_directory),
        overwrite=not args.no_overwrite,
    )

    try:
        (
            summary,
            edge_rows,
            trade_rows,
            token_rows,
            failure_rows,
            gates,
        ) = ShadowDiagnosticsEngine(configuration).run()

        output_paths = export_results(
            summary=summary,
            edge_rows=edge_rows,
            trade_rows=trade_rows,
            token_rows=token_rows,
            failure_rows=failure_rows,
            gates=gates,
            configuration=configuration,
        )

    except (
        ShadowDiagnosticsError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print(
        "\nPhase 14B — Shadow Execution Diagnostics "
        "and Edge Decomposition"
    )
    print("=" * 80)
    print(f"Attempts / confirmed: {summary['attempts']} / {summary['confirmed_attempts']}")
    print(f"Source trades / unique tokens: {summary['source_trades']} / {summary['unique_tokens']}")
    print()

    print("Edge Decomposition")
    print("-" * 80)
    print(
        "Quoted / realized profit: "
        f"${summary['quoted_profit_total_usd']:.6f} / "
        f"${summary['realized_profit_total_usd']:.6f}"
    )
    print(
        "Total profit degradation: "
        f"${summary['profit_degradation_total_usd']:.6f}"
    )
    print(
        "Network fees / execution penalties: "
        f"${summary['network_fee_total_usd']:.6f} / "
        f"${summary['execution_penalty_total_usd']:.6f}"
    )
    print(
        "Failure opportunity cost: "
        f"${summary['failure_opportunity_cost_usd']:.6f}"
    )
    print(
        "Median fee share / penalty share: "
        f"{summary['median_fee_share_of_quoted_edge'] * 100:.2f}% / "
        f"{summary['median_execution_penalty_share_of_quoted_edge'] * 100:.2f}%"
    )
    print(
        "Profitable confirmation rate: "
        f"{summary['profitable_confirmation_rate'] * 100:.2f}%"
    )
    print(
        "Single-trade profit concentration: "
        f"{summary['single_trade_profit_concentration'] * 100:.2f}%"
    )
    print(
        "Largest cost driver: "
        f"{summary['largest_cost_driver']} "
        f"(${summary['largest_cost_driver_usd']:.6f})"
    )
    print()

    print("Diagnostic Gates")
    print("-" * 80)
    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:34} | "
            f"{gate.observed:.6f} "
            f"{gate.comparison} {gate.required:.6f}"
        )
        if not gate.passed:
            print(f"       {gate.message}")

    print()
    print(f"Diagnostics passed: {summary['diagnostics_passed']}")
    print(f"Final decision: {summary['final_decision']}")
    print()

    print("Output files")
    print("-" * 80)
    for path in output_paths:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())