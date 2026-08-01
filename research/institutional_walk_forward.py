"""Phase 13D — Institutional Walk-Forward Research Engine.

Reads the Phase 13C feature store, preserves complete chronological cycle
boundaries, performs deterministic threshold selection on training cycles, and
evaluates the selected rule only on later out-of-sample cycles.

Research only: no wallet, no orders, no automatic promotion.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = "13D.1.0"

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = ROOT / "research" / "institutional_feature_store" / "features.jsonl"
DEFAULT_OUTPUT = ROOT / "research" / "institutional_walk_forward"


class WalkForwardError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Configuration:
    features_jsonl: Path = DEFAULT_INPUT
    output_directory: Path = DEFAULT_OUTPUT
    overwrite: bool = True
    initial_capital: float = 1000.0
    minimum_train_cycles: int = 5
    test_cycles: int = 2
    step_cycles: int = 2
    minimum_train_rows: int = 300
    minimum_test_rows: int = 50
    minimum_net_edge_bps: float = 0.0
    maximum_cost_bps: float = 100.0
    minimum_live_quality: float = 90.0
    required_folds: int = 5
    required_profitable_folds: int = 3
    required_oos_trades: int = 50
    required_oos_profit: float = 0.01
    required_profit_factor: float = 1.10
    maximum_drawdown_percent: float = 10.0

    def validate(self) -> None:
        for name in (
            "minimum_train_cycles", "test_cycles", "step_cycles",
            "minimum_train_rows", "minimum_test_rows", "required_folds",
            "required_profitable_folds", "required_oos_trades",
        ):
            if int(getattr(self, name)) <= 0:
                raise WalkForwardError(f"{name} must be positive.")

        for name in (
            "initial_capital", "minimum_net_edge_bps", "maximum_cost_bps",
            "minimum_live_quality", "required_oos_profit",
            "required_profit_factor", "maximum_drawdown_percent",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise WalkForwardError(f"{name} must be finite.")

        if self.initial_capital <= 0:
            raise WalkForwardError("initial_capital must be positive.")


@dataclass(frozen=True, slots=True)
class Rule:
    feature: str
    operator: str
    threshold: float

    def matches(self, row: Mapping[str, Any]) -> bool:
        value = number(row.get(self.feature))
        return value >= self.threshold if self.operator == ">=" else value <= self.threshold

    def label(self) -> str:
        return f"{self.feature} {self.operator} {self.threshold:.8f}"


@dataclass(frozen=True, slots=True)
class Fold:
    fold_id: int
    train_cycle_start: str
    train_cycle_end: str
    test_cycle_start: str
    test_cycle_end: str
    train_cycles: int
    test_cycles: int
    train_rows: int
    test_rows: int
    rule: str | None
    training_selected: int
    training_profit_usd: float
    test_selected: int
    test_wins: int
    test_losses: int
    test_profit_usd: float
    test_profit_factor: float
    test_drawdown_percent: float
    profitable: bool
    skipped: bool
    skip_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Gate:
    name: str
    passed: bool
    observed: float
    comparison: str
    required: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


FEATURE_RULES: tuple[tuple[str, str], ...] = (
    ("market_score", ">="),
    ("liquidity_score", ">="),
    ("volume_score", ">="),
    ("pair_score", ">="),
    ("intelligence_score", ">="),
    ("ai_priority", ">="),
    ("opportunity_probability", ">="),
    ("combined_confidence", ">="),
    ("prediction_confidence", ">="),
    ("trend_score", ">="),
    ("stability_score", ">="),
    ("prior_asset_win_rate", ">="),
    ("prior_asset_average_net_profit_usd", ">="),
    ("prior_global_win_rate", ">="),
    ("prior_global_average_net_profit_usd", ">="),
    ("rolling_asset_win_rate", ">="),
    ("rolling_asset_average_net_profit_usd", ">="),
    ("rolling_global_win_rate", ">="),
    ("rolling_global_average_net_profit_usd", ">="),
    ("total_cost_bps", "<="),
    ("quote_latency_ms", "<="),
    ("downside_risk", "<="),
)


def number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def truth(value: Any) -> bool:
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


def timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        result = datetime.fromisoformat(raw)
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                result = datetime.strptime(raw, pattern)
                break
            except ValueError:
                continue
        else:
            return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise WalkForwardError(f"Input file not found: {path}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise WalkForwardError(
                    f"Invalid JSONL at {path}:{line_number}: {error}"
                ) from error
            if not isinstance(value, dict):
                raise WalkForwardError(f"Expected object at {path}:{line_number}.")
            rows.append(value)
    rows.sort(
        key=lambda row: (
            timestamp(row.get("event_time"))
            or datetime.max.replace(tzinfo=timezone.utc),
            text(row.get("cycle_id")),
            integer(row.get("cycle_number")),
            text(row.get("institutional_event_id")),
        )
    )
    return rows


def base_candidate(row: Mapping[str, Any], config: Configuration) -> bool:
    if not truth(row.get("label_quote_successful")):
        return False
    if number(row.get("label_net_edge_bps")) < config.minimum_net_edge_bps:
        return False
    if number(row.get("total_cost_bps")) > config.maximum_cost_bps:
        return False
    if text(row.get("source_type")).upper() == "VERIFIED_LIVE":
        if number(row.get("enrichment_quality_score")) < config.minimum_live_quality:
            return False
    return True


def percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    weight = position - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def thresholds(values: Sequence[float]) -> tuple[float, ...]:
    return tuple(
        sorted(
            {
                round(percentile(values, q), 12)
                for q in (0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90)
            }
        )
    )


def selected_rows(
    rows: Sequence[Mapping[str, Any]],
    config: Configuration,
    rule: Rule | None,
) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if base_candidate(row, config)
        and (rule is None or rule.matches(row))
    ]


def performance(
    rows: Sequence[Mapping[str, Any]],
    initial_capital: float,
) -> dict[str, Any]:
    profits = [number(row.get("label_net_profit_usd")) for row in rows]
    wins = [value for value in profits if value > 0]
    losses = [value for value in profits if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (float("inf") if gross_profit > 0 else 0.0)
    )

    capital = initial_capital
    peak = initial_capital
    maximum_drawdown = 0.0
    curve: list[dict[str, Any]] = []

    for index, (row, profit) in enumerate(zip(rows, profits), 1):
        capital += profit
        peak = max(peak, capital)
        drawdown = ((peak - capital) / peak * 100.0) if peak > 0 else 0.0
        maximum_drawdown = max(maximum_drawdown, drawdown)
        curve.append(
            {
                "trade_index": index,
                "event_id": row.get("institutional_event_id"),
                "cycle_id": row.get("cycle_id"),
                "token": row.get("token"),
                "profit_usd": profit,
                "capital_usd": capital,
                "drawdown_percent": drawdown,
            }
        )

    return {
        "trades": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "profit_usd": sum(profits),
        "profit_factor": profit_factor,
        "drawdown_percent": maximum_drawdown,
        "ending_capital": capital,
        "curve": curve,
    }


def rank_performance(summary: Mapping[str, Any]) -> tuple[Any, ...]:
    raw_factor = summary["profit_factor"]

    try:
        factor = float(raw_factor)
    except (TypeError, ValueError):
        factor = 0.0

    if math.isinf(factor) and factor > 0:
        factor = 1e12
    elif not math.isfinite(factor):
        factor = 0.0
    return (
        int(number(summary["profit_usd"]) > 0),
        number(summary["profit_usd"]),
        integer(summary["wins"]),
        factor,
        integer(summary["trades"]),
        -number(summary["drawdown_percent"]),
    )


def choose_rule(
    rows: Sequence[Mapping[str, Any]],
    config: Configuration,
) -> tuple[Rule | None, dict[str, Any]]:
    base = selected_rows(rows, config, None)
    best_rule: Rule | None = None
    best_summary = performance(base, config.initial_capital)
    best_rank = rank_performance(best_summary)

    eligible_training = [row for row in rows if base_candidate(row, config)]

    for feature, operator in FEATURE_RULES:
        values = [number(row.get(feature)) for row in eligible_training]
        for threshold_value in thresholds(values):
            rule = Rule(feature, operator, threshold_value)
            chosen = selected_rows(rows, config, rule)
            if not chosen:
                continue
            summary = performance(chosen, config.initial_capital)
            rank = rank_performance(summary)
            if rank > best_rank:
                best_rule = rule
                best_summary = summary
                best_rank = rank

    return best_rule, best_summary


class Engine:
    def __init__(self, configuration: Configuration) -> None:
        self.config = configuration
        self.config.validate()

    def run(self) -> tuple[
        dict[str, Any],
        list[Fold],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[Gate],
    ]:
        rows = load_rows(self.config.features_jsonl)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(text(row.get("cycle_id"), "UNKNOWN"), []).append(row)

        cycles = sorted(
            grouped,
            key=lambda cycle_id: (
                min(
                    (
                        timestamp(row.get("event_time"))
                        for row in grouped[cycle_id]
                        if timestamp(row.get("event_time")) is not None
                    ),
                    default=datetime.max.replace(tzinfo=timezone.utc),
                ),
                cycle_id,
            ),
        )

        folds: list[Fold] = []
        predictions: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        all_oos: list[Mapping[str, Any]] = []

        start = self.config.minimum_train_cycles
        fold_id = 0

        while start < len(cycles):
            fold_id += 1
            train_ids = cycles[:start]
            test_ids = cycles[start:start + self.config.test_cycles]
            if not test_ids:
                break

            train = [row for cycle in train_ids for row in grouped[cycle]]
            test = [row for cycle in test_ids for row in grouped[cycle]]

            reason: str | None = None
            if len(train) < self.config.minimum_train_rows:
                reason = "Training rows below minimum."
            elif len(test) < self.config.minimum_test_rows:
                reason = "Test rows below minimum."

            if reason:
                folds.append(
                    Fold(
                        fold_id, train_ids[0], train_ids[-1], test_ids[0], test_ids[-1],
                        len(train_ids), len(test_ids), len(train), len(test), None,
                        0, 0.0, 0, 0, 0, 0.0, 0.0, 0.0, False, True, reason,
                    )
                )
                start += self.config.step_cycles
                continue

            rule, train_summary = choose_rule(train, self.config)
            chosen_test = selected_rows(test, self.config, rule)
            test_summary = performance(chosen_test, self.config.initial_capital)

            for row in test:
                is_selected = base_candidate(row, self.config) and (
                    rule is None or rule.matches(row)
                )
                predictions.append(
                    {
                        "fold_id": fold_id,
                        "event_id": row.get("institutional_event_id"),
                        "cycle_id": row.get("cycle_id"),
                        "cycle_number": row.get("cycle_number"),
                        "timestamp": row.get("event_time"),
                        "token": row.get("token"),
                        "source_type": row.get("source_type"),
                        "validation_status": row.get("validation_status"),
                        "selected": is_selected,
                        "rule": rule.label() if rule else "BASE_FILTER_ONLY",
                        "actual_profit_usd": number(row.get("label_net_profit_usd")),
                        "actual_profitable": truth(row.get("label_profitable")),
                    }
                )

            for row in chosen_test:
                trades.append(
                    {
                        "fold_id": fold_id,
                        "event_id": row.get("institutional_event_id"),
                        "cycle_id": row.get("cycle_id"),
                        "cycle_number": row.get("cycle_number"),
                        "timestamp": row.get("event_time"),
                        "token": row.get("token"),
                        "asset_key": row.get("asset_key"),
                        "source_type": row.get("source_type"),
                        "validation_status": row.get("validation_status"),
                        "rule": rule.label() if rule else "BASE_FILTER_ONLY",
                        "net_profit_usd": number(row.get("label_net_profit_usd")),
                        "net_edge_bps": number(row.get("label_net_edge_bps")),
                        "total_cost_bps": number(row.get("total_cost_bps")),
                        "quote_latency_ms": number(row.get("quote_latency_ms")),
                        "profitable": truth(row.get("label_profitable")),
                    }
                )

            all_oos.extend(chosen_test)
            folds.append(
                Fold(
                    fold_id=fold_id,
                    train_cycle_start=train_ids[0],
                    train_cycle_end=train_ids[-1],
                    test_cycle_start=test_ids[0],
                    test_cycle_end=test_ids[-1],
                    train_cycles=len(train_ids),
                    test_cycles=len(test_ids),
                    train_rows=len(train),
                    test_rows=len(test),
                    rule=rule.label() if rule else "BASE_FILTER_ONLY",
                    training_selected=integer(train_summary["trades"]),
                    training_profit_usd=number(train_summary["profit_usd"]),
                    test_selected=integer(test_summary["trades"]),
                    test_wins=integer(test_summary["wins"]),
                    test_losses=integer(test_summary["losses"]),
                    test_profit_usd=number(test_summary["profit_usd"]),
                    test_profit_factor=number(test_summary["profit_factor"]),
                    test_drawdown_percent=number(test_summary["drawdown_percent"]),
                    profitable=number(test_summary["profit_usd"]) > 0,
                    skipped=False,
                    skip_reason=None,
                )
            )

            start += self.config.step_cycles

        completed = [fold for fold in folds if not fold.skipped]
        profitable_folds = sum(fold.profitable for fold in completed)
        total = performance(all_oos, self.config.initial_capital)

        gates = [
            Gate("completed_folds", len(completed) >= self.config.required_folds,
                 float(len(completed)), ">=", float(self.config.required_folds),
                 "Enough chronological folds must complete."),
            Gate("profitable_folds", profitable_folds >= self.config.required_profitable_folds,
                 float(profitable_folds), ">=", float(self.config.required_profitable_folds),
                 "Multiple folds must be profitable."),
            Gate("out_of_sample_trades", integer(total["trades"]) >= self.config.required_oos_trades,
                 float(integer(total["trades"])), ">=", float(self.config.required_oos_trades),
                 "Enough out-of-sample selected trades are required."),
            Gate("out_of_sample_profit", number(total["profit_usd"]) >= self.config.required_oos_profit,
                 number(total["profit_usd"]), ">=", self.config.required_oos_profit,
                 "Out-of-sample profit must be positive."),
            Gate(
                "profit_factor",
                (
                    (
                        math.isinf(float(total["profit_factor"]))
                        and float(total["profit_factor"]) > 0
                    )
                    or float(total["profit_factor"])
                    >= self.config.required_profit_factor
                ),
                (
                    1e12
                    if (
                        math.isinf(float(total["profit_factor"]))
                        and float(total["profit_factor"]) > 0
                    )
                    else float(total["profit_factor"])
                ),
                ">=",
                self.config.required_profit_factor,
                "Profit factor must exceed the promotion threshold.",
            ),
            Gate("maximum_drawdown", number(total["drawdown_percent"]) <= self.config.maximum_drawdown_percent,
                 number(total["drawdown_percent"]), "<=", self.config.maximum_drawdown_percent,
                 "Drawdown must remain controlled."),
        ]

        promotion_allowed = all(gate.passed for gate in gates)
        profitable_rows = sum(truth(row.get("label_profitable")) for row in rows)
        weakness: list[str] = []
        if len(cycles) < 30:
            weakness.append("Fewer than 30 institutional cycles.")
        if profitable_rows < 30:
            weakness.append("Fewer than 30 profitable observations.")
        if integer(total["trades"]) < 30:
            weakness.append("Fewer than 30 out-of-sample selected trades.")
        if not promotion_allowed:
            weakness.append("One or more promotion checks failed.")

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": SCHEMA_VERSION,
            "rows": len(rows),
            "cycles": len(cycles),
            "historical_rows": sum(
                text(row.get("source_type")).upper() == "HISTORICAL" for row in rows
            ),
            "verified_live_rows": sum(
                text(row.get("source_type")).upper() == "VERIFIED_LIVE" for row in rows
            ),
            "folds_attempted": len(folds),
            "folds_completed": len(completed),
            "folds_skipped": len(folds) - len(completed),
            "profitable_folds": profitable_folds,
            "out_of_sample_trades": integer(total["trades"]),
            "out_of_sample_wins": integer(total["wins"]),
            "out_of_sample_losses": integer(total["losses"]),
            "out_of_sample_profit_usd": number(total["profit_usd"]),
            "out_of_sample_return_percent": (
                number(total["profit_usd"]) / self.config.initial_capital * 100.0
            ),
            "out_of_sample_profit_factor": (
                1e12
                if (
                    math.isinf(float(total["profit_factor"]))
                    and float(total["profit_factor"]) > 0
                )
                else float(total["profit_factor"])
            ),
            "out_of_sample_maximum_drawdown_percent": number(
                total["drawdown_percent"]
            ),
            "initial_capital_usd": self.config.initial_capital,
            "ending_capital_usd": number(total["ending_capital"]),
            "promotion_allowed": promotion_allowed,
            "promotion_decision": (
                "ALLOW_RESEARCH_PROMOTION"
                if promotion_allowed
                else "BLOCK_RESEARCH_PROMOTION"
            ),
            "statistically_weak": bool(weakness),
            "weakness_reasons": weakness,
            "valid": True,
        }

        return summary, folds, predictions, trades, list(total["curve"]), gates


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def export(
    config: Configuration,
    summary: Mapping[str, Any],
    folds: Sequence[Fold],
    predictions: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    equity: Sequence[Mapping[str, Any]],
    gates: Sequence[Gate],
) -> tuple[Path, ...]:
    output = config.output_directory
    output.mkdir(parents=True, exist_ok=True)

    paths = {
        "folds": output / "walk_forward_folds.csv",
        "predictions": output / "walk_forward_predictions.csv",
        "trades": output / "walk_forward_trades.csv",
        "equity": output / "walk_forward_equity_curve.csv",
        "report": output / "institutional_walk_forward_report.json",
        "manifest": output / "institutional_walk_forward_manifest.json",
    }

    if not config.overwrite:
        existing = [path for path in paths.values() if path.exists()]
        if existing:
            raise WalkForwardError(
                "Refusing to overwrite: " + ", ".join(str(path) for path in existing)
            )

    write_csv(paths["folds"], [fold.to_dict() for fold in folds])
    write_csv(paths["predictions"], predictions)
    write_csv(paths["trades"], trades)
    write_csv(paths["equity"], equity)

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(config),
                    "features_jsonl": str(config.features_jsonl),
                    "output_directory": str(config.output_directory),
                },
                "folds": [fold.to_dict() for fold in folds],
                "promotion_checks": [gate.to_dict() for gate in gates],
                "governance": {
                    "live_execution_enabled": False,
                    "wallet_connection_authorized": False,
                    "future_data_used": False,
                    "complete_cycle_boundaries_preserved": True,
                    "automatic_promotion_enabled": False,
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    files: dict[str, Any] = {}
    for name, path in paths.items():
        if name == "manifest":
            continue
        files[path.name] = {
            "path": str(path),
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        }

    paths["manifest"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "summary": dict(summary),
                "files": files,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return tuple(paths.values())


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Run the Phase 13D institutional walk-forward engine."
    )
    result.add_argument("--features-jsonl", default=str(DEFAULT_INPUT))
    result.add_argument("--output-directory", default=str(DEFAULT_OUTPUT))
    result.add_argument("--initial-capital", type=float, default=1000.0)
    result.add_argument("--minimum-train-cycles", type=int, default=5)
    result.add_argument("--test-cycles", type=int, default=2)
    result.add_argument("--step-cycles", type=int, default=2)
    result.add_argument("--minimum-train-rows", type=int, default=300)
    result.add_argument("--minimum-test-rows", type=int, default=50)
    result.add_argument("--minimum-net-edge-bps", type=float, default=0.0)
    result.add_argument("--maximum-cost-bps", type=float, default=100.0)
    result.add_argument("--minimum-live-quality", type=float, default=90.0)
    result.add_argument("--no-overwrite", action="store_true")
    result.add_argument("--verbose", action="store_true")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    config = Configuration(
        features_jsonl=Path(args.features_jsonl),
        output_directory=Path(args.output_directory),
        overwrite=not args.no_overwrite,
        initial_capital=args.initial_capital,
        minimum_train_cycles=args.minimum_train_cycles,
        test_cycles=args.test_cycles,
        step_cycles=args.step_cycles,
        minimum_train_rows=args.minimum_train_rows,
        minimum_test_rows=args.minimum_test_rows,
        minimum_net_edge_bps=args.minimum_net_edge_bps,
        maximum_cost_bps=args.maximum_cost_bps,
        minimum_live_quality=args.minimum_live_quality,
    )

    try:
        summary, folds, predictions, trades, equity, gates = Engine(config).run()
        output_paths = export(
            config, summary, folds, predictions, trades, equity, gates
        )
    except (WalkForwardError, OSError, ValueError) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nPhase 13D — Institutional Walk-Forward Research Engine")
    print("=" * 80)
    print(f"Rows / cycles: {summary['rows']} / {summary['cycles']}")
    print(
        "Historical / verified-live rows: "
        f"{summary['historical_rows']} / {summary['verified_live_rows']}"
    )
    print()
    print("Walk-Forward Results")
    print("-" * 80)
    print(
        "Folds attempted / completed / skipped: "
        f"{summary['folds_attempted']} / "
        f"{summary['folds_completed']} / "
        f"{summary['folds_skipped']}"
    )
    print(f"Profitable folds: {summary['profitable_folds']}")
    print(f"Out-of-sample trades: {summary['out_of_sample_trades']}")
    print(
        "Wins / losses: "
        f"{summary['out_of_sample_wins']} / {summary['out_of_sample_losses']}"
    )
    print(
        "Out-of-sample profit: "
        f"${summary['out_of_sample_profit_usd']:.6f}"
    )
    print(f"Profit factor: {summary['out_of_sample_profit_factor']:.4f}")
    print(
        "Maximum drawdown: "
        f"{summary['out_of_sample_maximum_drawdown_percent']:.6f}%"
    )
    print(
        "Initial / ending capital: "
        f"${summary['initial_capital_usd']:.2f} / "
        f"${summary['ending_capital_usd']:.2f}"
    )
    print()
    print("Promotion Checks")
    print("-" * 80)
    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:28} | "
            f"{gate.observed:.6f} {gate.comparison} {gate.required:.6f}"
        )
        print(f"       {gate.message}")
    print()
    print(f"Promotion decision: {summary['promotion_decision']}")
    print(f"Statistically weak: {summary['statistically_weak']}")
    for reason in summary["weakness_reasons"]:
        print(f"  - {reason}")
    print()
    print("Output files")
    print("-" * 80)
    for path in output_paths:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())