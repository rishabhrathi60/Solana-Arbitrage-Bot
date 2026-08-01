"""
Phase 13E — Institutional Monte Carlo and Robustness Validation

Consumes Phase 13D out-of-sample walk-forward trades and applies deterministic
and stochastic stress tests for:

- trade-order uncertainty
- execution-cost inflation
- slippage shocks
- latency penalties
- missed trades
- adverse profit perturbation
- bootstrap resampling
- capital drawdown

Inputs
------
research/institutional_walk_forward/walk_forward_trades.csv
research/institutional_walk_forward/institutional_walk_forward_report.json

Outputs
-------
research/institutional_robustness/
    robustness_scenarios.csv
    monte_carlo_paths.csv
    monte_carlo_terminal_distribution.csv
    robustness_trade_stress.csv
    institutional_robustness_report.json
    institutional_robustness_manifest.json

Research only:
- no wallet connection
- no live orders
- no strategy promotion
- no modification of scanner or execution state
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

SCHEMA_VERSION = "13E.1.0"

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

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "research"
    / "institutional_robustness"
)

SCENARIOS_CSV = "robustness_scenarios.csv"
PATHS_CSV = "monte_carlo_paths.csv"
TERMINAL_CSV = "monte_carlo_terminal_distribution.csv"
TRADE_STRESS_CSV = "robustness_trade_stress.csv"
REPORT_JSON = "institutional_robustness_report.json"
MANIFEST_JSON = "institutional_robustness_manifest.json"


class InstitutionalRobustnessError(RuntimeError):
    """Base exception for Phase 13E failures."""


@dataclass(frozen=True, slots=True)
class RobustnessConfiguration:
    trades_csv: Path = DEFAULT_TRADES_CSV
    walk_forward_report: Path = DEFAULT_WALK_FORWARD_REPORT
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    initial_capital_usd: float = 1_000.0
    monte_carlo_paths: int = 5_000
    random_seed: int = 13_013

    bootstrap_with_replacement: bool = True
    randomize_trade_order: bool = True

    cost_inflation_min: float = 1.00
    cost_inflation_max: float = 2.00
    slippage_shock_bps_min: float = 0.0
    slippage_shock_bps_max: float = 20.0
    latency_penalty_bps_per_second: float = 2.0
    miss_probability: float = 0.10
    adverse_profit_noise_bps: float = 5.0

    required_paths: int = 5_000
    maximum_loss_probability: float = 0.40
    minimum_median_profit_usd: float = 0.01
    minimum_fifth_percentile_profit_usd: float = -1.00
    maximum_95th_percentile_drawdown_percent: float = 10.0
    minimum_profitable_scenarios: int = 3

    def validate(self) -> None:
        integer_fields = (
            "monte_carlo_paths",
            "random_seed",
            "required_paths",
            "minimum_profitable_scenarios",
        )

        for name in integer_fields:
            value = int(getattr(self, name))

            if name == "random_seed":
                if value < 0:
                    raise InstitutionalRobustnessError(
                        "random_seed cannot be negative."
                    )
            elif value <= 0:
                raise InstitutionalRobustnessError(
                    f"{name} must be positive."
                )

        numeric_fields = (
            "initial_capital_usd",
            "cost_inflation_min",
            "cost_inflation_max",
            "slippage_shock_bps_min",
            "slippage_shock_bps_max",
            "latency_penalty_bps_per_second",
            "miss_probability",
            "adverse_profit_noise_bps",
            "maximum_loss_probability",
            "minimum_median_profit_usd",
            "minimum_fifth_percentile_profit_usd",
            "maximum_95th_percentile_drawdown_percent",
        )

        for name in numeric_fields:
            value = float(getattr(self, name))

            if not math.isfinite(value):
                raise InstitutionalRobustnessError(
                    f"{name} must be finite."
                )

        if self.initial_capital_usd <= 0:
            raise InstitutionalRobustnessError(
                "initial_capital_usd must be positive."
            )

        if self.cost_inflation_min <= 0:
            raise InstitutionalRobustnessError(
                "cost_inflation_min must be positive."
            )

        if self.cost_inflation_max < self.cost_inflation_min:
            raise InstitutionalRobustnessError(
                "cost_inflation_max must be >= cost_inflation_min."
            )

        if self.slippage_shock_bps_min < 0:
            raise InstitutionalRobustnessError(
                "slippage_shock_bps_min cannot be negative."
            )

        if self.slippage_shock_bps_max < self.slippage_shock_bps_min:
            raise InstitutionalRobustnessError(
                "slippage_shock_bps_max must be >= slippage_shock_bps_min."
            )

        if self.latency_penalty_bps_per_second < 0:
            raise InstitutionalRobustnessError(
                "latency_penalty_bps_per_second cannot be negative."
            )

        if not 0.0 <= self.miss_probability <= 1.0:
            raise InstitutionalRobustnessError(
                "miss_probability must be in [0, 1]."
            )

        if self.adverse_profit_noise_bps < 0:
            raise InstitutionalRobustnessError(
                "adverse_profit_noise_bps cannot be negative."
            )

        if not 0.0 <= self.maximum_loss_probability <= 1.0:
            raise InstitutionalRobustnessError(
                "maximum_loss_probability must be in [0, 1]."
            )


@dataclass(frozen=True, slots=True)
class Scenario:
    scenario_id: str
    description: str
    cost_multiplier: float
    slippage_shock_bps: float
    latency_multiplier: float
    miss_probability: float
    adverse_profit_noise_bps: float


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    scenario_id: str
    description: str
    trades_available: int
    trades_executed: int
    trades_missed: int
    wins: int
    losses: int
    net_profit_usd: float
    ending_capital_usd: float
    return_percent: float
    profit_factor: float
    maximum_drawdown_percent: float
    profitable: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RobustnessGate:
    name: str
    passed: bool
    observed: float
    comparison: str
    required: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now_text() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


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


def load_csv(
    path: Path,
) -> list[dict[str, Any]]:
    if not path.exists():
        raise InstitutionalRobustnessError(
            f"Input file does not exist: {path}"
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


def load_json(
    path: Path,
) -> dict[str, Any]:
    if not path.exists():
        raise InstitutionalRobustnessError(
            f"Input file does not exist: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise InstitutionalRobustnessError(
            f"Expected JSON object: {path}"
        )

    return payload


def percentile(
    values: Sequence[float],
    q: float,
) -> float:
    if not values:
        return 0.0

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    position = (
        (len(ordered) - 1)
        * q
    )

    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return ordered[lower]

    weight = position - lower

    return (
        ordered[lower]
        * (1.0 - weight)
        + ordered[upper]
        * weight
    )


def finite_profit_factor(
    wins: Sequence[float],
    losses: Sequence[float],
) -> float:
    gross_profit = sum(wins)
    gross_loss = abs(
        sum(losses)
    )

    if gross_loss > 0:
        return gross_profit / gross_loss

    if gross_profit > 0:
        return 1e12

    return 0.0


def calculate_path(
    profits: Sequence[float],
    initial_capital: float,
) -> dict[str, Any]:
    capital = initial_capital
    peak = initial_capital
    maximum_drawdown = 0.0
    curve: list[
        dict[str, Any]
    ] = []

    for index, profit in enumerate(
        profits,
        start=1,
    ):
        capital += profit
        peak = max(
            peak,
            capital,
        )

        drawdown = (
            (peak - capital)
            / peak
            * 100.0
            if peak > 0
            else 0.0
        )

        maximum_drawdown = max(
            maximum_drawdown,
            drawdown,
        )

        curve.append(
            {
                "trade_index": index,
                "profit_usd": profit,
                "capital_usd": capital,
                "drawdown_percent": drawdown,
            }
        )

    wins = [
        value
        for value in profits
        if value > 0
    ]

    losses = [
        value
        for value in profits
        if value < 0
    ]

    return {
        "trades": len(profits),
        "wins": len(wins),
        "losses": len(losses),
        "net_profit_usd": sum(
            profits
        ),
        "ending_capital_usd": capital,
        "return_percent": (
            (
                capital
                - initial_capital
            )
            / initial_capital
            * 100.0
            if initial_capital > 0
            else 0.0
        ),
        "profit_factor": (
            finite_profit_factor(
                wins,
                losses,
            )
        ),
        "maximum_drawdown_percent": (
            maximum_drawdown
        ),
        "curve": curve,
    }


def starting_amount(
    trade: Mapping[str, Any],
) -> float:
    explicit = safe_float(
        trade.get(
            "starting_amount_usd"
        )
    )

    if explicit > 0:
        return explicit

    profit = abs(
        safe_float(
            trade.get(
                "net_profit_usd"
            )
        )
    )

    edge_bps = abs(
        safe_float(
            trade.get(
                "net_edge_bps"
            )
        )
    )

    if edge_bps > 0:
        inferred = (
            profit
            / edge_bps
            * 10_000.0
        )

        if inferred > 0:
            return inferred

    return 1.0


def stressed_profit(
    trade: Mapping[str, Any],
    *,
    cost_multiplier: float,
    slippage_shock_bps: float,
    latency_multiplier: float,
    miss: bool,
    adverse_noise_bps: float,
) -> float:
    if miss:
        return 0.0

    base_net_profit = safe_float(
        trade.get(
            "net_profit_usd"
        )
    )

    amount = starting_amount(
        trade
    )

    base_cost_bps = max(
        0.0,
        safe_float(
            trade.get(
                "total_cost_bps"
            )
        ),
    )

    extra_cost_bps = (
        base_cost_bps
        * max(
            0.0,
            cost_multiplier - 1.0,
        )
    )

    latency_ms = max(
        0.0,
        safe_float(
            trade.get(
                "quote_latency_ms"
            )
        ),
    )

    latency_penalty_bps = (
        latency_ms
        / 1_000.0
        * latency_multiplier
    )

    total_extra_bps = (
        extra_cost_bps
        + max(
            0.0,
            slippage_shock_bps,
        )
        + max(
            0.0,
            latency_penalty_bps,
        )
        + max(
            0.0,
            adverse_noise_bps,
        )
    )

    extra_cost_usd = (
        amount
        * total_extra_bps
        / 10_000.0
    )

    return (
        base_net_profit
        - extra_cost_usd
    )


def deterministic_scenarios() -> tuple[
    Scenario,
    ...,
]:
    return (
        Scenario(
            scenario_id="BASELINE",
            description="Original walk-forward trade outcomes.",
            cost_multiplier=1.0,
            slippage_shock_bps=0.0,
            latency_multiplier=0.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=0.0,
        ),
        Scenario(
            scenario_id="COST_1_25X",
            description="Execution costs inflated by 25%.",
            cost_multiplier=1.25,
            slippage_shock_bps=0.0,
            latency_multiplier=0.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=0.0,
        ),
        Scenario(
            scenario_id="COST_1_50X",
            description="Execution costs inflated by 50%.",
            cost_multiplier=1.50,
            slippage_shock_bps=0.0,
            latency_multiplier=0.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=0.0,
        ),
        Scenario(
            scenario_id="SLIPPAGE_5_BPS",
            description="Additional five basis points of slippage.",
            cost_multiplier=1.0,
            slippage_shock_bps=5.0,
            latency_multiplier=0.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=0.0,
        ),
        Scenario(
            scenario_id="SLIPPAGE_10_BPS",
            description="Additional ten basis points of slippage.",
            cost_multiplier=1.0,
            slippage_shock_bps=10.0,
            latency_multiplier=0.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=0.0,
        ),
        Scenario(
            scenario_id="LATENCY_STRESS",
            description="Two bps per second quote-latency penalty.",
            cost_multiplier=1.0,
            slippage_shock_bps=0.0,
            latency_multiplier=2.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=0.0,
        ),
        Scenario(
            scenario_id="COMBINED_MODERATE",
            description="25% cost inflation, five bps slippage, latency penalty.",
            cost_multiplier=1.25,
            slippage_shock_bps=5.0,
            latency_multiplier=2.0,
            miss_probability=0.0,
            adverse_profit_noise_bps=2.0,
        ),
        Scenario(
            scenario_id="COMBINED_SEVERE",
            description="50% cost inflation, ten bps slippage, stronger latency penalty.",
            cost_multiplier=1.50,
            slippage_shock_bps=10.0,
            latency_multiplier=4.0,
            miss_probability=0.20,
            adverse_profit_noise_bps=5.0,
        ),
    )


class InstitutionalRobustnessEngine:
    def __init__(
        self,
        configuration: RobustnessConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or RobustnessConfiguration()
        )
        self.configuration.validate()

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        tuple[ScenarioResult, ...],
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
        tuple[RobustnessGate, ...],
    ]:
        trades = load_csv(
            self.configuration.trades_csv
        )

        walk_forward_report = load_json(
            self.configuration.walk_forward_report
        )

        if not trades:
            raise InstitutionalRobustnessError(
                "No out-of-sample walk-forward trades are available."
            )

        rng = random.Random(
            self.configuration.random_seed
        )

        scenario_results: list[
            ScenarioResult
        ] = []

        trade_stress_rows: list[
            dict[str, Any]
        ] = []

        for scenario in deterministic_scenarios():
            stressed: list[
                float
            ] = []

            executed = 0
            missed = 0

            for trade_index, trade in enumerate(
                trades,
                start=1,
            ):
                miss = (
                    rng.random()
                    < scenario.miss_probability
                )

                profit = stressed_profit(
                    trade,
                    cost_multiplier=(
                        scenario.cost_multiplier
                    ),
                    slippage_shock_bps=(
                        scenario.slippage_shock_bps
                    ),
                    latency_multiplier=(
                        scenario.latency_multiplier
                    ),
                    miss=miss,
                    adverse_noise_bps=(
                        scenario.adverse_profit_noise_bps
                    ),
                )

                if miss:
                    missed += 1
                else:
                    executed += 1
                    stressed.append(
                        profit
                    )

                trade_stress_rows.append(
                    {
                        "scenario_id": (
                            scenario.scenario_id
                        ),
                        "trade_index": trade_index,
                        "event_id": trade.get(
                            "event_id"
                        ),
                        "cycle_id": trade.get(
                            "cycle_id"
                        ),
                        "token": trade.get(
                            "token"
                        ),
                        "original_profit_usd": (
                            safe_float(
                                trade.get(
                                    "net_profit_usd"
                                )
                            )
                        ),
                        "stressed_profit_usd": (
                            profit
                        ),
                        "missed": miss,
                        "cost_multiplier": (
                            scenario.cost_multiplier
                        ),
                        "slippage_shock_bps": (
                            scenario.slippage_shock_bps
                        ),
                        "latency_multiplier": (
                            scenario.latency_multiplier
                        ),
                        "adverse_profit_noise_bps": (
                            scenario
                            .adverse_profit_noise_bps
                        ),
                    }
                )

            path = calculate_path(
                stressed,
                self.configuration
                .initial_capital_usd,
            )

            scenario_results.append(
                ScenarioResult(
                    scenario_id=(
                        scenario.scenario_id
                    ),
                    description=(
                        scenario.description
                    ),
                    trades_available=len(
                        trades
                    ),
                    trades_executed=executed,
                    trades_missed=missed,
                    wins=safe_int(
                        path["wins"]
                    ),
                    losses=safe_int(
                        path["losses"]
                    ),
                    net_profit_usd=safe_float(
                        path[
                            "net_profit_usd"
                        ]
                    ),
                    ending_capital_usd=safe_float(
                        path[
                            "ending_capital_usd"
                        ]
                    ),
                    return_percent=safe_float(
                        path[
                            "return_percent"
                        ]
                    ),
                    profit_factor=safe_float(
                        path[
                            "profit_factor"
                        ]
                    ),
                    maximum_drawdown_percent=safe_float(
                        path[
                            "maximum_drawdown_percent"
                        ]
                    ),
                    profitable=(
                        safe_float(
                            path[
                                "net_profit_usd"
                            ]
                        ) > 0
                    ),
                )
            )

        (
            path_rows,
            terminal_rows,
        ) = self._run_monte_carlo(
            trades,
            rng,
        )

        ending_capitals = [
            safe_float(
                row[
                    "ending_capital_usd"
                ]
            )
            for row in terminal_rows
        ]

        profits = [
            safe_float(
                row[
                    "net_profit_usd"
                ]
            )
            for row in terminal_rows
        ]

        drawdowns = [
            safe_float(
                row[
                    "maximum_drawdown_percent"
                ]
            )
            for row in terminal_rows
        ]

        loss_probability = (
            sum(
                profit < 0
                for profit in profits
            )
            / len(profits)
            if profits
            else 1.0
        )

        profitable_scenarios = sum(
            scenario.profitable
            for scenario in scenario_results
        )

        gates = (
            RobustnessGate(
                name="monte_carlo_paths",
                passed=(
                    len(terminal_rows)
                    >= self.configuration
                    .required_paths
                ),
                observed=float(
                    len(terminal_rows)
                ),
                comparison=">=",
                required=float(
                    self.configuration
                    .required_paths
                ),
                message=(
                    "Enough Monte Carlo paths "
                    "must complete."
                ),
            ),
            RobustnessGate(
                name="loss_probability",
                passed=(
                    loss_probability
                    <= self.configuration
                    .maximum_loss_probability
                ),
                observed=loss_probability,
                comparison="<=",
                required=(
                    self.configuration
                    .maximum_loss_probability
                ),
                message=(
                    "Loss probability must remain "
                    "below the robustness gate."
                ),
            ),
            RobustnessGate(
                name="median_profit",
                passed=(
                    percentile(
                        profits,
                        0.50,
                    )
                    >= self.configuration
                    .minimum_median_profit_usd
                ),
                observed=percentile(
                    profits,
                    0.50,
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_median_profit_usd
                ),
                message=(
                    "Median stressed profit must "
                    "remain positive."
                ),
            ),
            RobustnessGate(
                name="fifth_percentile_profit",
                passed=(
                    percentile(
                        profits,
                        0.05,
                    )
                    >= self.configuration
                    .minimum_fifth_percentile_profit_usd
                ),
                observed=percentile(
                    profits,
                    0.05,
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_fifth_percentile_profit_usd
                ),
                message=(
                    "Fifth-percentile profit must "
                    "remain above the loss floor."
                ),
            ),
            RobustnessGate(
                name="drawdown_95th_percentile",
                passed=(
                    percentile(
                        drawdowns,
                        0.95,
                    )
                    <= self.configuration
                    .maximum_95th_percentile_drawdown_percent
                ),
                observed=percentile(
                    drawdowns,
                    0.95,
                ),
                comparison="<=",
                required=(
                    self.configuration
                    .maximum_95th_percentile_drawdown_percent
                ),
                message=(
                    "The 95th-percentile drawdown "
                    "must remain controlled."
                ),
            ),
            RobustnessGate(
                name="profitable_scenarios",
                passed=(
                    profitable_scenarios
                    >= self.configuration
                    .minimum_profitable_scenarios
                ),
                observed=float(
                    profitable_scenarios
                ),
                comparison=">=",
                required=float(
                    self.configuration
                    .minimum_profitable_scenarios
                ),
                message=(
                    "Multiple deterministic stress "
                    "scenarios must remain profitable."
                ),
            ),
        )

        robustness_passed = all(
            gate.passed
            for gate in gates
        )

        walk_forward_summary = (
            walk_forward_report.get(
                "summary",
                {},
            )
        )

        reasons: list[
            str
        ] = []

        if len(trades) < 30:
            reasons.append(
                "Fewer than 30 out-of-sample trades."
            )

        if not robustness_passed:
            reasons.append(
                "One or more robustness gates failed."
            )

        if not safe_bool(
            walk_forward_summary.get(
                "promotion_allowed"
            )
        ):
            reasons.append(
                "Phase 13D research promotion remains blocked."
            )

        summary = {
            "generated_at": (
                utc_now_text()
            ),
            "schema_version": (
                SCHEMA_VERSION
            ),
            "input_trades": len(
                trades
            ),
            "monte_carlo_paths": len(
                terminal_rows
            ),
            "deterministic_scenarios": len(
                scenario_results
            ),
            "profitable_scenarios": (
                profitable_scenarios
            ),
            "median_ending_capital_usd": (
                percentile(
                    ending_capitals,
                    0.50,
                )
            ),
            "fifth_percentile_ending_capital_usd": (
                percentile(
                    ending_capitals,
                    0.05,
                )
            ),
            "ninety_fifth_percentile_ending_capital_usd": (
                percentile(
                    ending_capitals,
                    0.95,
                )
            ),
            "median_profit_usd": (
                percentile(
                    profits,
                    0.50,
                )
            ),
            "fifth_percentile_profit_usd": (
                percentile(
                    profits,
                    0.05,
                )
            ),
            "ninety_fifth_percentile_profit_usd": (
                percentile(
                    profits,
                    0.95,
                )
            ),
            "loss_probability": (
                loss_probability
            ),
            "median_drawdown_percent": (
                percentile(
                    drawdowns,
                    0.50,
                )
            ),
            "ninety_fifth_percentile_drawdown_percent": (
                percentile(
                    drawdowns,
                    0.95,
                )
            ),
            "robustness_passed": (
                robustness_passed
            ),
            "promotion_decision": (
                "ALLOW_ROBUSTNESS_PROMOTION"
                if robustness_passed
                and safe_bool(
                    walk_forward_summary.get(
                        "promotion_allowed"
                    )
                )
                else "BLOCK_ROBUSTNESS_PROMOTION"
            ),
            "statistically_weak": bool(
                reasons
            ),
            "weakness_reasons": (
                reasons
            ),
            "live_execution_enabled": (
                False
            ),
            "valid": True,
        }

        return (
            summary,
            tuple(
                scenario_results
            ),
            tuple(
                path_rows
            ),
            tuple(
                terminal_rows
            ),
            tuple(
                trade_stress_rows
            ),
            gates,
        )

    def _run_monte_carlo(
        self,
        trades: Sequence[
            Mapping[str, Any]
        ],
        rng: random.Random,
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        path_rows: list[
            dict[str, Any]
        ] = []

        terminal_rows: list[
            dict[str, Any]
        ] = []

        trade_count = len(
            trades
        )

        for path_id in range(
            1,
            self.configuration
            .monte_carlo_paths
            + 1,
        ):
            if (
                self.configuration
                .bootstrap_with_replacement
            ):
                sampled = [
                    trades[
                        rng.randrange(
                            trade_count
                        )
                    ]
                    for _ in range(
                        trade_count
                    )
                ]
            else:
                sampled = list(
                    trades
                )

            if (
                self.configuration
                .randomize_trade_order
            ):
                rng.shuffle(
                    sampled
                )

            stressed_profits: list[
                float
            ] = []

            path_trade_details: list[
                dict[str, Any]
            ] = []

            for trade_index, trade in enumerate(
                sampled,
                start=1,
            ):
                cost_multiplier = (
                    rng.uniform(
                        self.configuration
                        .cost_inflation_min,
                        self.configuration
                        .cost_inflation_max,
                    )
                )

                slippage_shock = (
                    rng.uniform(
                        self.configuration
                        .slippage_shock_bps_min,
                        self.configuration
                        .slippage_shock_bps_max,
                    )
                )

                miss = (
                    rng.random()
                    < self.configuration
                    .miss_probability
                )

                adverse_noise = max(
                    0.0,
                    rng.gauss(
                        0.0,
                        self.configuration
                        .adverse_profit_noise_bps,
                    ),
                )

                profit = stressed_profit(
                    trade,
                    cost_multiplier=(
                        cost_multiplier
                    ),
                    slippage_shock_bps=(
                        slippage_shock
                    ),
                    latency_multiplier=(
                        self.configuration
                        .latency_penalty_bps_per_second
                    ),
                    miss=miss,
                    adverse_noise_bps=(
                        adverse_noise
                    ),
                )

                if not miss:
                    stressed_profits.append(
                        profit
                    )

                path_trade_details.append(
                    {
                        "trade_index": (
                            trade_index
                        ),
                        "event_id": (
                            trade.get(
                                "event_id"
                            )
                        ),
                        "token": trade.get(
                            "token"
                        ),
                        "profit_usd": (
                            profit
                        ),
                        "missed": miss,
                    }
                )

            path = calculate_path(
                stressed_profits,
                self.configuration
                .initial_capital_usd,
            )

            capital_by_trade = {
                safe_int(
                    row[
                        "trade_index"
                    ]
                ): row
                for row in path[
                    "curve"
                ]
            }

            executed_index = 0

            for detail in path_trade_details:
                if detail["missed"]:
                    path_rows.append(
                        {
                            "path_id": (
                                path_id
                            ),
                            **detail,
                            "capital_usd": None,
                            "drawdown_percent": None,
                        }
                    )
                    continue

                executed_index += 1

                equity = (
                    capital_by_trade[
                        executed_index
                    ]
                )

                path_rows.append(
                    {
                        "path_id": (
                            path_id
                        ),
                        **detail,
                        "capital_usd": (
                            equity[
                                "capital_usd"
                            ]
                        ),
                        "drawdown_percent": (
                            equity[
                                "drawdown_percent"
                            ]
                        ),
                    }
                )

            terminal_rows.append(
                {
                    "path_id": (
                        path_id
                    ),
                    "trades_available": (
                        trade_count
                    ),
                    "trades_executed": (
                        safe_int(
                            path[
                                "trades"
                            ]
                        )
                    ),
                    "wins": safe_int(
                        path["wins"]
                    ),
                    "losses": safe_int(
                        path["losses"]
                    ),
                    "net_profit_usd": (
                        safe_float(
                            path[
                                "net_profit_usd"
                            ]
                        )
                    ),
                    "ending_capital_usd": (
                        safe_float(
                            path[
                                "ending_capital_usd"
                            ]
                        )
                    ),
                    "return_percent": (
                        safe_float(
                            path[
                                "return_percent"
                            ]
                        )
                    ),
                    "profit_factor": (
                        safe_float(
                            path[
                                "profit_factor"
                            ]
                        )
                    ),
                    "maximum_drawdown_percent": (
                        safe_float(
                            path[
                                "maximum_drawdown_percent"
                            ]
                        )
                    ),
                    "finished_below_start": (
                        safe_float(
                            path[
                                "ending_capital_usd"
                            ]
                        )
                        < self.configuration
                        .initial_capital_usd
                    ),
                }
            )

        return (
            path_rows,
            terminal_rows,
        )


def write_csv(
    path: Path,
    rows: Sequence[
        Mapping[str, Any]
    ],
) -> None:
    if not rows:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    fields: list[
        str
    ] = []

    seen: set[
        str
    ] = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(
                    key
                )
                fields.append(
                    key
                )

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
        writer.writerows(
            rows
        )


def sha256_file(
    path: Path,
) -> str:
    digest = hashlib.sha256()

    with path.open(
        "rb",
    ) as handle:
        for chunk in iter(
            lambda: handle.read(
                1024
                * 1024
            ),
            b"",
        ):
            digest.update(
                chunk
            )

    return digest.hexdigest()


def export_results(
    *,
    summary: Mapping[
        str,
        Any,
    ],
    scenarios: Sequence[
        ScenarioResult
    ],
    paths: Sequence[
        Mapping[str, Any]
    ],
    terminals: Sequence[
        Mapping[str, Any]
    ],
    trade_stress: Sequence[
        Mapping[str, Any]
    ],
    gates: Sequence[
        RobustnessGate
    ],
    configuration: RobustnessConfiguration,
) -> tuple[
    Path,
    ...,
]:
    output = (
        configuration
        .output_directory
    )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    scenarios_path = (
        output
        / SCENARIOS_CSV
    )

    paths_path = (
        output
        / PATHS_CSV
    )

    terminal_path = (
        output
        / TERMINAL_CSV
    )

    trade_stress_path = (
        output
        / TRADE_STRESS_CSV
    )

    report_path = (
        output
        / REPORT_JSON
    )

    manifest_path = (
        output
        / MANIFEST_JSON
    )

    destinations = (
        scenarios_path,
        paths_path,
        terminal_path,
        trade_stress_path,
        report_path,
        manifest_path,
    )

    if not configuration.overwrite:
        existing = [
            path
            for path in destinations
            if path.exists()
        ]

        if existing:
            raise InstitutionalRobustnessError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    write_csv(
        scenarios_path,
        [
            scenario.to_dict()
            for scenario
            in scenarios
        ],
    )

    write_csv(
        paths_path,
        paths,
    )

    write_csv(
        terminal_path,
        terminals,
    )

    write_csv(
        trade_stress_path,
        trade_stress,
    )

    report_path.write_text(
        json.dumps(
            {
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "summary": dict(
                    summary
                ),
                "configuration": {
                    **asdict(
                        configuration
                    ),
                    "trades_csv": str(
                        configuration
                        .trades_csv
                    ),
                    "walk_forward_report": str(
                        configuration
                        .walk_forward_report
                    ),
                    "output_directory": str(
                        configuration
                        .output_directory
                    ),
                },
                "deterministic_scenarios": [
                    scenario.to_dict()
                    for scenario
                    in scenarios
                ],
                "robustness_gates": [
                    gate.to_dict()
                    for gate in gates
                ],
                "governance": {
                    "live_execution_enabled": False,
                    "wallet_connection_authorized": False,
                    "automatic_promotion_enabled": False,
                    "walk_forward_promotion_required": True,
                    "research_only": True,
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    files: dict[
        str,
        Any,
    ] = {}

    for path, row_count in (
        (
            scenarios_path,
            len(scenarios),
        ),
        (
            paths_path,
            len(paths),
        ),
        (
            terminal_path,
            len(terminals),
        ),
        (
            trade_stress_path,
            len(trade_stress),
        ),
        (
            report_path,
            None,
        ),
    ):
        files[
            path.name
        ] = {
            "path": str(
                path
            ),
            "rows": (
                row_count
            ),
            "bytes": (
                path
                .stat()
                .st_size
            ),
            "sha256": (
                sha256_file(
                    path
                )
            ),
        }

    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "generated_at": (
                    utc_now_text()
                ),
                "summary": dict(
                    summary
                ),
                "files": files,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return destinations


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Run Phase 13E institutional "
            "Monte Carlo and robustness validation."
        )
    )

    result.add_argument(
        "--trades-csv",
        default=str(
            DEFAULT_TRADES_CSV
        ),
    )

    result.add_argument(
        "--walk-forward-report",
        default=str(
            DEFAULT_WALK_FORWARD_REPORT
        ),
    )

    result.add_argument(
        "--output-directory",
        default=str(
            DEFAULT_OUTPUT_DIRECTORY
        ),
    )

    result.add_argument(
        "--initial-capital",
        type=float,
        default=1_000.0,
    )

    result.add_argument(
        "--paths",
        type=int,
        default=5_000,
    )

    result.add_argument(
        "--seed",
        type=int,
        default=13_013,
    )

    result.add_argument(
        "--cost-inflation-min",
        type=float,
        default=1.00,
    )

    result.add_argument(
        "--cost-inflation-max",
        type=float,
        default=2.00,
    )

    result.add_argument(
        "--slippage-min-bps",
        type=float,
        default=0.0,
    )

    result.add_argument(
        "--slippage-max-bps",
        type=float,
        default=20.0,
    )

    result.add_argument(
        "--latency-penalty-bps-per-second",
        type=float,
        default=2.0,
    )

    result.add_argument(
        "--miss-probability",
        type=float,
        default=0.10,
    )

    result.add_argument(
        "--adverse-noise-bps",
        type=float,
        default=5.0,
    )

    result.add_argument(
        "--no-overwrite",
        action="store_true",
    )

    result.add_argument(
        "--verbose",
        action="store_true",
    )

    return result


def main(
    argv: Sequence[
        str
    ]
    | None = None,
) -> int:
    args = parser().parse_args(
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

    configuration = (
        RobustnessConfiguration(
            trades_csv=Path(
                args.trades_csv
            ),
            walk_forward_report=Path(
                args.walk_forward_report
            ),
            output_directory=Path(
                args.output_directory
            ),
            overwrite=(
                not args.no_overwrite
            ),
            initial_capital_usd=(
                args.initial_capital
            ),
            monte_carlo_paths=(
                args.paths
            ),
            random_seed=(
                args.seed
            ),
            cost_inflation_min=(
                args.cost_inflation_min
            ),
            cost_inflation_max=(
                args.cost_inflation_max
            ),
            slippage_shock_bps_min=(
                args.slippage_min_bps
            ),
            slippage_shock_bps_max=(
                args.slippage_max_bps
            ),
            latency_penalty_bps_per_second=(
                args.latency_penalty_bps_per_second
            ),
            miss_probability=(
                args.miss_probability
            ),
            adverse_profit_noise_bps=(
                args.adverse_noise_bps
            ),
        )
    )

    try:
        (
            summary,
            scenarios,
            paths,
            terminals,
            trade_stress,
            gates,
        ) = InstitutionalRobustnessEngine(
            configuration
        ).run()

        output_paths = export_results(
            summary=summary,
            scenarios=scenarios,
            paths=paths,
            terminals=terminals,
            trade_stress=trade_stress,
            gates=gates,
            configuration=configuration,
        )

    except (
        InstitutionalRobustnessError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 13E — Institutional "
        "Monte Carlo and Robustness Validation"
    )

    print("=" * 80)

    print("Evidence")
    print("-" * 80)

    print(
        f"Input out-of-sample trades: "
        f"{summary['input_trades']}"
    )

    print(
        f"Monte Carlo paths: "
        f"{summary['monte_carlo_paths']}"
    )

    print(
        "Deterministic scenarios / profitable: "
        f"{summary['deterministic_scenarios']} / "
        f"{summary['profitable_scenarios']}"
    )

    print()

    print("Monte Carlo Distribution")
    print("-" * 80)

    print(
        "Median ending capital: "
        f"${summary['median_ending_capital_usd']:.6f}"
    )

    print(
        "5th / 95th percentile ending capital: "
        f"${summary['fifth_percentile_ending_capital_usd']:.6f} / "
        f"${summary['ninety_fifth_percentile_ending_capital_usd']:.6f}"
    )

    print(
        "Median profit: "
        f"${summary['median_profit_usd']:.6f}"
    )

    print(
        "5th / 95th percentile profit: "
        f"${summary['fifth_percentile_profit_usd']:.6f} / "
        f"${summary['ninety_fifth_percentile_profit_usd']:.6f}"
    )

    print(
        "Probability of finishing below start: "
        f"{summary['loss_probability'] * 100:.2f}%"
    )

    print(
        "Median / 95th percentile drawdown: "
        f"{summary['median_drawdown_percent']:.6f}% / "
        f"{summary['ninety_fifth_percentile_drawdown_percent']:.6f}%"
    )

    print()

    print("Deterministic Scenarios")
    print("-" * 80)

    for scenario in scenarios:
        print(
            f"{scenario.scenario_id:<20} | "
            f"trades={scenario.trades_executed:<3} | "
            f"net=${scenario.net_profit_usd:+.6f} | "
            f"PF={scenario.profit_factor:.4f} | "
            f"DD={scenario.maximum_drawdown_percent:.6f}% | "
            f"profitable={scenario.profitable}"
        )

    print()

    print("Robustness Gates")
    print("-" * 80)

    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:30} | "
            f"{gate.observed:.6f} "
            f"{gate.comparison} "
            f"{gate.required:.6f}"
        )

        print(
            f"       {gate.message}"
        )

    print()

    print(
        "Robustness passed: "
        f"{summary['robustness_passed']}"
    )

    print(
        "Promotion decision: "
        f"{summary['promotion_decision']}"
    )

    print(
        "Statistically weak: "
        f"{summary['statistically_weak']}"
    )

    for reason in summary[
        "weakness_reasons"
    ]:
        print(
            f"  - {reason}"
        )

    print()

    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(
            path
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )