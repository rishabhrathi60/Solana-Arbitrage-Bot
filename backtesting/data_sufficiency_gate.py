"""
Phase 10C — Step 4: Data Sufficiency Gate

Blocks strategy promotion and live-readiness claims until the historical
research dataset contains enough evidence.

This module is intentionally conservative. It does not modify the scanner,
database, models, strategy parameters, or live execution state.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from backtesting.backtest_engine import (
        BacktestEngineError,
        ConservativeCompositeStrategy,
        EngineConfiguration,
        InstitutionalBacktestEngine,
        RiskLimits,
    )
    from backtesting.event_builder import (
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from backtesting.historical_dataset import DEFAULT_DATABASE_PATH
    from backtesting.strategy_diagnostics import (
        DiagnosticsConfiguration,
        StrategyDiagnosticsEngine,
        StrategyDiagnosticsError,
    )
    from backtesting.walk_forward_validation import (
        MonteCarloConfiguration,
        MonteCarloStressTester,
        ValidationEngineError,
        WalkForwardConfiguration,
        WalkForwardValidator,
        summarize_validation,
    )
except ModuleNotFoundError:
    from backtest_engine import (  # type: ignore
        BacktestEngineError,
        ConservativeCompositeStrategy,
        EngineConfiguration,
        InstitutionalBacktestEngine,
        RiskLimits,
    )
    from event_builder import (  # type: ignore
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from historical_dataset import DEFAULT_DATABASE_PATH  # type: ignore
    from strategy_diagnostics import (  # type: ignore
        DiagnosticsConfiguration,
        StrategyDiagnosticsEngine,
        StrategyDiagnosticsError,
    )
    from walk_forward_validation import (  # type: ignore
        MonteCarloConfiguration,
        MonteCarloStressTester,
        ValidationEngineError,
        WalkForwardConfiguration,
        WalkForwardValidator,
        summarize_validation,
    )


LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIRECTORY = Path("backtesting") / "readiness"
DEFAULT_REPORT_PATH = DEFAULT_OUTPUT_DIRECTORY / "data_sufficiency_report.json"


class DataSufficiencyError(RuntimeError):
    """Base exception for readiness-gate failures."""


class InvalidSufficiencyConfigurationError(DataSufficiencyError):
    """Raised when gate thresholds are invalid."""


@dataclass(frozen=True, slots=True)
class SufficiencyRequirements:
    minimum_scanner_cycles: int = 100
    minimum_total_events: int = 10_000
    minimum_successful_quotes: int = 9_500
    minimum_profitable_observations: int = 100
    minimum_executed_backtest_trades: int = 100
    minimum_walk_forward_folds: int = 5
    minimum_profitable_walk_forward_folds: int = 3
    minimum_total_out_of_sample_trades: int = 50
    minimum_out_of_sample_profit_usd: float = 0.01
    minimum_out_of_sample_profit_factor: float = 1.10
    maximum_out_of_sample_drawdown_pct: float = 10.0
    maximum_monte_carlo_loss_probability: float = 0.40
    minimum_monte_carlo_paths: int = 5_000
    minimum_holdout_cycles: int = 20
    minimum_holdout_events: int = 1_000

    def validate(self) -> None:
        integer_fields = (
            "minimum_scanner_cycles",
            "minimum_total_events",
            "minimum_successful_quotes",
            "minimum_profitable_observations",
            "minimum_executed_backtest_trades",
            "minimum_walk_forward_folds",
            "minimum_profitable_walk_forward_folds",
            "minimum_total_out_of_sample_trades",
            "minimum_monte_carlo_paths",
            "minimum_holdout_cycles",
            "minimum_holdout_events",
        )

        for name in integer_fields:
            value = int(getattr(self, name))
            if value < 0:
                raise InvalidSufficiencyConfigurationError(
                    f"{name} cannot be negative."
                )

        numeric_fields = (
            "minimum_out_of_sample_profit_usd",
            "minimum_out_of_sample_profit_factor",
            "maximum_out_of_sample_drawdown_pct",
            "maximum_monte_carlo_loss_probability",
        )

        for name in numeric_fields:
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise InvalidSufficiencyConfigurationError(
                    f"{name} must be finite."
                )

        if not 0.0 <= self.maximum_monte_carlo_loss_probability <= 1.0:
            raise InvalidSufficiencyConfigurationError(
                "maximum_monte_carlo_loss_probability must be in [0, 1]."
            )


@dataclass(frozen=True, slots=True)
class GateCheck:
    code: str
    category: str
    description: str
    observed: float
    required: float
    comparison: str
    passed: bool
    blocking: bool
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SufficiencyEvidence:
    total_events: int
    scanner_cycles: int
    successful_quotes: int
    profitable_observations: int
    quote_errors: int

    executed_backtest_trades: int
    executed_backtest_wins: int
    executed_backtest_losses: int
    baseline_net_profit_usd: float
    baseline_profit_factor: float
    baseline_maximum_drawdown_pct: float

    walk_forward_folds: int
    profitable_walk_forward_folds: int
    unprofitable_walk_forward_folds: int
    total_out_of_sample_trades: int
    aggregate_out_of_sample_profit_usd: float
    average_out_of_sample_profit_factor: float
    worst_out_of_sample_drawdown_pct: float

    monte_carlo_paths: int
    monte_carlo_probability_below_start: float
    monte_carlo_median_ending_capital_usd: float
    monte_carlo_p05_ending_capital_usd: float
    monte_carlo_p95_drawdown_pct: float

    training_events: int
    training_cycles: int
    holdout_events: int
    holdout_cycles: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SufficiencyReport:
    generated_at: datetime
    database_path: str
    requirements: SufficiencyRequirements
    evidence: SufficiencyEvidence
    checks: tuple[GateCheck, ...]

    checks_passed: int
    checks_failed: int
    blocking_failures: int

    data_sufficient: bool
    strategy_promotion_allowed: bool
    live_readiness_allowed: bool
    operating_mode: str
    final_decision: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(sep=" "),
            "database_path": self.database_path,
            "requirements": asdict(self.requirements),
            "evidence": self.evidence.to_dict(),
            "checks": [check.to_dict() for check in self.checks],
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "blocking_failures": self.blocking_failures,
            "data_sufficient": self.data_sufficient,
            "strategy_promotion_allowed": self.strategy_promotion_allowed,
            "live_readiness_allowed": self.live_readiness_allowed,
            "operating_mode": self.operating_mode,
            "final_decision": self.final_decision,
        }

    def export_json(
        self,
        path: str | Path = DEFAULT_REPORT_PATH,
    ) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(
                self.to_dict(),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return output_path


class DataSufficiencyGate:
    """
    Evaluates research evidence against immutable promotion thresholds.

    Any blocking failure keeps the system in PAPER_RESEARCH mode.
    """

    def __init__(
        self,
        requirements: SufficiencyRequirements | None = None,
    ) -> None:
        self.requirements = requirements or SufficiencyRequirements()
        self.requirements.validate()

    def evaluate(
        self,
        evidence: SufficiencyEvidence,
        *,
        database_path: str | Path,
    ) -> SufficiencyReport:
        checks = self._build_checks(evidence)

        checks_passed = sum(check.passed for check in checks)
        checks_failed = sum(not check.passed for check in checks)
        blocking_failures = sum(
            not check.passed and check.blocking
            for check in checks
        )

        data_sufficient = blocking_failures == 0

        strategy_promotion_allowed = data_sufficient
        live_readiness_allowed = (
            data_sufficient
            and evidence.aggregate_out_of_sample_profit_usd > 0
            and evidence.monte_carlo_probability_below_start
            <= self.requirements.maximum_monte_carlo_loss_probability
        )

        if live_readiness_allowed:
            operating_mode = "PAPER_VALIDATED"
            final_decision = "ELIGIBLE_FOR_SEPARATE_LIVE_READINESS_REVIEW"
        elif strategy_promotion_allowed:
            operating_mode = "PAPER_PROMOTION_ELIGIBLE"
            final_decision = "STRATEGY_PROMOTION_ALLOWED_LIVE_STILL_BLOCKED"
        else:
            operating_mode = "PAPER_RESEARCH"
            final_decision = "BLOCK_STRATEGY_PROMOTION_AND_LIVE_READINESS"

        return SufficiencyReport(
            generated_at=datetime.now(),
            database_path=str(database_path),
            requirements=self.requirements,
            evidence=evidence,
            checks=tuple(checks),
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            blocking_failures=blocking_failures,
            data_sufficient=data_sufficient,
            strategy_promotion_allowed=strategy_promotion_allowed,
            live_readiness_allowed=live_readiness_allowed,
            operating_mode=operating_mode,
            final_decision=final_decision,
        )

    def _build_checks(
        self,
        evidence: SufficiencyEvidence,
    ) -> list[GateCheck]:
        requirements = self.requirements

        return [
            self._minimum_check(
                "CYCLES",
                "DATA_VOLUME",
                "Scanner cycles",
                evidence.scanner_cycles,
                requirements.minimum_scanner_cycles,
                "Collect additional scanner cycles.",
            ),
            self._minimum_check(
                "TOTAL_EVENTS",
                "DATA_VOLUME",
                "Historical observations",
                evidence.total_events,
                requirements.minimum_total_events,
                "Continue paper scanning until the historical event target is met.",
            ),
            self._minimum_check(
                "SUCCESSFUL_QUOTES",
                "DATA_QUALITY",
                "Successful quotes",
                evidence.successful_quotes,
                requirements.minimum_successful_quotes,
                "Improve quote coverage and collect more successful observations.",
            ),
            self._minimum_check(
                "PROFITABLE_OBSERVATIONS",
                "CLASS_BALANCE",
                "Profitable observations",
                evidence.profitable_observations,
                requirements.minimum_profitable_observations,
                "Collect more genuinely profitable observations before training.",
            ),
            self._minimum_check(
                "EXECUTED_TRADES",
                "STRATEGY_SAMPLE",
                "Executed backtest trades",
                evidence.executed_backtest_trades,
                requirements.minimum_executed_backtest_trades,
                "Accumulate more out-of-sample-capable trade examples.",
            ),
            self._minimum_check(
                "WALK_FORWARD_FOLDS",
                "OUT_OF_SAMPLE",
                "Walk-forward folds",
                evidence.walk_forward_folds,
                requirements.minimum_walk_forward_folds,
                "Collect enough cycles to create more chronological folds.",
            ),
            self._minimum_check(
                "PROFITABLE_FOLDS",
                "OUT_OF_SAMPLE",
                "Profitable walk-forward folds",
                evidence.profitable_walk_forward_folds,
                requirements.minimum_profitable_walk_forward_folds,
                "Do not promote until most required folds are profitable.",
            ),
            self._minimum_check(
                "OUT_OF_SAMPLE_TRADES",
                "OUT_OF_SAMPLE",
                "Total out-of-sample trades",
                evidence.total_out_of_sample_trades,
                requirements.minimum_total_out_of_sample_trades,
                "Increase chronological test coverage without reusing holdout data.",
            ),
            self._minimum_check(
                "OUT_OF_SAMPLE_PROFIT",
                "OUT_OF_SAMPLE",
                "Aggregate out-of-sample profit",
                evidence.aggregate_out_of_sample_profit_usd,
                requirements.minimum_out_of_sample_profit_usd,
                "Strategy must show positive fee-adjusted out-of-sample profit.",
            ),
            self._minimum_check(
                "OUT_OF_SAMPLE_PROFIT_FACTOR",
                "OUT_OF_SAMPLE",
                "Average out-of-sample profit factor",
                evidence.average_out_of_sample_profit_factor,
                requirements.minimum_out_of_sample_profit_factor,
                "Improve strategy economics before promotion.",
            ),
            self._maximum_check(
                "OUT_OF_SAMPLE_DRAWDOWN",
                "RISK",
                "Worst out-of-sample drawdown percentage",
                evidence.worst_out_of_sample_drawdown_pct,
                requirements.maximum_out_of_sample_drawdown_pct,
                "Reduce drawdown through stricter strategy and sizing controls.",
            ),
            self._minimum_check(
                "MONTE_CARLO_PATHS",
                "STRESS_TEST",
                "Monte Carlo paths",
                evidence.monte_carlo_paths,
                requirements.minimum_monte_carlo_paths,
                "Run the required number of stress-test paths.",
            ),
            self._maximum_check(
                "MONTE_CARLO_LOSS_PROBABILITY",
                "STRESS_TEST",
                "Probability of finishing below starting capital",
                evidence.monte_carlo_probability_below_start,
                requirements.maximum_monte_carlo_loss_probability,
                "Strategy must survive execution stress with acceptable loss probability.",
            ),
            self._minimum_check(
                "HOLDOUT_CYCLES",
                "HOLDOUT",
                "Chronological holdout cycles",
                evidence.holdout_cycles,
                requirements.minimum_holdout_cycles,
                "Reserve more untouched cycles for final validation.",
            ),
            self._minimum_check(
                "HOLDOUT_EVENTS",
                "HOLDOUT",
                "Chronological holdout events",
                evidence.holdout_events,
                requirements.minimum_holdout_events,
                "Reserve more untouched historical observations.",
            ),
        ]

    @staticmethod
    def _minimum_check(
        code: str,
        category: str,
        description: str,
        observed: float,
        required: float,
        remediation: str,
    ) -> GateCheck:
        return GateCheck(
            code=code,
            category=category,
            description=description,
            observed=float(observed),
            required=float(required),
            comparison=">=",
            passed=float(observed) >= float(required),
            blocking=True,
            remediation=remediation,
        )

    @staticmethod
    def _maximum_check(
        code: str,
        category: str,
        description: str,
        observed: float,
        required: float,
        remediation: str,
    ) -> GateCheck:
        return GateCheck(
            code=code,
            category=category,
            description=description,
            observed=float(observed),
            required=float(required),
            comparison="<=",
            passed=float(observed) <= float(required),
            blocking=True,
            remediation=remediation,
        )


def gather_sufficiency_evidence(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    monte_carlo_paths: int = 5_000,
) -> SufficiencyEvidence:
    events = build_backtest_events(
        database_path,
        strict=True,
    )

    event_summary = events.summarize()

    strategy = ConservativeCompositeStrategy()
    engine_configuration = EngineConfiguration(
        risk=RiskLimits(),
    )

    baseline = InstitutionalBacktestEngine(
        strategy,
        engine_configuration,
    ).run(events)

    diagnostics_engine = StrategyDiagnosticsEngine(
        DiagnosticsConfiguration(
            training_fraction=0.70,
        )
    )

    (
        _executed,
        _comparisons,
        _thresholds,
        _cycles,
        diagnostics_summary,
    ) = diagnostics_engine.run(
        events,
        baseline,
        strategy=strategy,
    )

    walk_forward_configuration = WalkForwardConfiguration()
    folds = WalkForwardValidator(
        walk_forward_configuration,
        engine_configuration=engine_configuration,
    ).run(events)

    baseline_trade_profits = [
        trade.realized_profit_usd
        for trade in baseline.trades
    ]

    paths = MonteCarloStressTester(
        MonteCarloConfiguration(
            simulations=monte_carlo_paths,
        )
    ).run(
        baseline_trade_profits,
        initial_capital_usd=(
            engine_configuration.risk.initial_capital_usd
        ),
    )

    validation_summary = summarize_validation(
        folds,
        paths,
        initial_capital_usd=(
            engine_configuration.risk.initial_capital_usd
        ),
    )

    finite_profit_factors = [
        fold.test_metrics.profit_factor
        for fold in folds
        if math.isfinite(
            fold.test_metrics.profit_factor
        )
    ]

    average_out_of_sample_profit_factor = (
        sum(finite_profit_factors)
        / len(finite_profit_factors)
        if finite_profit_factors
        else 0.0
    )

    return SufficiencyEvidence(
        total_events=event_summary.total_events,
        scanner_cycles=event_summary.total_cycles,
        successful_quotes=event_summary.successful_quotes,
        profitable_observations=event_summary.profitable_events,
        quote_errors=event_summary.quote_errors,
        executed_backtest_trades=baseline.metrics.trades,
        executed_backtest_wins=baseline.metrics.wins,
        executed_backtest_losses=baseline.metrics.losses,
        baseline_net_profit_usd=baseline.metrics.net_profit_usd,
        baseline_profit_factor=baseline.metrics.profit_factor,
        baseline_maximum_drawdown_pct=(
            baseline.metrics.maximum_drawdown_pct
        ),
        walk_forward_folds=validation_summary.folds,
        profitable_walk_forward_folds=(
            validation_summary.profitable_test_folds
        ),
        unprofitable_walk_forward_folds=(
            validation_summary.unprofitable_test_folds
        ),
        total_out_of_sample_trades=(
            validation_summary.total_test_trades
        ),
        aggregate_out_of_sample_profit_usd=(
            validation_summary.aggregate_test_profit_usd
        ),
        average_out_of_sample_profit_factor=(
            average_out_of_sample_profit_factor
        ),
        worst_out_of_sample_drawdown_pct=(
            validation_summary.worst_test_drawdown_pct
        ),
        monte_carlo_paths=len(paths),
        monte_carlo_probability_below_start=(
            validation_summary.probability_of_finishing_below_start
        ),
        monte_carlo_median_ending_capital_usd=(
            validation_summary
            .median_monte_carlo_ending_capital_usd
        ),
        monte_carlo_p05_ending_capital_usd=(
            validation_summary
            .p05_monte_carlo_ending_capital_usd
        ),
        monte_carlo_p95_drawdown_pct=(
            validation_summary
            .p95_monte_carlo_drawdown_pct
        ),
        training_events=diagnostics_summary.training_events,
        training_cycles=diagnostics_summary.training_cycles,
        holdout_events=diagnostics_summary.holdout_events,
        holdout_cycles=diagnostics_summary.holdout_cycles,
    )


def run_data_sufficiency_gate(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    output_path: str | Path = DEFAULT_REPORT_PATH,
    monte_carlo_paths: int = 5_000,
    requirements: SufficiencyRequirements | None = None,
) -> SufficiencyReport:
    evidence = gather_sufficiency_evidence(
        database_path,
        monte_carlo_paths=monte_carlo_paths,
    )

    report = DataSufficiencyGate(
        requirements
    ).evaluate(
        evidence,
        database_path=database_path,
    )

    report.export_json(output_path)
    return report


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate whether the research dataset is sufficient "
            "for strategy promotion or live-readiness review."
        )
    )

    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
    )

    parser.add_argument(
        "--output",
        default=str(DEFAULT_REPORT_PATH),
    )

    parser.add_argument(
        "--monte-carlo-paths",
        type=int,
        default=5_000,
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
    )

    return parser


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = _build_argument_parser().parse_args(
        argv
    )
    _configure_logging(args.verbose)

    try:
        report = run_data_sufficiency_gate(
            args.database,
            output_path=args.output,
            monte_carlo_paths=args.monte_carlo_paths,
        )
    except (
        DataSufficiencyError,
        BacktestEngineError,
        EventBuilderError,
        StrategyDiagnosticsError,
        ValidationEngineError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nPhase 10C Step 4 — Data Sufficiency Gate")
    print("=" * 80)
    print(f"Database: {report.database_path}")
    print(f"Operating mode: {report.operating_mode}")
    print(f"Final decision: {report.final_decision}")
    print()

    print("Gate Summary")
    print("-" * 80)
    print(f"Checks passed: {report.checks_passed}")
    print(f"Checks failed: {report.checks_failed}")
    print(f"Blocking failures: {report.blocking_failures}")
    print(f"Data sufficient: {report.data_sufficient}")
    print(
        "Strategy promotion allowed: "
        f"{report.strategy_promotion_allowed}"
    )
    print(
        "Live readiness allowed: "
        f"{report.live_readiness_allowed}"
    )
    print()

    print("Evidence")
    print("-" * 80)
    evidence = report.evidence
    print(
        "Events / cycles / profitable observations: "
        f"{evidence.total_events} / "
        f"{evidence.scanner_cycles} / "
        f"{evidence.profitable_observations}"
    )
    print(
        "Executed trades / wins / losses: "
        f"{evidence.executed_backtest_trades} / "
        f"{evidence.executed_backtest_wins} / "
        f"{evidence.executed_backtest_losses}"
    )
    print(
        "Walk-forward folds / profitable folds: "
        f"{evidence.walk_forward_folds} / "
        f"{evidence.profitable_walk_forward_folds}"
    )
    print(
        "Out-of-sample trades / profit: "
        f"{evidence.total_out_of_sample_trades} / "
        f"${evidence.aggregate_out_of_sample_profit_usd:.6f}"
    )
    print(
        "Monte Carlo loss probability: "
        f"{evidence.monte_carlo_probability_below_start * 100.0:.2f}%"
    )
    print(
        "Training / holdout cycles: "
        f"{evidence.training_cycles} / "
        f"{evidence.holdout_cycles}"
    )
    print()

    print("Gate Checks")
    print("-" * 80)

    for check in report.checks:
        status = "PASS" if check.passed else "FAIL"

        print(
            f"{status:4} | {check.code:32} | "
            f"observed={check.observed:.6f} "
            f"{check.comparison} required={check.required:.6f}"
        )

        if not check.passed:
            print(f"       {check.remediation}")

    print()
    print(f"Report: {args.output}")

    return 0 if report.data_sufficient else 2


if __name__ == "__main__":
    raise SystemExit(main())