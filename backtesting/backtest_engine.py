"""
Phase 10C — Institutional event-driven backtesting engine.

Zero-lookahead rule:
Strategies receive only StrategyEventView fields. Realized outcome fields are
revealed only after the strategy decision and hard risk checks.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

try:
    from backtesting.event_builder import (
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from backtesting.historical_dataset import DEFAULT_DATABASE_PATH
except ModuleNotFoundError:
    from event_builder import (
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from historical_dataset import DEFAULT_DATABASE_PATH


LOGGER = logging.getLogger(__name__)
DEFAULT_OUTPUT_DIRECTORY = Path("backtesting") / "results"


class BacktestEngineError(RuntimeError):
    pass


class InvalidBacktestConfigurationError(BacktestEngineError):
    pass


class StrategyExecutionError(BacktestEngineError):
    pass


@dataclass(frozen=True, slots=True)
class StrategyEventView:
    source_event_id: int
    timestamp: datetime
    cycle_number: int
    cycle_id: str
    cycle_position: int
    token: str
    token_key: str
    mint: str | None
    asset_key: str
    buy_route: str | None
    sell_route: str | None
    route_pair: str
    starting_amount_usd: float
    estimated_cost_usd: float
    cost_bps: float
    decision: str
    eligible: bool
    quote_successful: bool
    market_score: float
    liquidity_score: float
    volume_score: float
    pair_score: float
    intelligence_score: float
    composite_market_score: float
    score_dispersion: float
    minimum_component_score: float
    maximum_component_score: float
    has_mint: bool
    has_route: bool
    has_error: bool


@dataclass(frozen=True, slots=True)
class StrategyContext:
    current_capital_usd: float
    peak_capital_usd: float
    current_drawdown_pct: float
    trades_taken: int
    wins: int
    losses: int
    consecutive_losses: int
    cycle_trades_taken: int
    cycle_profit_usd: float
    historical_win_rate: float
    recent_trade_count: int
    recent_win_rate: float
    recent_average_profit_usd: float


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    action: str
    confidence: float
    reason: str
    risk_fraction: float = 1.0

    def validate(self) -> None:
        action = self.action.strip().upper()
        if action not in {"EXECUTE", "SKIP"}:
            raise StrategyExecutionError(f"Unsupported action: {self.action!r}")
        if not math.isfinite(self.confidence) or not 0 <= self.confidence <= 1:
            raise StrategyExecutionError("confidence must be in [0, 1].")
        if not math.isfinite(self.risk_fraction) or not 0 <= self.risk_fraction <= 1:
            raise StrategyExecutionError("risk_fraction must be in [0, 1].")
        if not self.reason.strip():
            raise StrategyExecutionError("reason cannot be empty.")


class BacktestStrategy(Protocol):
    @property
    def name(self) -> str:
        ...

    def decide(
        self,
        event: StrategyEventView,
        context: StrategyContext,
    ) -> StrategyDecision:
        ...


@dataclass(frozen=True, slots=True)
class RiskLimits:
    initial_capital_usd: float = 1_000.0
    risk_per_trade_pct: float = 0.25
    maximum_position_pct: float = 5.0
    maximum_cycle_loss_pct: float = 2.0
    maximum_drawdown_pct: float = 10.0
    maximum_consecutive_losses: int = 3
    maximum_trades_per_cycle: int = 3
    minimum_trade_notional_usd: float = 1.0
    minimum_confidence: float = 0.50
    cooldown_cycles: int = 2

    def validate(self) -> None:
        if not math.isfinite(self.initial_capital_usd) or self.initial_capital_usd <= 0:
            raise InvalidBacktestConfigurationError(
                "initial_capital_usd must be positive and finite."
            )
        for name in (
            "risk_per_trade_pct",
            "maximum_position_pct",
            "maximum_cycle_loss_pct",
            "maximum_drawdown_pct",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0 < value <= 100:
                raise InvalidBacktestConfigurationError(
                    f"{name} must be in (0, 100]."
                )
        if not 0 <= self.minimum_confidence <= 1:
            raise InvalidBacktestConfigurationError(
                "minimum_confidence must be in [0, 1]."
            )


@dataclass(frozen=True, slots=True)
class EngineConfiguration:
    risk: RiskLimits = RiskLimits()
    recent_window: int = 20
    reject_quote_errors: bool = True
    require_source_eligibility: bool = False
    require_source_execute: bool = False

    def validate(self) -> None:
        self.risk.validate()
        if self.recent_window <= 0:
            raise InvalidBacktestConfigurationError(
                "recent_window must be positive."
            )


@dataclass(frozen=True, slots=True)
class BacktestTrade:
    trade_number: int
    source_event_id: int
    timestamp: datetime
    cycle_number: int
    cycle_id: str
    token: str
    asset_key: str
    strategy_name: str
    reason: str
    confidence: float
    reference_notional_usd: float
    position_notional_usd: float
    position_scale: float
    reference_net_profit_usd: float
    realized_profit_usd: float
    capital_before_usd: float
    capital_after_usd: float
    drawdown_after_pct: float
    winning_trade: bool

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class BacktestRejection:
    source_event_id: int
    timestamp: datetime
    cycle_number: int
    cycle_id: str
    token: str
    stage: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class EquityPoint:
    timestamp: datetime
    cycle_number: int
    cycle_id: str
    capital_usd: float
    peak_capital_usd: float
    drawdown_pct: float
    cycle_profit_usd: float
    cumulative_profit_usd: float
    trades: int
    wins: int
    losses: int
    halted: bool

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class BacktestMetrics:
    initial_capital_usd: float
    ending_capital_usd: float
    net_profit_usd: float
    total_return_pct: float
    trades: int
    wins: int
    losses: int
    win_rate_pct: float
    average_trade_usd: float
    average_win_usd: float
    average_loss_usd: float
    payoff_ratio: float
    profit_factor: float
    expectancy_usd: float
    best_trade_usd: float
    worst_trade_usd: float
    maximum_drawdown_pct: float
    maximum_consecutive_losses: int
    positive_cycles: int
    negative_cycles: int
    flat_cycles: int
    rejected_events: int
    strategy_skips: int
    risk_rejections: int


@dataclass(frozen=True, slots=True)
class BacktestResult:
    strategy_name: str
    generated_at: datetime
    configuration: EngineConfiguration
    metrics: BacktestMetrics
    trades: tuple[BacktestTrade, ...]
    equity_curve: tuple[EquityPoint, ...]
    rejections: tuple[BacktestRejection, ...]

    def export(self, directory: str | Path = DEFAULT_OUTPUT_DIRECTORY) -> tuple[Path, ...]:
        output = Path(directory)
        output.mkdir(parents=True, exist_ok=True)

        trades_path = output / "backtest_trades.csv"
        equity_path = output / "backtest_equity_curve.csv"
        rejections_path = output / "backtest_rejections.csv"
        report_path = output / "backtest_report.json"

        _write_csv(trades_path, [row.to_dict() for row in self.trades])
        _write_csv(equity_path, [row.to_dict() for row in self.equity_curve])
        _write_csv(
            rejections_path,
            [row.to_dict() for row in self.rejections],
        )

        report_path.write_text(
            json.dumps(
                {
                    "strategy_name": self.strategy_name,
                    "generated_at": self.generated_at.isoformat(sep=" "),
                    "configuration": {
                        "risk": asdict(self.configuration.risk),
                        "recent_window": self.configuration.recent_window,
                        "reject_quote_errors": self.configuration.reject_quote_errors,
                        "require_source_eligibility": (
                            self.configuration.require_source_eligibility
                        ),
                        "require_source_execute": (
                            self.configuration.require_source_execute
                        ),
                    },
                    "metrics": asdict(self.metrics),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        return trades_path, equity_path, rejections_path, report_path


@dataclass(slots=True)
class _State:
    capital: float
    peak: float
    trades: int = 0
    wins: int = 0
    losses: int = 0
    consecutive_losses: int = 0
    max_consecutive_losses: int = 0
    cycle_number: int = 0
    cycle_id: str = ""
    cycle_start_capital: float = 0.0
    cycle_profit: float = 0.0
    cycle_trades: int = 0
    cooldown_until_cycle: int = -1
    halted: bool = False
    historical_profits: list[float] = field(default_factory=list)
    recent_profits: deque[float] = field(default_factory=deque)


class InstitutionalBacktestEngine:
    def __init__(
        self,
        strategy: BacktestStrategy,
        configuration: EngineConfiguration | None = None,
    ) -> None:
        self.strategy = strategy
        self.configuration = configuration or EngineConfiguration()
        self.configuration.validate()

    def run(
        self,
        events: BacktestEventCollection | Sequence[BacktestEvent],
    ) -> BacktestResult:
        ordered = tuple(events)
        if not ordered:
            raise BacktestEngineError("No events available.")

        state = _State(
            capital=self.configuration.risk.initial_capital_usd,
            peak=self.configuration.risk.initial_capital_usd,
            cycle_start_capital=self.configuration.risk.initial_capital_usd,
            recent_profits=deque(maxlen=self.configuration.recent_window),
        )
        trades: list[BacktestTrade] = []
        rejections: list[BacktestRejection] = []
        equity: list[EquityPoint] = []

        grouped: dict[str, list[BacktestEvent]] = defaultdict(list)
        for event in ordered:
            grouped[event.cycle_id].append(event)

        for cycle_id, cycle_events in grouped.items():
            cycle_events.sort(
                key=lambda event: (event.timestamp, event.source_event_id)
            )
            first = cycle_events[0]
            self._begin_cycle(state, first.cycle_number, cycle_id)

            for event in cycle_events:
                if state.halted:
                    rejections.append(
                        self._reject(event, "ENGINE", "Portfolio halted.")
                    )
                    continue

                view = self._view(event)
                context = self._context(state)

                try:
                    decision = self.strategy.decide(view, context)
                    decision.validate()
                except Exception as error:
                    LOGGER.exception("Strategy error")
                    rejections.append(
                        self._reject(
                            event,
                            "STRATEGY",
                            f"Strategy error: {error}",
                        )
                    )
                    continue

                if decision.action.strip().upper() != "EXECUTE":
                    rejections.append(
                        self._reject(
                            event,
                            "STRATEGY",
                            f"Skipped: {decision.reason}",
                        )
                    )
                    continue

                risk_reason = self._risk_reason(event, decision, state)
                if risk_reason:
                    rejections.append(
                        self._reject(event, "RISK", risk_reason)
                    )
                    continue

                position = self._position(event, decision, state)
                if position < self.configuration.risk.minimum_trade_notional_usd:
                    rejections.append(
                        self._reject(
                            event,
                            "SIZING",
                            "Position below minimum trade notional.",
                        )
                    )
                    continue

                trade = self._apply(
                    event,
                    decision,
                    position,
                    state,
                    len(trades) + 1,
                )
                trades.append(trade)
                self._post_trade_limits(state)

            equity.append(self._equity(first.timestamp, state))

        metrics = self._metrics(state, trades, equity, rejections)

        return BacktestResult(
            strategy_name=self.strategy.name,
            generated_at=datetime.now(),
            configuration=self.configuration,
            metrics=metrics,
            trades=tuple(trades),
            equity_curve=tuple(equity),
            rejections=tuple(rejections),
        )

    @staticmethod
    def _view(event: BacktestEvent) -> StrategyEventView:
        return StrategyEventView(
            source_event_id=event.source_event_id,
            timestamp=event.timestamp,
            cycle_number=event.cycle_number,
            cycle_id=event.cycle_id,
            cycle_position=event.cycle_position,
            token=event.token,
            token_key=event.token_key,
            mint=event.mint,
            asset_key=event.asset_key,
            buy_route=event.buy_route,
            sell_route=event.sell_route,
            route_pair=event.route_pair,
            starting_amount_usd=event.starting_amount_usd,
            estimated_cost_usd=event.estimated_cost_usd,
            cost_bps=event.cost_bps,
            decision=event.decision,
            eligible=event.eligible,
            quote_successful=event.quote_successful,
            market_score=event.market_score,
            liquidity_score=event.liquidity_score,
            volume_score=event.volume_score,
            pair_score=event.pair_score,
            intelligence_score=event.intelligence_score,
            composite_market_score=event.composite_market_score,
            score_dispersion=event.score_dispersion,
            minimum_component_score=event.minimum_component_score,
            maximum_component_score=event.maximum_component_score,
            has_mint=event.has_mint,
            has_route=event.has_route,
            has_error=event.has_error,
        )

    def _context(self, state: _State) -> StrategyContext:
        history = state.historical_profits
        recent = list(state.recent_profits)
        wins = [value for value in history if value > 0]
        recent_wins = [value for value in recent if value > 0]

        return StrategyContext(
            current_capital_usd=state.capital,
            peak_capital_usd=state.peak,
            current_drawdown_pct=self._drawdown(state),
            trades_taken=state.trades,
            wins=state.wins,
            losses=state.losses,
            consecutive_losses=state.consecutive_losses,
            cycle_trades_taken=state.cycle_trades,
            cycle_profit_usd=state.cycle_profit,
            historical_win_rate=len(wins) / len(history) if history else 0.0,
            recent_trade_count=len(recent),
            recent_win_rate=(
                len(recent_wins) / len(recent)
                if recent
                else 0.0
            ),
            recent_average_profit_usd=(
                statistics.fmean(recent)
                if recent
                else 0.0
            ),
        )

    def _risk_reason(
        self,
        event: BacktestEvent,
        decision: StrategyDecision,
        state: _State,
    ) -> str | None:
        limits = self.configuration.risk

        if self.configuration.reject_quote_errors and not event.quote_successful:
            return "Quote unsuccessful."
        if self.configuration.require_source_eligibility and not event.eligible:
            return "Source event not eligible."
        if self.configuration.require_source_execute and event.decision != "EXECUTE":
            return "Source event decision was not EXECUTE."
        if decision.confidence < limits.minimum_confidence:
            return "Strategy confidence below hard minimum."
        if state.cycle_trades >= limits.maximum_trades_per_cycle:
            return "Maximum trades per cycle reached."
        if state.consecutive_losses >= limits.maximum_consecutive_losses:
            return "Maximum consecutive losses reached."
        if state.cycle_number <= state.cooldown_until_cycle:
            return "Cooldown active."

        cycle_loss_limit = (
            state.cycle_start_capital
            * limits.maximum_cycle_loss_pct
            / 100.0
        )
        if state.cycle_profit <= -cycle_loss_limit:
            return "Maximum cycle loss reached."
        if self._drawdown(state) >= limits.maximum_drawdown_pct:
            return "Maximum drawdown reached."
        return None

    def _position(
        self,
        event: BacktestEvent,
        decision: StrategyDecision,
        state: _State,
    ) -> float:
        limits = self.configuration.risk
        risk_budget = state.capital * limits.risk_per_trade_pct / 100.0
        position_cap = state.capital * limits.maximum_position_pct / 100.0

        reference_loss = max(
            event.estimated_cost_usd,
            abs(event.net_profit_usd) if event.net_profit_usd < 0 else 0.0,
            1e-9,
        )
        scale = risk_budget / reference_loss

        return max(
            0.0,
            min(
                position_cap,
                event.starting_amount_usd
                * scale
                * decision.confidence
                * decision.risk_fraction,
            ),
        )

    def _apply(
        self,
        event: BacktestEvent,
        decision: StrategyDecision,
        position: float,
        state: _State,
        trade_number: int,
    ) -> BacktestTrade:
        reference = max(event.starting_amount_usd, 1e-12)
        scale = position / reference
        realized = event.net_profit_usd * scale
        before = state.capital

        state.capital += realized
        state.peak = max(state.peak, state.capital)
        state.trades += 1
        state.cycle_trades += 1
        state.cycle_profit += realized
        state.historical_profits.append(realized)
        state.recent_profits.append(realized)

        winning = realized > 0
        if winning:
            state.wins += 1
            state.consecutive_losses = 0
        else:
            state.losses += 1
            state.consecutive_losses += 1
            state.max_consecutive_losses = max(
                state.max_consecutive_losses,
                state.consecutive_losses,
            )

        return BacktestTrade(
            trade_number=trade_number,
            source_event_id=event.source_event_id,
            timestamp=event.timestamp,
            cycle_number=event.cycle_number,
            cycle_id=event.cycle_id,
            token=event.token,
            asset_key=event.asset_key,
            strategy_name=self.strategy.name,
            reason=decision.reason,
            confidence=decision.confidence,
            reference_notional_usd=event.starting_amount_usd,
            position_notional_usd=position,
            position_scale=scale,
            reference_net_profit_usd=event.net_profit_usd,
            realized_profit_usd=realized,
            capital_before_usd=before,
            capital_after_usd=state.capital,
            drawdown_after_pct=self._drawdown(state),
            winning_trade=winning,
        )

    def _post_trade_limits(self, state: _State) -> None:
        limits = self.configuration.risk
        if state.consecutive_losses >= limits.maximum_consecutive_losses:
            state.cooldown_until_cycle = state.cycle_number + limits.cooldown_cycles
        if self._drawdown(state) >= limits.maximum_drawdown_pct:
            state.halted = True
        if state.capital <= 0:
            state.halted = True

    @staticmethod
    def _begin_cycle(state: _State, cycle_number: int, cycle_id: str) -> None:
        state.cycle_number = cycle_number
        state.cycle_id = cycle_id
        state.cycle_start_capital = state.capital
        state.cycle_profit = 0.0
        state.cycle_trades = 0

    def _equity(self, timestamp: datetime, state: _State) -> EquityPoint:
        return EquityPoint(
            timestamp=timestamp,
            cycle_number=state.cycle_number,
            cycle_id=state.cycle_id,
            capital_usd=state.capital,
            peak_capital_usd=state.peak,
            drawdown_pct=self._drawdown(state),
            cycle_profit_usd=state.cycle_profit,
            cumulative_profit_usd=(
                state.capital - self.configuration.risk.initial_capital_usd
            ),
            trades=state.trades,
            wins=state.wins,
            losses=state.losses,
            halted=state.halted,
        )

    @staticmethod
    def _drawdown(state: _State) -> float:
        if state.peak <= 0:
            return 0.0
        return max(0.0, (state.peak - state.capital) / state.peak * 100.0)

    @staticmethod
    def _reject(
        event: BacktestEvent,
        stage: str,
        reason: str,
    ) -> BacktestRejection:
        return BacktestRejection(
            source_event_id=event.source_event_id,
            timestamp=event.timestamp,
            cycle_number=event.cycle_number,
            cycle_id=event.cycle_id,
            token=event.token,
            stage=stage,
            reason=reason,
        )

    def _metrics(
        self,
        state: _State,
        trades: Sequence[BacktestTrade],
        equity: Sequence[EquityPoint],
        rejections: Sequence[BacktestRejection],
    ) -> BacktestMetrics:
        initial = self.configuration.risk.initial_capital_usd
        profits = [trade.realized_profit_usd for trade in trades]
        wins = [value for value in profits if value > 0]
        losses = [value for value in profits if value < 0]

        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = (
            gross_profit / gross_loss
            if gross_loss > 0
            else (float("inf") if gross_profit > 0 else 0.0)
        )
        average_win = statistics.fmean(wins) if wins else 0.0
        average_loss = statistics.fmean(losses) if losses else 0.0
        payoff_ratio = (
            average_win / abs(average_loss)
            if average_loss < 0
            else 0.0
        )
        cycles = [point.cycle_profit_usd for point in equity]
        rejection_counts = Counter(item.stage for item in rejections)

        return BacktestMetrics(
            initial_capital_usd=initial,
            ending_capital_usd=state.capital,
            net_profit_usd=state.capital - initial,
            total_return_pct=(state.capital / initial - 1.0) * 100.0,
            trades=len(trades),
            wins=len(wins),
            losses=len(losses),
            win_rate_pct=len(wins) / len(trades) * 100.0 if trades else 0.0,
            average_trade_usd=statistics.fmean(profits) if profits else 0.0,
            average_win_usd=average_win,
            average_loss_usd=average_loss,
            payoff_ratio=payoff_ratio,
            profit_factor=profit_factor,
            expectancy_usd=statistics.fmean(profits) if profits else 0.0,
            best_trade_usd=max(profits) if profits else 0.0,
            worst_trade_usd=min(profits) if profits else 0.0,
            maximum_drawdown_pct=max(
                (point.drawdown_pct for point in equity),
                default=0.0,
            ),
            maximum_consecutive_losses=state.max_consecutive_losses,
            positive_cycles=sum(value > 0 for value in cycles),
            negative_cycles=sum(value < 0 for value in cycles),
            flat_cycles=sum(value == 0 for value in cycles),
            rejected_events=len(rejections),
            strategy_skips=rejection_counts.get("STRATEGY", 0),
            risk_rejections=rejection_counts.get("RISK", 0),
        )


class ConservativeCompositeStrategy:
    def __init__(
        self,
        minimum_composite_score: float = 85.0,
        minimum_market_score: float = 80.0,
        minimum_liquidity_score: float = 80.0,
        minimum_intelligence_score: float = 60.0,
        maximum_score_dispersion: float = 25.0,
    ) -> None:
        self.minimum_composite_score = minimum_composite_score
        self.minimum_market_score = minimum_market_score
        self.minimum_liquidity_score = minimum_liquidity_score
        self.minimum_intelligence_score = minimum_intelligence_score
        self.maximum_score_dispersion = maximum_score_dispersion

    @property
    def name(self) -> str:
        return "ConservativeCompositeStrategy"

    def decide(
        self,
        event: StrategyEventView,
        context: StrategyContext,
    ) -> StrategyDecision:
        if not event.quote_successful:
            return StrategyDecision("SKIP", 0.0, "Quote unsuccessful.")
        if not event.has_route:
            return StrategyDecision("SKIP", 0.0, "Route incomplete.")
        if event.composite_market_score < self.minimum_composite_score:
            return StrategyDecision("SKIP", 0.25, "Composite score too low.")
        if event.market_score < self.minimum_market_score:
            return StrategyDecision("SKIP", 0.25, "Market score too low.")
        if event.liquidity_score < self.minimum_liquidity_score:
            return StrategyDecision("SKIP", 0.25, "Liquidity score too low.")
        if event.intelligence_score < self.minimum_intelligence_score:
            return StrategyDecision("SKIP", 0.25, "Intelligence score too low.")
        if event.score_dispersion > self.maximum_score_dispersion:
            return StrategyDecision("SKIP", 0.30, "Score disagreement too high.")

        confidence = min(1.0, event.composite_market_score / 100.0)
        if context.consecutive_losses:
            confidence *= 0.75

        return StrategyDecision(
            action="EXECUTE",
            confidence=confidence,
            risk_fraction=0.50,
            reason="All pre-outcome gates passed.",
        )


def _write_csv(path: Path, records: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Phase 10C institutional backtest."
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    parser.add_argument("--initial-capital", type=float, default=1_000.0)
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.25)
    parser.add_argument("--maximum-position-pct", type=float, default=5.0)
    parser.add_argument("--minimum-confidence", type=float, default=0.50)
    parser.add_argument("--require-source-eligibility", action="store_true")
    parser.add_argument("--require-source-execute", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        events = build_backtest_events(args.database, strict=True)
        configuration = EngineConfiguration(
            risk=RiskLimits(
                initial_capital_usd=args.initial_capital,
                risk_per_trade_pct=args.risk_per_trade_pct,
                maximum_position_pct=args.maximum_position_pct,
                minimum_confidence=args.minimum_confidence,
            ),
            require_source_eligibility=args.require_source_eligibility,
            require_source_execute=args.require_source_execute,
        )
        engine = InstitutionalBacktestEngine(
            ConservativeCompositeStrategy(),
            configuration,
        )
        result = engine.run(events)
        paths = result.export(args.output_directory)
    except (BacktestEngineError, EventBuilderError, OSError, ValueError) as error:
        LOGGER.error("%s", error)
        return 1

    metrics = result.metrics
    print("\nInstitutional Event-Driven Backtest")
    print("=" * 76)
    print(f"Strategy: {result.strategy_name}")
    print(f"Initial capital: ${metrics.initial_capital_usd:.2f}")
    print(f"Ending capital: ${metrics.ending_capital_usd:.2f}")
    print(f"Net profit: ${metrics.net_profit_usd:.6f}")
    print(f"Total return: {metrics.total_return_pct:.6f}%")
    print(f"Trades: {metrics.trades}")
    print(f"Wins / losses: {metrics.wins} / {metrics.losses}")
    print(f"Win rate: {metrics.win_rate_pct:.2f}%")
    print(f"Profit factor: {metrics.profit_factor:.4f}")
    print(f"Expectancy: ${metrics.expectancy_usd:.6f}")
    print(f"Maximum drawdown: {metrics.maximum_drawdown_pct:.6f}%")
    print(f"Rejected events: {metrics.rejected_events}")
    print("\nOutput files")
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())