"""
Phase 11E — Institutional Research Report and Promotion Decision.

Combines existing JSON evidence into one read-only report. It never modifies
SQLite, scanner state, risk controls, wallet settings, or live execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

REPORT_SCHEMA_VERSION = "11E.1.0"
DEFAULT_OUTPUT_DIRECTORY = Path("research") / "final_report"


class ResearchReportError(RuntimeError):
    pass


class MissingEvidenceError(ResearchReportError):
    pass


@dataclass(frozen=True, slots=True)
class EvidencePaths:
    backtest_report: Path = Path("backtesting/results/backtest_report.json")
    validation_report: Path = Path(
        "backtesting/validation_results/validation_report.json"
    )
    diagnostics_report: Path = Path(
        "backtesting/diagnostics/strategy_diagnostics.json"
    )
    sufficiency_report: Path = Path(
        "backtesting/readiness/data_sufficiency_report.json"
    )
    feature_store_metadata: Path = Path(
        "research/feature_store/feature_store_metadata.json"
    )
    regime_summary: Path = Path(
        "research/regimes/regime_summary.json"
    )
    strategy_lab_report: Path = Path(
        "research/strategy_lab/strategy_report.json"
    )
    ml_research_report: Path = Path(
        "research/ml_research/ml_research_report.json"
    )

    def items(self) -> tuple[tuple[str, Path], ...]:
        return (
            ("backtest_report", self.backtest_report),
            ("validation_report", self.validation_report),
            ("diagnostics_report", self.diagnostics_report),
            ("sufficiency_report", self.sufficiency_report),
            ("feature_store_metadata", self.feature_store_metadata),
            ("regime_summary", self.regime_summary),
            ("strategy_lab_report", self.strategy_lab_report),
            ("ml_research_report", self.ml_research_report),
        )


@dataclass(frozen=True, slots=True)
class ReportConfiguration:
    evidence_paths: EvidencePaths = EvidencePaths()
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    require_all_evidence: bool = True
    overwrite: bool = True


@dataclass(frozen=True, slots=True)
class EvidenceFile:
    name: str
    path: str
    exists: bool
    size_bytes: int
    sha256: str | None
    loaded: bool
    error: str | None


@dataclass(frozen=True, slots=True)
class PromotionCheck:
    code: str
    description: str
    passed: bool
    blocking: bool
    observed: Any
    required: Any
    reason: str


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    generated_at: datetime
    decision: str
    paper_promotion_allowed: bool
    live_promotion_allowed: bool
    blocking_failures: int
    passed_checks: int
    failed_checks: int
    primary_reason: str
    supporting_reasons: tuple[str, ...]
    checks: tuple[PromotionCheck, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["generated_at"] = self.generated_at.isoformat()
        return data


@dataclass(frozen=True, slots=True)
class ResearchSummary:
    generated_at: datetime
    schema_version: str
    events: int
    cycles: int
    profitable_observations: int
    quote_errors: int
    backtest_trades: int
    backtest_wins: int
    backtest_losses: int
    backtest_profit_usd: float
    backtest_drawdown_pct: float
    walk_forward_folds: int
    profitable_folds: int
    out_of_sample_trades: int
    out_of_sample_profit_usd: float
    monte_carlo_loss_probability: float
    strongest_feature: str | None
    diagnostics_statistically_weak: bool
    feature_store_rows: int
    feature_count: int
    label_count: int
    unique_regimes: int
    strongest_regime: str | None
    weakest_regime: str | None
    profitable_strategy_candidates: int
    best_strategy_candidate: str | None
    strategy_lab_statistically_weak: bool
    ml_statistically_sufficient: bool
    ml_models_completed: int
    ml_champion: str | None
    ml_promotion_allowed: bool
    data_sufficient: bool
    sufficiency_blocking_failures: int
    final_decision: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["generated_at"] = self.generated_at.isoformat()
        return data


class EvidenceLoader:
    def __init__(
        self,
        configuration: ReportConfiguration,
    ) -> None:
        self.configuration = configuration

    def load(
        self,
    ) -> tuple[dict[str, Mapping[str, Any]], tuple[EvidenceFile, ...]]:
        evidence: dict[str, Mapping[str, Any]] = {}
        files: list[EvidenceFile] = []
        failures: list[str] = []

        for name, path in self.configuration.evidence_paths.items():
            if not path.exists():
                files.append(
                    EvidenceFile(
                        name=name,
                        path=str(path),
                        exists=False,
                        size_bytes=0,
                        sha256=None,
                        loaded=False,
                        error="File not found.",
                    )
                )
                failures.append(f"{name}: {path}")
                continue

            try:
                payload = json.loads(path.read_text(encoding="utf-8"))

                if not isinstance(payload, Mapping):
                    raise ResearchReportError(
                        f"{path} does not contain a JSON object."
                    )

                evidence[name] = payload
                files.append(
                    EvidenceFile(
                        name=name,
                        path=str(path),
                        exists=True,
                        size_bytes=path.stat().st_size,
                        sha256=sha256_file(path),
                        loaded=True,
                        error=None,
                    )
                )
            except Exception as error:
                files.append(
                    EvidenceFile(
                        name=name,
                        path=str(path),
                        exists=True,
                        size_bytes=path.stat().st_size,
                        sha256=sha256_file(path),
                        loaded=False,
                        error=str(error),
                    )
                )
                failures.append(f"{name}: {error}")

        if failures and self.configuration.require_all_evidence:
            raise MissingEvidenceError(
                "Required evidence could not be loaded:\n"
                + "\n".join(failures)
            )

        return evidence, tuple(files)


class PromotionEngine:
    def evaluate(
        self,
        evidence: Mapping[str, Mapping[str, Any]],
    ) -> PromotionDecision:
        checks = self._checks(evidence)
        blocking_failures = sum(
            check.blocking and not check.passed
            for check in checks
        )
        passed = sum(check.passed for check in checks)
        failed = len(checks) - passed

        paper_allowed = blocking_failures == 0

        sufficiency = evidence.get("sufficiency_report", {})
        ml_summary = mapping(
            evidence.get("ml_research_report", {}).get("summary")
        )

        live_allowed = (
            paper_allowed
            and bool(sufficiency.get("live_readiness_allowed", False))
            and bool(ml_summary.get("promotion_allowed", False))
        )

        if live_allowed:
            decision = "ELIGIBLE_FOR_LIVE_READINESS_REVIEW"
            primary_reason = (
                "Research gates passed, but a separate operational, security, "
                "wallet, and execution review is still required."
            )
        elif paper_allowed:
            decision = "PROMOTE_TO_PAPER_TRADING"
            primary_reason = (
                "Evidence supports controlled paper testing only. "
                "Live trading remains blocked."
            )
        else:
            decision = "BLOCK_PROMOTION"
            primary_reason = "One or more blocking research gates failed."

        return PromotionDecision(
            generated_at=datetime.now(timezone.utc),
            decision=decision,
            paper_promotion_allowed=paper_allowed,
            live_promotion_allowed=live_allowed,
            blocking_failures=blocking_failures,
            passed_checks=passed,
            failed_checks=failed,
            primary_reason=primary_reason,
            supporting_reasons=tuple(
                check.reason for check in checks if not check.passed
            ),
            checks=tuple(checks),
        )

    def _checks(
        self,
        evidence: Mapping[str, Mapping[str, Any]],
    ) -> list[PromotionCheck]:
        sufficiency = evidence.get("sufficiency_report", {})
        validation = mapping(
            evidence.get("validation_report", {}).get("summary")
        )
        diagnostics = mapping(
            evidence.get("diagnostics_report", {}).get("summary")
        )
        strategy = mapping(
            evidence.get("strategy_lab_report", {}).get("summary")
        )
        ml_summary = mapping(
            evidence.get("ml_research_report", {}).get("summary")
        )
        backtest = mapping(
            evidence.get("backtest_report", {}).get("metrics")
        )

        return [
            boolean_check(
                "DATA_SUFFICIENCY",
                "Data sufficiency gate passed",
                bool(sufficiency.get("data_sufficient", False)),
                "Historical evidence remains below hard minimums.",
            ),
            minimum_check(
                "BACKTEST_PROFIT",
                "Baseline backtest profit is positive",
                number(backtest.get("net_profit_usd")),
                0.0,
                strict=True,
                reason="Baseline backtest remains unprofitable.",
            ),
            minimum_check(
                "OUT_OF_SAMPLE_PROFIT",
                "Aggregate out-of-sample profit is positive",
                number(validation.get("aggregate_test_profit_usd")),
                0.0,
                strict=True,
                reason="Walk-forward out-of-sample profit is not positive.",
            ),
            minimum_check(
                "PROFITABLE_FOLDS",
                "At least three walk-forward folds are profitable",
                number(validation.get("profitable_test_folds")),
                3.0,
                strict=False,
                reason="Too few profitable walk-forward folds.",
            ),
            maximum_check(
                "MONTE_CARLO_LOSS_PROBABILITY",
                "Monte Carlo loss probability is no greater than 40%",
                number(
                    validation.get("probability_of_finishing_below_start")
                ),
                0.40,
                "Monte Carlo loss probability exceeds 40%.",
            ),
            boolean_check(
                "DIAGNOSTICS_STRENGTH",
                "Strategy diagnostics are statistically credible",
                not bool(diagnostics.get("statistically_weak", True)),
                "Strategy diagnostics remain statistically weak.",
            ),
            minimum_check(
                "STRATEGY_CANDIDATES",
                "Strategy Lab found a profitable candidate",
                number(strategy.get("profitable_candidates")),
                1.0,
                strict=False,
                reason="Strategy Lab found no profitable candidate.",
            ),
            boolean_check(
                "STRATEGY_LAB_STRENGTH",
                "Strategy Lab evidence is statistically credible",
                not bool(strategy.get("statistically_weak", True)),
                "Strategy Lab findings remain statistically weak.",
            ),
            boolean_check(
                "ML_DATA_SUFFICIENCY",
                "ML dataset is statistically sufficient",
                bool(ml_summary.get("statistically_sufficient", False)),
                "ML training is blocked by insufficient class balance or cycles.",
            ),
            boolean_check(
                "ML_PROMOTION",
                "ML champion passed promotion checks",
                bool(ml_summary.get("promotion_allowed", False)),
                "No ML champion is approved for promotion.",
            ),
        ]


class ReportBuilder:
    def __init__(
        self,
        configuration: ReportConfiguration | None = None,
    ) -> None:
        self.configuration = configuration or ReportConfiguration()

    def build(
        self,
    ) -> tuple[
        dict[str, Any],
        PromotionDecision,
        ResearchSummary,
        tuple[EvidenceFile, ...],
    ]:
        evidence, files = EvidenceLoader(self.configuration).load()
        decision = PromotionEngine().evaluate(evidence)
        summary = self._summary(evidence, decision)

        report = {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary.to_dict(),
            "promotion_decision": decision.to_dict(),
            "evidence_files": [asdict(item) for item in files],
            "evidence": evidence,
            "governance": {
                "live_execution_enabled": False,
                "wallet_connection_authorized": False,
                "automatic_model_promotion_enabled": False,
                "automatic_strategy_promotion_enabled": False,
                "required_next_step": (
                    "Continue paper data collection and rerun all research "
                    "gates after meaningful new evidence."
                ),
            },
        }

        return report, decision, summary, files

    @staticmethod
    def _summary(
        evidence: Mapping[str, Mapping[str, Any]],
        decision: PromotionDecision,
    ) -> ResearchSummary:
        sufficiency = evidence.get("sufficiency_report", {})
        sufficiency_data = mapping(sufficiency.get("evidence"))
        backtest = mapping(
            evidence.get("backtest_report", {}).get("metrics")
        )
        validation = mapping(
            evidence.get("validation_report", {}).get("summary")
        )
        diagnostics = mapping(
            evidence.get("diagnostics_report", {}).get("summary")
        )
        feature_store = mapping(
            evidence.get("feature_store_metadata", {}).get("summary")
        )
        regimes = evidence.get("regime_summary", {})
        strategy = mapping(
            evidence.get("strategy_lab_report", {}).get("summary")
        )
        ml_summary = mapping(
            evidence.get("ml_research_report", {}).get("summary")
        )

        return ResearchSummary(
            generated_at=datetime.now(timezone.utc),
            schema_version=REPORT_SCHEMA_VERSION,
            events=integer(sufficiency_data.get("total_events")),
            cycles=integer(sufficiency_data.get("scanner_cycles")),
            profitable_observations=integer(
                sufficiency_data.get("profitable_observations")
            ),
            quote_errors=integer(sufficiency_data.get("quote_errors")),
            backtest_trades=integer(backtest.get("trades")),
            backtest_wins=integer(backtest.get("wins")),
            backtest_losses=integer(backtest.get("losses")),
            backtest_profit_usd=number(backtest.get("net_profit_usd")),
            backtest_drawdown_pct=number(
                backtest.get("maximum_drawdown_pct")
            ),
            walk_forward_folds=integer(validation.get("folds")),
            profitable_folds=integer(
                validation.get("profitable_test_folds")
            ),
            out_of_sample_trades=integer(
                validation.get("total_test_trades")
            ),
            out_of_sample_profit_usd=number(
                validation.get("aggregate_test_profit_usd")
            ),
            monte_carlo_loss_probability=number(
                validation.get("probability_of_finishing_below_start")
            ),
            strongest_feature=optional_text(
                diagnostics.get("strongest_feature")
            ),
            diagnostics_statistically_weak=bool(
                diagnostics.get("statistically_weak", True)
            ),
            feature_store_rows=integer(feature_store.get("rows")),
            feature_count=integer(feature_store.get("feature_count")),
            label_count=integer(feature_store.get("label_count")),
            unique_regimes=integer(regimes.get("unique_regimes")),
            strongest_regime=optional_text(regimes.get("strongest_regime")),
            weakest_regime=optional_text(regimes.get("weakest_regime")),
            profitable_strategy_candidates=integer(
                strategy.get("profitable_candidates")
            ),
            best_strategy_candidate=optional_text(
                strategy.get("best_candidate_rule")
            ),
            strategy_lab_statistically_weak=bool(
                strategy.get("statistically_weak", True)
            ),
            ml_statistically_sufficient=bool(
                ml_summary.get("statistically_sufficient", False)
            ),
            ml_models_completed=integer(
                ml_summary.get("models_completed")
            ),
            ml_champion=optional_text(ml_summary.get("champion_model")),
            ml_promotion_allowed=bool(
                ml_summary.get("promotion_allowed", False)
            ),
            data_sufficient=bool(
                sufficiency.get("data_sufficient", False)
            ),
            sufficiency_blocking_failures=integer(
                sufficiency.get("blocking_failures")
            ),
            final_decision=decision.decision,
        )


def export_report(
    report: Mapping[str, Any],
    decision: PromotionDecision,
    summary: ResearchSummary,
    files: Sequence[EvidenceFile],
    configuration: ReportConfiguration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(parents=True, exist_ok=True)

    institutional = output / "institutional_research_report.json"
    executive = output / "executive_summary.txt"
    promotion = output / "promotion_decision.json"
    manifest = output / "evidence_manifest.json"

    destinations = (institutional, executive, promotion, manifest)

    if not configuration.overwrite:
        existing = [path for path in destinations if path.exists()]
        if existing:
            raise ResearchReportError(
                "Refusing to overwrite: "
                + ", ".join(str(path) for path in existing)
            )

    institutional.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    promotion.write_text(
        json.dumps(decision.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    executive.write_text(
        executive_summary(summary, decision),
        encoding="utf-8",
    )
    manifest.write_text(
        json.dumps(
            {
                "schema_version": REPORT_SCHEMA_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "evidence_files": [asdict(item) for item in files],
                "outputs": {
                    "institutional_report": str(institutional),
                    "executive_summary": str(executive),
                    "promotion_decision": str(promotion),
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return destinations


def executive_summary(
    summary: ResearchSummary,
    decision: PromotionDecision,
) -> str:
    lines = [
        "PHASE 11E — INSTITUTIONAL RESEARCH REPORT",
        "=" * 72,
        "",
        "Historical Dataset",
        "-" * 72,
        f"Events: {summary.events}",
        f"Cycles: {summary.cycles}",
        f"Profitable observations: {summary.profitable_observations}",
        f"Quote errors: {summary.quote_errors}",
        "",
        "Backtest",
        "-" * 72,
        f"Trades: {summary.backtest_trades}",
        f"Wins / losses: {summary.backtest_wins} / {summary.backtest_losses}",
        f"Net profit: ${summary.backtest_profit_usd:.6f}",
        f"Maximum drawdown: {summary.backtest_drawdown_pct:.6f}%",
        "",
        "Walk-Forward and Monte Carlo",
        "-" * 72,
        f"Profitable folds: {summary.profitable_folds} / "
        f"{summary.walk_forward_folds}",
        f"Out-of-sample trades: {summary.out_of_sample_trades}",
        f"Out-of-sample profit: ${summary.out_of_sample_profit_usd:.6f}",
        "Monte Carlo probability below start: "
        f"{summary.monte_carlo_loss_probability * 100.0:.2f}%",
        "",
        "Strategy Research",
        "-" * 72,
        f"Strongest diagnostic feature: {summary.strongest_feature}",
        "Strategy Lab profitable candidates: "
        f"{summary.profitable_strategy_candidates}",
        f"Best strategy candidate: {summary.best_strategy_candidate}",
        f"Unique regimes: {summary.unique_regimes}",
        f"Strongest / weakest regime: "
        f"{summary.strongest_regime} / {summary.weakest_regime}",
        "",
        "Machine Learning",
        "-" * 72,
        f"Statistically sufficient: {summary.ml_statistically_sufficient}",
        f"Models completed: {summary.ml_models_completed}",
        f"Champion model: {summary.ml_champion}",
        f"ML promotion allowed: {summary.ml_promotion_allowed}",
        "",
        "Data Sufficiency",
        "-" * 72,
        f"Data sufficient: {summary.data_sufficient}",
        f"Blocking failures: {summary.sufficiency_blocking_failures}",
        "",
        "FINAL DECISION",
        "=" * 72,
        decision.decision,
        "",
        decision.primary_reason,
    ]

    if decision.supporting_reasons:
        lines.extend(("", "Blocking reasons:"))
        lines.extend(
            f"- {reason}" for reason in decision.supporting_reasons
        )

    lines.extend(
        (
            "",
            "Governance",
            "-" * 72,
            "Live execution enabled: False",
            "Wallet connection authorized: False",
            "Automatic model promotion enabled: False",
            "Automatic strategy promotion enabled: False",
            "",
        )
    )

    return "\n".join(lines)


def boolean_check(
    code: str,
    description: str,
    observed: bool,
    reason: str,
) -> PromotionCheck:
    return PromotionCheck(
        code=code,
        description=description,
        passed=observed,
        blocking=True,
        observed=observed,
        required=True,
        reason="Passed." if observed else reason,
    )


def minimum_check(
    code: str,
    description: str,
    observed: float,
    required: float,
    *,
    strict: bool,
    reason: str,
) -> PromotionCheck:
    passed = observed > required if strict else observed >= required
    return PromotionCheck(
        code=code,
        description=description,
        passed=passed,
        blocking=True,
        observed=observed,
        required=f"> {required}" if strict else f">= {required}",
        reason="Passed." if passed else reason,
    )


def maximum_check(
    code: str,
    description: str,
    observed: float,
    required: float,
    reason: str,
) -> PromotionCheck:
    passed = observed <= required
    return PromotionCheck(
        code=code,
        description=description,
        passed=passed,
        blocking=True,
        observed=observed,
        required=f"<= {required}",
        reason="Passed." if passed else reason,
    )


def mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def integer(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Generate the Phase 11E institutional research report."
    )
    result.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    result.add_argument(
        "--allow-missing-evidence",
        action="store_true",
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


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    configuration = ReportConfiguration(
        output_directory=Path(args.output_directory),
        require_all_evidence=not args.allow_missing_evidence,
        overwrite=not args.no_overwrite,
    )

    try:
        report, decision, summary, files = ReportBuilder(
            configuration
        ).build()
        output_paths = export_report(
            report,
            decision,
            summary,
            files,
            configuration,
        )
    except (ResearchReportError, OSError, ValueError) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nPhase 11E — Institutional Research Report")
    print("=" * 80)
    print(f"Events: {summary.events}")
    print(f"Cycles: {summary.cycles}")
    print(f"Backtest trades: {summary.backtest_trades}")
    print(f"Backtest profit: ${summary.backtest_profit_usd:.6f}")
    print(
        "Profitable walk-forward folds: "
        f"{summary.profitable_folds} / {summary.walk_forward_folds}"
    )
    print(
        "Out-of-sample profit: "
        f"${summary.out_of_sample_profit_usd:.6f}"
    )
    print(
        "Monte Carlo loss probability: "
        f"{summary.monte_carlo_loss_probability * 100.0:.2f}%"
    )
    print(
        "Strategy Lab profitable candidates: "
        f"{summary.profitable_strategy_candidates}"
    )
    print(f"ML champion: {summary.ml_champion}")
    print(f"Data sufficient: {summary.data_sufficient}")
    print()
    print("FINAL DECISION")
    print("-" * 80)
    print(decision.decision)
    print(decision.primary_reason)

    if decision.supporting_reasons:
        print()
        print("Blocking reasons:")
        for reason in decision.supporting_reasons:
            print(f"  - {reason}")

    print()
    print("Output files")
    print("-" * 80)
    for path in output_paths:
        print(path)

    return 2 if decision.decision == "BLOCK_PROMOTION" else 0


if __name__ == "__main__":
    raise SystemExit(main())