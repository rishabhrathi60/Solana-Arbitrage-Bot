"""
Phase 13F — Institutional Evidence and Promotion Gate

Aggregates evidence from the institutional research pipeline and produces one
final governance decision for research promotion and live readiness.

Default evidence sources
------------------------
Phase 10C:
    backtesting/readiness/data_sufficiency_report.json

Phase 13B:
    research/institutional_dataset/institutional_dataset_validation.json
    research/institutional_dataset/institutional_dataset_manifest.json

Phase 13C:
    research/institutional_feature_store/adapter_validation.json
    research/institutional_feature_store/feature_manifest.json

Phase 13D:
    research/institutional_walk_forward/institutional_walk_forward_report.json

Phase 13E:
    research/institutional_robustness/institutional_robustness_report.json

Outputs
-------
research/institutional_promotion_gate/
    institutional_evidence_report.json
    institutional_promotion_decision.json
    institutional_gate_checks.csv
    institutional_evidence_manifest.json
    executive_summary.txt

Governance
----------
This module never:
- connects a wallet;
- sends or signs a transaction;
- enables live execution;
- changes scanner decisions;
- alters research evidence;
- automatically promotes a strategy.

It only reads existing reports and emits a final blocking or allowing decision.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "13F.1.0"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_DATA_SUFFICIENCY_REPORT = (
    PROJECT_ROOT
    / "backtesting"
    / "readiness"
    / "data_sufficiency_report.json"
)

DEFAULT_DATASET_VALIDATION = (
    PROJECT_ROOT
    / "research"
    / "institutional_dataset"
    / "institutional_dataset_validation.json"
)

DEFAULT_DATASET_MANIFEST = (
    PROJECT_ROOT
    / "research"
    / "institutional_dataset"
    / "institutional_dataset_manifest.json"
)

DEFAULT_FEATURE_VALIDATION = (
    PROJECT_ROOT
    / "research"
    / "institutional_feature_store"
    / "adapter_validation.json"
)

DEFAULT_FEATURE_MANIFEST = (
    PROJECT_ROOT
    / "research"
    / "institutional_feature_store"
    / "feature_manifest.json"
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

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "research"
    / "institutional_promotion_gate"
)

EVIDENCE_REPORT_JSON = "institutional_evidence_report.json"
PROMOTION_DECISION_JSON = "institutional_promotion_decision.json"
GATE_CHECKS_CSV = "institutional_gate_checks.csv"
EVIDENCE_MANIFEST_JSON = "institutional_evidence_manifest.json"
EXECUTIVE_SUMMARY_TXT = "executive_summary.txt"


class InstitutionalPromotionGateError(RuntimeError):
    """Base exception for Phase 13F failures."""


@dataclass(frozen=True, slots=True)
class GateConfiguration:
    data_sufficiency_report: Path = DEFAULT_DATA_SUFFICIENCY_REPORT
    dataset_validation: Path = DEFAULT_DATASET_VALIDATION
    dataset_manifest: Path = DEFAULT_DATASET_MANIFEST
    feature_validation: Path = DEFAULT_FEATURE_VALIDATION
    feature_manifest: Path = DEFAULT_FEATURE_MANIFEST
    walk_forward_report: Path = DEFAULT_WALK_FORWARD_REPORT
    robustness_report: Path = DEFAULT_ROBUSTNESS_REPORT
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    minimum_institutional_cycles: int = 100
    minimum_institutional_rows: int = 10_000
    minimum_profitable_observations: int = 100
    minimum_verified_live_cycles: int = 20
    minimum_verified_live_rows: int = 1_000

    minimum_completed_folds: int = 5
    minimum_profitable_folds: int = 3
    minimum_oos_trades: int = 50
    minimum_oos_profit_usd: float = 0.01
    minimum_oos_profit_factor: float = 1.10
    maximum_oos_drawdown_percent: float = 10.0

    minimum_monte_carlo_paths: int = 5_000
    maximum_monte_carlo_loss_probability: float = 0.40
    minimum_median_stressed_profit_usd: float = 0.01
    minimum_profitable_stress_scenarios: int = 3

    require_phase_10c_pass: bool = True
    require_dataset_validation: bool = True
    require_feature_store_validation: bool = True
    require_walk_forward_promotion: bool = True
    require_robustness_pass: bool = True

    def validate(self) -> None:
        positive_integers = (
            "minimum_institutional_cycles",
            "minimum_institutional_rows",
            "minimum_profitable_observations",
            "minimum_verified_live_cycles",
            "minimum_verified_live_rows",
            "minimum_completed_folds",
            "minimum_profitable_folds",
            "minimum_oos_trades",
            "minimum_monte_carlo_paths",
            "minimum_profitable_stress_scenarios",
        )

        for name in positive_integers:
            if int(getattr(self, name)) <= 0:
                raise InstitutionalPromotionGateError(
                    f"{name} must be positive."
                )

        numeric_fields = (
            "minimum_oos_profit_usd",
            "minimum_oos_profit_factor",
            "maximum_oos_drawdown_percent",
            "maximum_monte_carlo_loss_probability",
            "minimum_median_stressed_profit_usd",
        )

        for name in numeric_fields:
            value = float(getattr(self, name))

            if not math.isfinite(value):
                raise InstitutionalPromotionGateError(
                    f"{name} must be finite."
                )

        if self.maximum_oos_drawdown_percent < 0:
            raise InstitutionalPromotionGateError(
                "maximum_oos_drawdown_percent cannot be negative."
            )

        if not 0.0 <= self.maximum_monte_carlo_loss_probability <= 1.0:
            raise InstitutionalPromotionGateError(
                "maximum_monte_carlo_loss_probability must be in [0, 1]."
            )


@dataclass(frozen=True, slots=True)
class GateCheck:
    category: str
    name: str
    passed: bool
    blocking: bool
    observed: Any
    comparison: str
    required: Any
    message: str
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def load_json(
    path: Path,
    *,
    required: bool = True,
) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise InstitutionalPromotionGateError(
                f"Required evidence file does not exist: {path}"
            )

        return {}

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise InstitutionalPromotionGateError(
            f"Expected JSON object: {path}"
        )

    return payload


def nested_get(
    mapping: Mapping[str, Any],
    *path: str,
    default: Any = None,
) -> Any:
    current: Any = mapping

    for key in path:
        if not isinstance(current, Mapping):
            return default

        if key not in current:
            return default

        current = current[key]

    return current


def first_value(
    mapping: Mapping[str, Any],
    paths: Sequence[Sequence[str]],
    *,
    default: Any = None,
) -> Any:
    for path in paths:
        value = nested_get(
            mapping,
            *path,
            default=None,
        )

        if value is not None:
            return value

    return default


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


class InstitutionalEvidenceGate:
    def __init__(
        self,
        configuration: GateConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or GateConfiguration()
        )
        self.configuration.validate()

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        tuple[GateCheck, ...],
        dict[str, Any],
    ]:
        phase_10c = load_json(
            self.configuration.data_sufficiency_report,
            required=self.configuration.require_phase_10c_pass,
        )

        dataset_validation = load_json(
            self.configuration.dataset_validation,
            required=self.configuration.require_dataset_validation,
        )

        dataset_manifest = load_json(
            self.configuration.dataset_manifest,
            required=True,
        )

        feature_validation = load_json(
            self.configuration.feature_validation,
            required=self.configuration.require_feature_store_validation,
        )

        feature_manifest = load_json(
            self.configuration.feature_manifest,
            required=True,
        )

        walk_forward = load_json(
            self.configuration.walk_forward_report,
            required=True,
        )

        robustness = load_json(
            self.configuration.robustness_report,
            required=True,
        )

        evidence = self._extract_evidence(
            phase_10c=phase_10c,
            dataset_validation=dataset_validation,
            dataset_manifest=dataset_manifest,
            feature_validation=feature_validation,
            feature_manifest=feature_manifest,
            walk_forward=walk_forward,
            robustness=robustness,
        )

        checks = self._build_checks(
            evidence
        )

        blocking_failures = [
            check
            for check in checks
            if check.blocking
            and not check.passed
        ]

        all_blocking_passed = (
            not blocking_failures
        )

        decision = (
            "ALLOW_RESEARCH_PROMOTION_AND_TINY_LIVE_PILOT_REVIEW"
            if all_blocking_passed
            else "BLOCK_STRATEGY_PROMOTION_AND_LIVE_READINESS"
        )

        research_promotion_allowed = (
            all_blocking_passed
        )

        live_readiness_allowed = (
            all_blocking_passed
            and safe_bool(
                evidence[
                    "walk_forward"
                ][
                    "promotion_allowed"
                ]
            )
            and safe_bool(
                evidence[
                    "robustness"
                ][
                    "robustness_passed"
                ]
            )
        )

        blocking_reasons = [
            check.message
            for check in blocking_failures
        ]

        passed_checks = sum(
            check.passed
            for check in checks
        )

        failed_checks = (
            len(checks)
            - passed_checks
        )

        summary = {
            "generated_at": (
                utc_now_text()
            ),
            "schema_version": (
                SCHEMA_VERSION
            ),
            "operating_mode": (
                "PAPER_RESEARCH_GOVERNANCE"
            ),
            "final_decision": (
                decision
            ),
            "research_promotion_allowed": (
                research_promotion_allowed
            ),
            "live_readiness_allowed": (
                live_readiness_allowed
            ),
            "checks_run": len(
                checks
            ),
            "checks_passed": (
                passed_checks
            ),
            "checks_failed": (
                failed_checks
            ),
            "blocking_failures": len(
                blocking_failures
            ),
            "blocking_reasons": (
                blocking_reasons
            ),
            "evidence": evidence,
            "live_execution_enabled": (
                False
            ),
            "wallet_connection_authorized": (
                False
            ),
            "automatic_promotion_enabled": (
                False
            ),
            "valid": True,
        }

        return (
            summary,
            tuple(checks),
            evidence,
        )

    def _extract_evidence(
        self,
        *,
        phase_10c: Mapping[str, Any],
        dataset_validation: Mapping[str, Any],
        dataset_manifest: Mapping[str, Any],
        feature_validation: Mapping[str, Any],
        feature_manifest: Mapping[str, Any],
        walk_forward: Mapping[str, Any],
        robustness: Mapping[str, Any],
    ) -> dict[str, Any]:
        dataset_summary = first_value(
            dataset_validation,
            (
                ("summary",),
                ("dataset_summary",),
            ),
            default={},
        )

        if not isinstance(
            dataset_summary,
            Mapping,
        ):
            dataset_summary = {}

        dataset_manifest_summary = first_value(
            dataset_manifest,
            (
                ("summary",),
            ),
            default={},
        )

        if not isinstance(
            dataset_manifest_summary,
            Mapping,
        ):
            dataset_manifest_summary = {}

        feature_summary = first_value(
            feature_validation,
            (
                ("summary",),
            ),
            default={},
        )

        if not isinstance(
            feature_summary,
            Mapping,
        ):
            feature_summary = {}

        feature_manifest_summary = first_value(
            feature_manifest,
            (
                ("summary",),
            ),
            default={},
        )

        if not isinstance(
            feature_manifest_summary,
            Mapping,
        ):
            feature_manifest_summary = {}

        walk_summary = first_value(
            walk_forward,
            (
                ("summary",),
            ),
            default={},
        )

        if not isinstance(
            walk_summary,
            Mapping,
        ):
            walk_summary = {}

        robustness_summary = first_value(
            robustness,
            (
                ("summary",),
            ),
            default={},
        )

        if not isinstance(
            robustness_summary,
            Mapping,
        ):
            robustness_summary = {}

        phase_10c_summary = first_value(
            phase_10c,
            (
                ("summary",),
                ("gate_summary",),
            ),
            default={},
        )

        if not isinstance(
            phase_10c_summary,
            Mapping,
        ):
            phase_10c_summary = {}

        phase_10c_passed = safe_bool(
            first_value(
                phase_10c,
                (
                    ("summary", "data_sufficient"),
                    ("summary", "strategy_promotion_allowed"),
                    ("data_sufficient",),
                    ("strategy_promotion_allowed",),
                    ("valid",),
                ),
                default=False,
            )
        )

        total_rows = safe_int(
            first_value(
                dataset_summary,
                (
                    ("total_rows",),
                    ("rows",),
                ),
                default=first_value(
                    dataset_manifest_summary,
                    (
                        ("total_rows",),
                        ("rows",),
                    ),
                    default=0,
                ),
            )
        )

        total_cycles = safe_int(
            first_value(
                dataset_summary,
                (
                    ("total_cycles",),
                    ("cycles",),
                ),
                default=first_value(
                    dataset_manifest_summary,
                    (
                        ("total_cycles",),
                        ("cycles",),
                    ),
                    default=0,
                ),
            )
        )

        verified_live_rows = safe_int(
            first_value(
                dataset_summary,
                (
                    ("verified_live_rows",),
                ),
                default=first_value(
                    feature_summary,
                    (
                        ("verified_live_rows",),
                    ),
                    default=0,
                ),
            )
        )

        verified_live_cycles = safe_int(
            first_value(
                dataset_summary,
                (
                    ("verified_live_cycles",),
                ),
                default=0,
            )
        )

        profitable_observations = safe_int(
            first_value(
                dataset_summary,
                (
                    ("profitable_observations",),
                    ("profitable_rows",),
                ),
                default=first_value(
                    feature_summary,
                    (
                        ("profitable_rows",),
                    ),
                    default=0,
                ),
            )
        )

        dataset_valid = safe_bool(
            first_value(
                dataset_validation,
                (
                    ("valid",),
                    ("summary", "valid"),
                ),
                default=False,
            )
        )

        feature_valid = safe_bool(
            first_value(
                feature_validation,
                (
                    ("valid",),
                    ("summary", "valid"),
                ),
                default=False,
            )
        )

        walk_promotion_allowed = safe_bool(
            first_value(
                walk_summary,
                (
                    ("promotion_allowed",),
                ),
                default=False,
            )
        )

        robustness_passed = safe_bool(
            first_value(
                robustness_summary,
                (
                    ("robustness_passed",),
                ),
                default=False,
            )
        )

        return {
            "phase_10c": {
                "report_present": bool(
                    phase_10c
                ),
                "passed": (
                    phase_10c_passed
                ),
                "raw_summary": dict(
                    phase_10c_summary
                ),
            },
            "institutional_dataset": {
                "valid": (
                    dataset_valid
                ),
                "total_rows": (
                    total_rows
                ),
                "total_cycles": (
                    total_cycles
                ),
                "verified_live_rows": (
                    verified_live_rows
                ),
                "verified_live_cycles": (
                    verified_live_cycles
                ),
                "profitable_observations": (
                    profitable_observations
                ),
                "raw_summary": dict(
                    dataset_summary
                ),
            },
            "feature_store": {
                "valid": (
                    feature_valid
                ),
                "rows": safe_int(
                    first_value(
                        feature_summary,
                        (
                            ("rows",),
                        ),
                        default=first_value(
                            feature_manifest_summary,
                            (
                                ("rows",),
                            ),
                            default=0,
                        ),
                    )
                ),
                "cycles": safe_int(
                    first_value(
                        feature_summary,
                        (
                            ("cycles",),
                        ),
                        default=first_value(
                            feature_manifest_summary,
                            (
                                ("cycles",),
                            ),
                            default=0,
                        ),
                    )
                ),
                "raw_summary": dict(
                    feature_summary
                ),
            },
            "walk_forward": {
                "promotion_allowed": (
                    walk_promotion_allowed
                ),
                "promotion_decision": (
                    first_value(
                        walk_summary,
                        (
                            ("promotion_decision",),
                        ),
                        default="UNKNOWN",
                    )
                ),
                "folds_completed": safe_int(
                    first_value(
                        walk_summary,
                        (
                            ("folds_completed",),
                        ),
                        default=0,
                    )
                ),
                "profitable_folds": safe_int(
                    first_value(
                        walk_summary,
                        (
                            ("profitable_folds",),
                        ),
                        default=0,
                    )
                ),
                "out_of_sample_trades": safe_int(
                    first_value(
                        walk_summary,
                        (
                            ("out_of_sample_trades",),
                        ),
                        default=0,
                    )
                ),
                "out_of_sample_profit_usd": safe_float(
                    first_value(
                        walk_summary,
                        (
                            ("out_of_sample_profit_usd",),
                        ),
                        default=0.0,
                    )
                ),
                "out_of_sample_profit_factor": safe_float(
                    first_value(
                        walk_summary,
                        (
                            ("out_of_sample_profit_factor",),
                        ),
                        default=0.0,
                    )
                ),
                "out_of_sample_maximum_drawdown_percent": safe_float(
                    first_value(
                        walk_summary,
                        (
                            ("out_of_sample_maximum_drawdown_percent",),
                        ),
                        default=0.0,
                    )
                ),
                "statistically_weak": safe_bool(
                    first_value(
                        walk_summary,
                        (
                            ("statistically_weak",),
                        ),
                        default=True,
                    )
                ),
                "weakness_reasons": first_value(
                    walk_summary,
                    (
                        ("weakness_reasons",),
                    ),
                    default=[],
                ),
            },
            "robustness": {
                "robustness_passed": (
                    robustness_passed
                ),
                "promotion_decision": first_value(
                    robustness_summary,
                    (
                        ("promotion_decision",),
                    ),
                    default="UNKNOWN",
                ),
                "input_trades": safe_int(
                    first_value(
                        robustness_summary,
                        (
                            ("input_trades",),
                        ),
                        default=0,
                    )
                ),
                "monte_carlo_paths": safe_int(
                    first_value(
                        robustness_summary,
                        (
                            ("monte_carlo_paths",),
                        ),
                        default=0,
                    )
                ),
                "loss_probability": safe_float(
                    first_value(
                        robustness_summary,
                        (
                            ("loss_probability",),
                        ),
                        default=1.0,
                    )
                ),
                "median_profit_usd": safe_float(
                    first_value(
                        robustness_summary,
                        (
                            ("median_profit_usd",),
                        ),
                        default=0.0,
                    )
                ),
                "profitable_scenarios": safe_int(
                    first_value(
                        robustness_summary,
                        (
                            ("profitable_scenarios",),
                        ),
                        default=0,
                    )
                ),
                "statistically_weak": safe_bool(
                    first_value(
                        robustness_summary,
                        (
                            ("statistically_weak",),
                        ),
                        default=True,
                    )
                ),
                "weakness_reasons": first_value(
                    robustness_summary,
                    (
                        ("weakness_reasons",),
                    ),
                    default=[],
                ),
            },
        }

    def _build_checks(
        self,
        evidence: Mapping[str, Any],
    ) -> list[GateCheck]:
        checks: list[GateCheck] = []

        phase_10c = evidence[
            "phase_10c"
        ]

        dataset = evidence[
            "institutional_dataset"
        ]

        feature_store = evidence[
            "feature_store"
        ]

        walk_forward = evidence[
            "walk_forward"
        ]

        robustness = evidence[
            "robustness"
        ]

        checks.append(
            GateCheck(
                category="FOUNDATION",
                name="PHASE_10C_DATA_SUFFICIENCY",
                passed=(
                    safe_bool(
                        phase_10c["passed"]
                    )
                    if self.configuration
                    .require_phase_10c_pass
                    else True
                ),
                blocking=(
                    self.configuration
                    .require_phase_10c_pass
                ),
                observed=safe_bool(
                    phase_10c["passed"]
                ),
                comparison="==",
                required=True,
                message=(
                    "Historical evidence remains below "
                    "the hard Phase 10C sufficiency gate."
                ),
                source=str(
                    self.configuration
                    .data_sufficiency_report
                ),
            )
        )

        checks.append(
            GateCheck(
                category="DATASET",
                name="INSTITUTIONAL_DATASET_VALID",
                passed=(
                    safe_bool(
                        dataset["valid"]
                    )
                    if self.configuration
                    .require_dataset_validation
                    else True
                ),
                blocking=(
                    self.configuration
                    .require_dataset_validation
                ),
                observed=safe_bool(
                    dataset["valid"]
                ),
                comparison="==",
                required=True,
                message=(
                    "The canonical institutional dataset "
                    "must pass validation."
                ),
                source=str(
                    self.configuration
                    .dataset_validation
                ),
            )
        )

        checks.append(
            GateCheck(
                category="DATASET",
                name="INSTITUTIONAL_ROWS",
                passed=(
                    safe_int(
                        dataset["total_rows"]
                    )
                    >= self.configuration
                    .minimum_institutional_rows
                ),
                blocking=True,
                observed=safe_int(
                    dataset["total_rows"]
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_institutional_rows
                ),
                message=(
                    "Collect more canonical institutional observations."
                ),
                source=str(
                    self.configuration
                    .dataset_manifest
                ),
            )
        )

        checks.append(
            GateCheck(
                category="DATASET",
                name="INSTITUTIONAL_CYCLES",
                passed=(
                    safe_int(
                        dataset["total_cycles"]
                    )
                    >= self.configuration
                    .minimum_institutional_cycles
                ),
                blocking=True,
                observed=safe_int(
                    dataset["total_cycles"]
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_institutional_cycles
                ),
                message=(
                    "Collect more complete institutional scanner cycles."
                ),
                source=str(
                    self.configuration
                    .dataset_manifest
                ),
            )
        )

        checks.append(
            GateCheck(
                category="DATASET",
                name="PROFITABLE_OBSERVATIONS",
                passed=(
                    safe_int(
                        dataset[
                            "profitable_observations"
                        ]
                    )
                    >= self.configuration
                    .minimum_profitable_observations
                ),
                blocking=True,
                observed=safe_int(
                    dataset[
                        "profitable_observations"
                    ]
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_profitable_observations
                ),
                message=(
                    "Collect more genuinely profitable observations."
                ),
                source=str(
                    self.configuration
                    .dataset_manifest
                ),
            )
        )

        checks.append(
            GateCheck(
                category="VERIFIED_LIVE",
                name="VERIFIED_LIVE_ROWS",
                passed=(
                    safe_int(
                        dataset[
                            "verified_live_rows"
                        ]
                    )
                    >= self.configuration
                    .minimum_verified_live_rows
                ),
                blocking=True,
                observed=safe_int(
                    dataset[
                        "verified_live_rows"
                    ]
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_verified_live_rows
                ),
                message=(
                    "Collect more verified-live feature rows."
                ),
                source=str(
                    self.configuration
                    .dataset_manifest
                ),
            )
        )

        checks.append(
            GateCheck(
                category="VERIFIED_LIVE",
                name="VERIFIED_LIVE_CYCLES",
                passed=(
                    safe_int(
                        dataset[
                            "verified_live_cycles"
                        ]
                    )
                    >= self.configuration
                    .minimum_verified_live_cycles
                ),
                blocking=True,
                observed=safe_int(
                    dataset[
                        "verified_live_cycles"
                    ]
                ),
                comparison=">=",
                required=(
                    self.configuration
                    .minimum_verified_live_cycles
                ),
                message=(
                    "Collect more verified-live scanner cycles."
                ),
                source=str(
                    self.configuration
                    .dataset_manifest
                ),
            )
        )

        checks.append(
            GateCheck(
                category="FEATURE_STORE",
                name="FEATURE_STORE_VALID",
                passed=(
                    safe_bool(
                        feature_store["valid"]
                    )
                    if self.configuration
                    .require_feature_store_validation
                    else True
                ),
                blocking=(
                    self.configuration
                    .require_feature_store_validation
                ),
                observed=safe_bool(
                    feature_store["valid"]
                ),
                comparison="==",
                required=True,
                message=(
                    "The institutional feature store "
                    "must pass validation."
                ),
                source=str(
                    self.configuration
                    .feature_validation
                ),
            )
        )

        checks.extend(
            [
                GateCheck(
                    category="WALK_FORWARD",
                    name="COMPLETED_FOLDS",
                    passed=(
                        safe_int(
                            walk_forward[
                                "folds_completed"
                            ]
                        )
                        >= self.configuration
                        .minimum_completed_folds
                    ),
                    blocking=True,
                    observed=safe_int(
                        walk_forward[
                            "folds_completed"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_completed_folds
                    ),
                    message=(
                        "Enough chronological walk-forward "
                        "folds must complete."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
                GateCheck(
                    category="WALK_FORWARD",
                    name="PROFITABLE_FOLDS",
                    passed=(
                        safe_int(
                            walk_forward[
                                "profitable_folds"
                            ]
                        )
                        >= self.configuration
                        .minimum_profitable_folds
                    ),
                    blocking=True,
                    observed=safe_int(
                        walk_forward[
                            "profitable_folds"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_profitable_folds
                    ),
                    message=(
                        "Multiple walk-forward folds "
                        "must remain profitable."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
                GateCheck(
                    category="WALK_FORWARD",
                    name="OUT_OF_SAMPLE_TRADES",
                    passed=(
                        safe_int(
                            walk_forward[
                                "out_of_sample_trades"
                            ]
                        )
                        >= self.configuration
                        .minimum_oos_trades
                    ),
                    blocking=True,
                    observed=safe_int(
                        walk_forward[
                            "out_of_sample_trades"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_oos_trades
                    ),
                    message=(
                        "Collect more out-of-sample trade examples."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
                GateCheck(
                    category="WALK_FORWARD",
                    name="OUT_OF_SAMPLE_PROFIT",
                    passed=(
                        safe_float(
                            walk_forward[
                                "out_of_sample_profit_usd"
                            ]
                        )
                        >= self.configuration
                        .minimum_oos_profit_usd
                    ),
                    blocking=True,
                    observed=safe_float(
                        walk_forward[
                            "out_of_sample_profit_usd"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_oos_profit_usd
                    ),
                    message=(
                        "Out-of-sample profit must remain positive."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
                GateCheck(
                    category="WALK_FORWARD",
                    name="OUT_OF_SAMPLE_PROFIT_FACTOR",
                    passed=(
                        safe_float(
                            walk_forward[
                                "out_of_sample_profit_factor"
                            ]
                        )
                        >= self.configuration
                        .minimum_oos_profit_factor
                    ),
                    blocking=True,
                    observed=safe_float(
                        walk_forward[
                            "out_of_sample_profit_factor"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_oos_profit_factor
                    ),
                    message=(
                        "Out-of-sample profit factor "
                        "must exceed the promotion threshold."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
                GateCheck(
                    category="WALK_FORWARD",
                    name="OUT_OF_SAMPLE_DRAWDOWN",
                    passed=(
                        safe_float(
                            walk_forward[
                                "out_of_sample_maximum_drawdown_percent"
                            ]
                        )
                        <= self.configuration
                        .maximum_oos_drawdown_percent
                    ),
                    blocking=True,
                    observed=safe_float(
                        walk_forward[
                            "out_of_sample_maximum_drawdown_percent"
                        ]
                    ),
                    comparison="<=",
                    required=(
                        self.configuration
                        .maximum_oos_drawdown_percent
                    ),
                    message=(
                        "Out-of-sample drawdown must remain controlled."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
                GateCheck(
                    category="WALK_FORWARD",
                    name="WALK_FORWARD_PROMOTION",
                    passed=(
                        safe_bool(
                            walk_forward[
                                "promotion_allowed"
                            ]
                        )
                        if self.configuration
                        .require_walk_forward_promotion
                        else True
                    ),
                    blocking=(
                        self.configuration
                        .require_walk_forward_promotion
                    ),
                    observed=safe_bool(
                        walk_forward[
                            "promotion_allowed"
                        ]
                    ),
                    comparison="==",
                    required=True,
                    message=(
                        "Phase 13D walk-forward promotion remains blocked."
                    ),
                    source=str(
                        self.configuration
                        .walk_forward_report
                    ),
                ),
            ]
        )

        checks.extend(
            [
                GateCheck(
                    category="ROBUSTNESS",
                    name="MONTE_CARLO_PATHS",
                    passed=(
                        safe_int(
                            robustness[
                                "monte_carlo_paths"
                            ]
                        )
                        >= self.configuration
                        .minimum_monte_carlo_paths
                    ),
                    blocking=True,
                    observed=safe_int(
                        robustness[
                            "monte_carlo_paths"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_monte_carlo_paths
                    ),
                    message=(
                        "Enough Monte Carlo paths must complete."
                    ),
                    source=str(
                        self.configuration
                        .robustness_report
                    ),
                ),
                GateCheck(
                    category="ROBUSTNESS",
                    name="MONTE_CARLO_LOSS_PROBABILITY",
                    passed=(
                        safe_float(
                            robustness[
                                "loss_probability"
                            ]
                        )
                        <= self.configuration
                        .maximum_monte_carlo_loss_probability
                    ),
                    blocking=True,
                    observed=safe_float(
                        robustness[
                            "loss_probability"
                        ]
                    ),
                    comparison="<=",
                    required=(
                        self.configuration
                        .maximum_monte_carlo_loss_probability
                    ),
                    message=(
                        "Monte Carlo loss probability "
                        "must remain below the gate."
                    ),
                    source=str(
                        self.configuration
                        .robustness_report
                    ),
                ),
                GateCheck(
                    category="ROBUSTNESS",
                    name="MEDIAN_STRESSED_PROFIT",
                    passed=(
                        safe_float(
                            robustness[
                                "median_profit_usd"
                            ]
                        )
                        >= self.configuration
                        .minimum_median_stressed_profit_usd
                    ),
                    blocking=True,
                    observed=safe_float(
                        robustness[
                            "median_profit_usd"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_median_stressed_profit_usd
                    ),
                    message=(
                        "Median stressed profit must remain positive."
                    ),
                    source=str(
                        self.configuration
                        .robustness_report
                    ),
                ),
                GateCheck(
                    category="ROBUSTNESS",
                    name="PROFITABLE_STRESS_SCENARIOS",
                    passed=(
                        safe_int(
                            robustness[
                                "profitable_scenarios"
                            ]
                        )
                        >= self.configuration
                        .minimum_profitable_stress_scenarios
                    ),
                    blocking=True,
                    observed=safe_int(
                        robustness[
                            "profitable_scenarios"
                        ]
                    ),
                    comparison=">=",
                    required=(
                        self.configuration
                        .minimum_profitable_stress_scenarios
                    ),
                    message=(
                        "Multiple deterministic stress "
                        "scenarios must remain profitable."
                    ),
                    source=str(
                        self.configuration
                        .robustness_report
                    ),
                ),
                GateCheck(
                    category="ROBUSTNESS",
                    name="ROBUSTNESS_PROMOTION",
                    passed=(
                        safe_bool(
                            robustness[
                                "robustness_passed"
                            ]
                        )
                        if self.configuration
                        .require_robustness_pass
                        else True
                    ),
                    blocking=(
                        self.configuration
                        .require_robustness_pass
                    ),
                    observed=safe_bool(
                        robustness[
                            "robustness_passed"
                        ]
                    ),
                    comparison="==",
                    required=True,
                    message=(
                        "Phase 13E robustness validation must pass."
                    ),
                    source=str(
                        self.configuration
                        .robustness_report
                    ),
                ),
            ]
        )

        return checks


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

    fieldnames: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def executive_summary_text(
    summary: Mapping[str, Any],
    checks: Sequence[GateCheck],
) -> str:
    evidence = summary["evidence"]

    lines = [
        "Phase 13F — Institutional Evidence and Promotion Gate",
        "=" * 80,
        "",
        f"Final decision: {summary['final_decision']}",
        f"Research promotion allowed: {summary['research_promotion_allowed']}",
        f"Live readiness allowed: {summary['live_readiness_allowed']}",
        "",
        "Evidence",
        "-" * 80,
        (
            "Institutional rows / cycles: "
            f"{evidence['institutional_dataset']['total_rows']} / "
            f"{evidence['institutional_dataset']['total_cycles']}"
        ),
        (
            "Verified-live rows / cycles: "
            f"{evidence['institutional_dataset']['verified_live_rows']} / "
            f"{evidence['institutional_dataset']['verified_live_cycles']}"
        ),
        (
            "Profitable observations: "
            f"{evidence['institutional_dataset']['profitable_observations']}"
        ),
        (
            "Walk-forward folds / profitable folds: "
            f"{evidence['walk_forward']['folds_completed']} / "
            f"{evidence['walk_forward']['profitable_folds']}"
        ),
        (
            "Out-of-sample trades / profit: "
            f"{evidence['walk_forward']['out_of_sample_trades']} / "
            f"${evidence['walk_forward']['out_of_sample_profit_usd']:.6f}"
        ),
        (
            "Monte Carlo paths / loss probability: "
            f"{evidence['robustness']['monte_carlo_paths']} / "
            f"{evidence['robustness']['loss_probability'] * 100:.2f}%"
        ),
        "",
        "Blocking reasons",
        "-" * 80,
    ]

    blocking = [
        check
        for check in checks
        if check.blocking
        and not check.passed
    ]

    if blocking:
        for check in blocking:
            lines.append(
                f"- {check.name}: {check.message} "
                f"(observed={check.observed}, "
                f"required {check.comparison} {check.required})"
            )
    else:
        lines.append(
            "- None. All blocking evidence gates passed."
        )

    lines.extend(
        [
            "",
            "Governance",
            "-" * 80,
            "Live execution remains disabled by this report.",
            "Wallet connection is not authorized by this report.",
            "No automatic strategy promotion is performed.",
            "",
        ]
    )

    return "\n".join(lines)


def export_results(
    *,
    summary: Mapping[str, Any],
    checks: Sequence[GateCheck],
    evidence: Mapping[str, Any],
    configuration: GateConfiguration,
) -> tuple[Path, ...]:
    output = configuration.output_directory

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    evidence_report = (
        output
        / EVIDENCE_REPORT_JSON
    )

    decision_report = (
        output
        / PROMOTION_DECISION_JSON
    )

    checks_csv = (
        output
        / GATE_CHECKS_CSV
    )

    manifest = (
        output
        / EVIDENCE_MANIFEST_JSON
    )

    executive_summary = (
        output
        / EXECUTIVE_SUMMARY_TXT
    )

    destinations = (
        evidence_report,
        decision_report,
        checks_csv,
        manifest,
        executive_summary,
    )

    if not configuration.overwrite:
        existing = [
            path
            for path in destinations
            if path.exists()
        ]

        if existing:
            raise InstitutionalPromotionGateError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    evidence_report.write_text(
        json.dumps(
            {
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "generated_at": (
                    utc_now_text()
                ),
                "evidence": dict(
                    evidence
                ),
                "checks": [
                    check.to_dict()
                    for check in checks
                ],
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    decision_report.write_text(
        json.dumps(
            {
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "generated_at": (
                    utc_now_text()
                ),
                "final_decision": (
                    summary[
                        "final_decision"
                    ]
                ),
                "research_promotion_allowed": (
                    summary[
                        "research_promotion_allowed"
                    ]
                ),
                "live_readiness_allowed": (
                    summary[
                        "live_readiness_allowed"
                    ]
                ),
                "blocking_failures": (
                    summary[
                        "blocking_failures"
                    ]
                ),
                "blocking_reasons": (
                    summary[
                        "blocking_reasons"
                    ]
                ),
                "live_execution_enabled": (
                    False
                ),
                "wallet_connection_authorized": (
                    False
                ),
                "automatic_promotion_enabled": (
                    False
                ),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    write_csv(
        checks_csv,
        [
            check.to_dict()
            for check in checks
        ],
    )

    executive_summary.write_text(
        executive_summary_text(
            summary,
            checks,
        ),
        encoding="utf-8",
    )

    source_paths = {
        "phase_10c": (
            configuration
            .data_sufficiency_report
        ),
        "dataset_validation": (
            configuration
            .dataset_validation
        ),
        "dataset_manifest": (
            configuration
            .dataset_manifest
        ),
        "feature_validation": (
            configuration
            .feature_validation
        ),
        "feature_manifest": (
            configuration
            .feature_manifest
        ),
        "walk_forward_report": (
            configuration
            .walk_forward_report
        ),
        "robustness_report": (
            configuration
            .robustness_report
        ),
    }

    source_metadata: dict[
        str,
        Any,
    ] = {}

    for name, path in source_paths.items():
        source_metadata[name] = {
            "path": str(path),
            "exists": path.exists(),
            "bytes": (
                path.stat().st_size
                if path.exists()
                else None
            ),
            "sha256": (
                sha256_file(path)
                if path.exists()
                else None
            ),
        }

    output_metadata: dict[
        str,
        Any,
    ] = {}

    for path in (
        evidence_report,
        decision_report,
        checks_csv,
        executive_summary,
    ):
        output_metadata[path.name] = {
            "path": str(path),
            "bytes": (
                path.stat().st_size
            ),
            "sha256": (
                sha256_file(path)
            ),
        }

    manifest.write_text(
        json.dumps(
            {
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "generated_at": (
                    utc_now_text()
                ),
                "summary": {
                    key: value
                    for key, value
                    in summary.items()
                    if key != "evidence"
                },
                "source_evidence": (
                    source_metadata
                ),
                "outputs": (
                    output_metadata
                ),
                "governance": {
                    "live_execution_enabled": False,
                    "wallet_connection_authorized": False,
                    "automatic_promotion_enabled": False,
                    "manual_security_review_required": True,
                    "tiny_live_pilot_requires_separate_approval": True,
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    return destinations


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Run Phase 13F institutional "
            "evidence and promotion gate."
        )
    )

    result.add_argument(
        "--data-sufficiency-report",
        default=str(
            DEFAULT_DATA_SUFFICIENCY_REPORT
        ),
    )

    result.add_argument(
        "--dataset-validation",
        default=str(
            DEFAULT_DATASET_VALIDATION
        ),
    )

    result.add_argument(
        "--dataset-manifest",
        default=str(
            DEFAULT_DATASET_MANIFEST
        ),
    )

    result.add_argument(
        "--feature-validation",
        default=str(
            DEFAULT_FEATURE_VALIDATION
        ),
    )

    result.add_argument(
        "--feature-manifest",
        default=str(
            DEFAULT_FEATURE_MANIFEST
        ),
    )

    result.add_argument(
        "--walk-forward-report",
        default=str(
            DEFAULT_WALK_FORWARD_REPORT
        ),
    )

    result.add_argument(
        "--robustness-report",
        default=str(
            DEFAULT_ROBUSTNESS_REPORT
        ),
    )

    result.add_argument(
        "--output-directory",
        default=str(
            DEFAULT_OUTPUT_DIRECTORY
        ),
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
    argv: Sequence[str] | None = None,
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

    configuration = GateConfiguration(
        data_sufficiency_report=Path(
            args.data_sufficiency_report
        ),
        dataset_validation=Path(
            args.dataset_validation
        ),
        dataset_manifest=Path(
            args.dataset_manifest
        ),
        feature_validation=Path(
            args.feature_validation
        ),
        feature_manifest=Path(
            args.feature_manifest
        ),
        walk_forward_report=Path(
            args.walk_forward_report
        ),
        robustness_report=Path(
            args.robustness_report
        ),
        output_directory=Path(
            args.output_directory
        ),
        overwrite=(
            not args.no_overwrite
        ),
    )

    try:
        (
            summary,
            checks,
            evidence,
        ) = InstitutionalEvidenceGate(
            configuration
        ).run()

        output_paths = export_results(
            summary=summary,
            checks=checks,
            evidence=evidence,
            configuration=configuration,
        )

    except (
        InstitutionalPromotionGateError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 13F — Institutional "
        "Evidence and Promotion Gate"
    )

    print("=" * 80)

    print(
        "Operating mode: "
        f"{summary['operating_mode']}"
    )

    print(
        "Final decision: "
        f"{summary['final_decision']}"
    )

    print()

    print("Gate Summary")
    print("-" * 80)

    print(
        f"Checks run: "
        f"{summary['checks_run']}"
    )

    print(
        f"Checks passed: "
        f"{summary['checks_passed']}"
    )

    print(
        f"Checks failed: "
        f"{summary['checks_failed']}"
    )

    print(
        f"Blocking failures: "
        f"{summary['blocking_failures']}"
    )

    print(
        "Research promotion allowed: "
        f"{summary['research_promotion_allowed']}"
    )

    print(
        "Live readiness allowed: "
        f"{summary['live_readiness_allowed']}"
    )

    print()

    print("Evidence")
    print("-" * 80)

    institutional = summary[
        "evidence"
    ][
        "institutional_dataset"
    ]

    walk = summary[
        "evidence"
    ][
        "walk_forward"
    ]

    robustness = summary[
        "evidence"
    ][
        "robustness"
    ]

    print(
        "Rows / cycles / profitable observations: "
        f"{institutional['total_rows']} / "
        f"{institutional['total_cycles']} / "
        f"{institutional['profitable_observations']}"
    )

    print(
        "Verified-live rows / cycles: "
        f"{institutional['verified_live_rows']} / "
        f"{institutional['verified_live_cycles']}"
    )

    print(
        "Walk-forward folds / profitable folds: "
        f"{walk['folds_completed']} / "
        f"{walk['profitable_folds']}"
    )

    print(
        "Out-of-sample trades / profit: "
        f"{walk['out_of_sample_trades']} / "
        f"${walk['out_of_sample_profit_usd']:.6f}"
    )

    print(
        "Monte Carlo paths / loss probability: "
        f"{robustness['monte_carlo_paths']} / "
        f"{robustness['loss_probability'] * 100:.2f}%"
    )

    print()

    print("Gate Checks")
    print("-" * 80)

    for check in checks:
        print(
            f"{'PASS' if check.passed else 'FAIL'} | "
            f"{check.name:34} | "
            f"observed={check.observed} "
            f"{check.comparison} "
            f"required={check.required}"
        )

        if not check.passed:
            print(
                f"       {check.message}"
            )

    if summary[
        "blocking_reasons"
    ]:
        print()

        print("Blocking Reasons")
        print("-" * 80)

        for reason in summary[
            "blocking_reasons"
        ]:
            print(
                f"  - {reason}"
            )

    print()

    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )