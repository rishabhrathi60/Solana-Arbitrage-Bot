"""
Phase 14F — Consolidated Post-Filter Readiness Gate

Combines Phase 13F and Phases 14A–14E into one final post-filter readiness
decision. This phase distinguishes:

- security and resilience controls that passed;
- execution-aware candidates that remain profitable;
- statistical and independence requirements that remain insufficient.

Inputs
------
research/institutional_promotion_gate/institutional_promotion_decision.json
execution/shadow_results/shadow_execution_report.json
execution/shadow_diagnostics/shadow_execution_diagnostics_report.json
execution/resilience_results/operational_resilience_report.json
execution/live_readiness_audit/live_readiness_audit_report.json
execution/execution_aware_filter/execution_aware_report.json

Outputs
-------
execution/post_filter_readiness/
    post_filter_readiness_gate_checks.csv
    post_filter_evidence_summary.csv
    post_filter_readiness_report.json
    post_filter_readiness_manifest.json
    executive_summary.txt

Safety
------
Audit and governance only. No wallet access, signing, broadcasting, live
execution, runtime configuration changes, or automatic promotion.
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

SCHEMA_VERSION = "14F.1.0"
OPERATING_MODE = "POST_FILTER_GOVERNANCE_ONLY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PHASE_13F = (
    PROJECT_ROOT
    / "research"
    / "institutional_promotion_gate"
    / "institutional_promotion_decision.json"
)

DEFAULT_PHASE_14A = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_report.json"
)

DEFAULT_PHASE_14B = (
    PROJECT_ROOT
    / "execution"
    / "shadow_diagnostics"
    / "shadow_execution_diagnostics_report.json"
)

DEFAULT_PHASE_14C = (
    PROJECT_ROOT
    / "execution"
    / "resilience_results"
    / "operational_resilience_report.json"
)

DEFAULT_PHASE_14D = (
    PROJECT_ROOT
    / "execution"
    / "live_readiness_audit"
    / "live_readiness_audit_report.json"
)

DEFAULT_PHASE_14E = (
    PROJECT_ROOT
    / "execution"
    / "execution_aware_filter"
    / "execution_aware_report.json"
)

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "execution"
    / "post_filter_readiness"
)

GATES_CSV = "post_filter_readiness_gate_checks.csv"
EVIDENCE_CSV = "post_filter_evidence_summary.csv"
REPORT_JSON = "post_filter_readiness_report.json"
MANIFEST_JSON = "post_filter_readiness_manifest.json"
SUMMARY_TXT = "executive_summary.txt"


class PostFilterReadinessError(RuntimeError):
    """Base exception for Phase 14F failures."""


@dataclass(frozen=True, slots=True)
class Configuration:
    phase_13f_report: Path = DEFAULT_PHASE_13F
    phase_14a_report: Path = DEFAULT_PHASE_14A
    phase_14b_report: Path = DEFAULT_PHASE_14B
    phase_14c_report: Path = DEFAULT_PHASE_14C
    phase_14d_report: Path = DEFAULT_PHASE_14D
    phase_14e_report: Path = DEFAULT_PHASE_14E
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    minimum_institutional_rows: int = 10_000
    minimum_institutional_cycles: int = 100
    minimum_profitable_observations: int = 100
    minimum_verified_live_rows: int = 1_000
    minimum_verified_live_cycles: int = 20
    minimum_out_of_sample_trades: int = 50
    minimum_filtered_source_trades: int = 3
    minimum_filtered_profitable_confirmation_rate: float = 0.60
    minimum_filtered_median_profit_usd: float = 0.0
    maximum_filtered_single_trade_concentration: float = 0.60

    require_phase_13f: bool = True
    require_phase_14a: bool = True
    require_phase_14b: bool = True
    require_phase_14c: bool = True
    require_phase_14d: bool = True
    require_phase_14e: bool = True

    def validate(self) -> None:
        for name in (
            "minimum_institutional_rows",
            "minimum_institutional_cycles",
            "minimum_profitable_observations",
            "minimum_verified_live_rows",
            "minimum_verified_live_cycles",
            "minimum_out_of_sample_trades",
            "minimum_filtered_source_trades",
        ):
            if int(getattr(self, name)) <= 0:
                raise PostFilterReadinessError(
                    f"{name} must be positive."
                )

        for name in (
            "minimum_filtered_profitable_confirmation_rate",
            "minimum_filtered_median_profit_usd",
            "maximum_filtered_single_trade_concentration",
        ):
            value = float(getattr(self, name))

            if not math.isfinite(value):
                raise PostFilterReadinessError(
                    f"{name} must be finite."
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


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat(timespec="milliseconds")


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


def safe_int(
    value: Any,
    default: int = 0,
) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(
    value: Any,
    default: float = 0.0,
) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    return numeric if math.isfinite(numeric) else default


def load_json(
    path: Path,
    *,
    required: bool,
) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise PostFilterReadinessError(
                f"Required upstream report is missing: {path}"
            )

        return {}

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise PostFilterReadinessError(
            f"Expected JSON object: {path}"
        )

    return payload


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    if not rows:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    fields: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)

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
        writer.writerows(rows)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


class PostFilterReadinessGate:
    def __init__(self, configuration: Configuration) -> None:
        self.config = configuration
        self.config.validate()

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        list[GateCheck],
        list[dict[str, Any]],
    ]:
        phase_13f = load_json(
            self.config.phase_13f_report,
            required=self.config.require_phase_13f,
        )

        phase_14a = load_json(
            self.config.phase_14a_report,
            required=self.config.require_phase_14a,
        )

        phase_14b = load_json(
            self.config.phase_14b_report,
            required=self.config.require_phase_14b,
        )

        phase_14c = load_json(
            self.config.phase_14c_report,
            required=self.config.require_phase_14c,
        )

        phase_14d = load_json(
            self.config.phase_14d_report,
            required=self.config.require_phase_14d,
        )

        phase_14e = load_json(
            self.config.phase_14e_report,
            required=self.config.require_phase_14e,
        )

        phase_14a_summary = phase_14a.get("summary", {})
        phase_14b_summary = phase_14b.get("summary", {})
        phase_14c_summary = phase_14c.get("summary", {})
        phase_14d_summary = phase_14d.get("summary", {})
        phase_14e_summary = phase_14e.get("summary", {})

        institutional_rows = safe_int(
            phase_13f.get(
                "evidence",
                {},
            ).get(
                "institutional_dataset",
                {},
            ).get(
                "total_rows"
            )
        )

        institutional_cycles = safe_int(
            phase_13f.get(
                "evidence",
                {},
            ).get(
                "institutional_dataset",
                {},
            ).get(
                "total_cycles"
            )
        )

        profitable_observations = safe_int(
            phase_13f.get(
                "evidence",
                {},
            ).get(
                "institutional_dataset",
                {},
            ).get(
                "profitable_observations"
            )
        )

        verified_live_rows = safe_int(
            phase_13f.get(
                "evidence",
                {},
            ).get(
                "institutional_dataset",
                {},
            ).get(
                "verified_live_rows"
            )
        )

        verified_live_cycles = safe_int(
            phase_13f.get(
                "evidence",
                {},
            ).get(
                "institutional_dataset",
                {},
            ).get(
                "verified_live_cycles"
            )
        )

        out_of_sample_trades = safe_int(
            phase_13f.get(
                "evidence",
                {},
            ).get(
                "walk_forward",
                {},
            ).get(
                "out_of_sample_trades"
            )
        )

        accepted_trades = safe_int(
            phase_14e_summary.get(
                "accepted_trades"
            )
        )

        filtered_profitable_rate = safe_float(
            phase_14e_summary.get(
                "profitable_confirmation_rate"
            )
        )

        filtered_median_profit = safe_float(
            phase_14e_summary.get(
                "median_realized_profit_usd"
            )
        )

        filtered_concentration = safe_float(
            phase_14e_summary.get(
                "single_trade_profit_concentration"
            )
        )

        gates = [
            GateCheck(
                category="UPSTREAM",
                name="PHASE_13F_LIVE_READINESS",
                passed=safe_bool(
                    phase_13f.get(
                        "live_readiness_allowed"
                    )
                ),
                blocking=True,
                observed=safe_bool(
                    phase_13f.get(
                        "live_readiness_allowed"
                    )
                ),
                comparison="==",
                required=True,
                message=(
                    "Phase 13F institutional live readiness remains blocked."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="DATA_SUFFICIENCY",
                name="INSTITUTIONAL_ROWS",
                passed=(
                    institutional_rows
                    >= self.config.minimum_institutional_rows
                ),
                blocking=True,
                observed=institutional_rows,
                comparison=">=",
                required=self.config.minimum_institutional_rows,
                message=(
                    "Collect more institutional observations."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="DATA_SUFFICIENCY",
                name="INSTITUTIONAL_CYCLES",
                passed=(
                    institutional_cycles
                    >= self.config.minimum_institutional_cycles
                ),
                blocking=True,
                observed=institutional_cycles,
                comparison=">=",
                required=self.config.minimum_institutional_cycles,
                message=(
                    "Collect more complete institutional scanner cycles."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="DATA_SUFFICIENCY",
                name="PROFITABLE_OBSERVATIONS",
                passed=(
                    profitable_observations
                    >= self.config.minimum_profitable_observations
                ),
                blocking=True,
                observed=profitable_observations,
                comparison=">=",
                required=self.config.minimum_profitable_observations,
                message=(
                    "Collect more genuinely profitable observations."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="DATA_SUFFICIENCY",
                name="VERIFIED_LIVE_ROWS",
                passed=(
                    verified_live_rows
                    >= self.config.minimum_verified_live_rows
                ),
                blocking=True,
                observed=verified_live_rows,
                comparison=">=",
                required=self.config.minimum_verified_live_rows,
                message=(
                    "Collect more verified-live observations."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="DATA_SUFFICIENCY",
                name="VERIFIED_LIVE_CYCLES",
                passed=(
                    verified_live_cycles
                    >= self.config.minimum_verified_live_cycles
                ),
                blocking=True,
                observed=verified_live_cycles,
                comparison=">=",
                required=self.config.minimum_verified_live_cycles,
                message=(
                    "Collect more verified-live scanner cycles."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="INDEPENDENCE",
                name="OUT_OF_SAMPLE_TRADES",
                passed=(
                    out_of_sample_trades
                    >= self.config.minimum_out_of_sample_trades
                ),
                blocking=True,
                observed=out_of_sample_trades,
                comparison=">=",
                required=self.config.minimum_out_of_sample_trades,
                message=(
                    "Collect more independent out-of-sample trades."
                ),
                source=str(
                    self.config.phase_13f_report
                ),
            ),
            GateCheck(
                category="SHADOW_EXECUTION",
                name="PHASE_14A_OPERATIONAL_GATE",
                passed=safe_bool(
                    phase_14a_summary.get(
                        "operational_gate_passed"
                    )
                ),
                blocking=True,
                observed=safe_bool(
                    phase_14a_summary.get(
                        "operational_gate_passed"
                    )
                ),
                comparison="==",
                required=True,
                message=(
                    "Phase 14A operational shadow gate remains blocked."
                ),
                source=str(
                    self.config.phase_14a_report
                ),
            ),
            GateCheck(
                category="EDGE_DIAGNOSTICS",
                name="PHASE_14B_DIAGNOSTICS_GATE",
                passed=safe_bool(
                    phase_14b_summary.get(
                        "diagnostics_passed"
                    )
                ),
                blocking=True,
                observed=safe_bool(
                    phase_14b_summary.get(
                        "diagnostics_passed"
                    )
                ),
                comparison="==",
                required=True,
                message=(
                    "Phase 14B edge diagnostics remain blocked."
                ),
                source=str(
                    self.config.phase_14b_report
                ),
            ),
            GateCheck(
                category="RESILIENCE",
                name="PHASE_14C_RESILIENCE_GATE",
                passed=safe_bool(
                    phase_14c_summary.get(
                        "resilience_passed"
                    )
                ),
                blocking=True,
                observed=safe_bool(
                    phase_14c_summary.get(
                        "resilience_passed"
                    )
                ),
                comparison="==",
                required=True,
                message=(
                    "Phase 14C resilience validation must pass."
                ),
                source=str(
                    self.config.phase_14c_report
                ),
            ),
            GateCheck(
                category="SECURITY",
                name="PHASE_14D_SECURITY_GATE",
                passed=safe_bool(
                    phase_14d_summary.get(
                        "security_audit_passed"
                    )
                ),
                blocking=True,
                observed=safe_bool(
                    phase_14d_summary.get(
                        "security_audit_passed"
                    )
                ),
                comparison="==",
                required=True,
                message=(
                    "Phase 14D security audit remains blocked."
                ),
                source=str(
                    self.config.phase_14d_report
                ),
            ),
            GateCheck(
                category="POST_FILTER",
                name="FILTERED_SOURCE_TRADES",
                passed=(
                    accepted_trades
                    >= self.config.minimum_filtered_source_trades
                ),
                blocking=True,
                observed=accepted_trades,
                comparison=">=",
                required=self.config.minimum_filtered_source_trades,
                message=(
                    "Too few independent trades survive the "
                    "execution-aware filter."
                ),
                source=str(
                    self.config.phase_14e_report
                ),
            ),
            GateCheck(
                category="POST_FILTER",
                name="FILTERED_PROFITABLE_CONFIRMATION_RATE",
                passed=(
                    filtered_profitable_rate
                    >= self.config
                    .minimum_filtered_profitable_confirmation_rate
                ),
                blocking=True,
                observed=filtered_profitable_rate,
                comparison=">=",
                required=(
                    self.config
                    .minimum_filtered_profitable_confirmation_rate
                ),
                message=(
                    "Filtered profitable confirmation rate "
                    "is below the gate."
                ),
                source=str(
                    self.config.phase_14e_report
                ),
            ),
            GateCheck(
                category="POST_FILTER",
                name="FILTERED_MEDIAN_REALIZED_PROFIT",
                passed=(
                    filtered_median_profit
                    >= self.config
                    .minimum_filtered_median_profit_usd
                ),
                blocking=True,
                observed=filtered_median_profit,
                comparison=">=",
                required=(
                    self.config
                    .minimum_filtered_median_profit_usd
                ),
                message=(
                    "Filtered median realized profit remains negative."
                ),
                source=str(
                    self.config.phase_14e_report
                ),
            ),
            GateCheck(
                category="POST_FILTER",
                name="FILTERED_PROFIT_CONCENTRATION",
                passed=(
                    filtered_concentration
                    <= self.config
                    .maximum_filtered_single_trade_concentration
                ),
                blocking=True,
                observed=filtered_concentration,
                comparison="<=",
                required=(
                    self.config
                    .maximum_filtered_single_trade_concentration
                ),
                message=(
                    "Filtered profits remain too concentrated "
                    "in one source trade."
                ),
                source=str(
                    self.config.phase_14e_report
                ),
            ),
            GateCheck(
                category="POST_FILTER",
                name="PHASE_14E_FILTER_GATE",
                passed=safe_bool(
                    phase_14e_summary.get(
                        "filter_passed"
                    )
                ),
                blocking=True,
                observed=safe_bool(
                    phase_14e_summary.get(
                        "filter_passed"
                    )
                ),
                comparison="==",
                required=True,
                message=(
                    "Phase 14E execution-aware filter remains blocked."
                ),
                source=str(
                    self.config.phase_14e_report
                ),
            ),
            GateCheck(
                category="SAFETY",
                name="NO_LIVE_EXECUTION_STATE",
                passed=(
                    not safe_bool(
                        phase_14a_summary.get(
                            "live_execution_enabled"
                        )
                    )
                    and not safe_bool(
                        phase_14d_summary.get(
                            "safety",
                            {},
                        ).get(
                            "live_execution_enabled"
                        )
                    )
                ),
                blocking=True,
                observed=0,
                comparison="==",
                required=0,
                message=(
                    "An upstream report indicates live execution is enabled."
                ),
                source=(
                    f"{self.config.phase_14a_report}; "
                    f"{self.config.phase_14d_report}"
                ),
            ),
        ]

        blocking_failures = [
            gate
            for gate in gates
            if gate.blocking
            and not gate.passed
        ]

        operational_controls_passed = all(
            gate.passed
            for gate in gates
            if gate.category in {
                "RESILIENCE",
                "SECURITY",
                "SAFETY",
            }
        )

        post_filter_economics_passed = all(
            gate.passed
            for gate in gates
            if gate.category == "POST_FILTER"
            and gate.name not in {
                "FILTERED_SOURCE_TRADES",
                "FILTERED_PROFIT_CONCENTRATION",
            }
        )

        evidence_sufficiency_passed = all(
            gate.passed
            for gate in gates
            if gate.category in {
                "DATA_SUFFICIENCY",
                "INDEPENDENCE",
            }
        )

        live_readiness_allowed = not blocking_failures

        evidence_rows = [
            {
                "metric": "institutional_rows",
                "value": institutional_rows,
                "source": str(
                    self.config.phase_13f_report
                ),
            },
            {
                "metric": "institutional_cycles",
                "value": institutional_cycles,
                "source": str(
                    self.config.phase_13f_report
                ),
            },
            {
                "metric": "profitable_observations",
                "value": profitable_observations,
                "source": str(
                    self.config.phase_13f_report
                ),
            },
            {
                "metric": "verified_live_rows",
                "value": verified_live_rows,
                "source": str(
                    self.config.phase_13f_report
                ),
            },
            {
                "metric": "verified_live_cycles",
                "value": verified_live_cycles,
                "source": str(
                    self.config.phase_13f_report
                ),
            },
            {
                "metric": "out_of_sample_trades",
                "value": out_of_sample_trades,
                "source": str(
                    self.config.phase_13f_report
                ),
            },
            {
                "metric": "accepted_filtered_trades",
                "value": accepted_trades,
                "source": str(
                    self.config.phase_14e_report
                ),
            },
            {
                "metric": "filtered_profitable_confirmation_rate",
                "value": filtered_profitable_rate,
                "source": str(
                    self.config.phase_14e_report
                ),
            },
            {
                "metric": "filtered_median_realized_profit_usd",
                "value": filtered_median_profit,
                "source": str(
                    self.config.phase_14e_report
                ),
            },
            {
                "metric": "filtered_single_trade_concentration",
                "value": filtered_concentration,
                "source": str(
                    self.config.phase_14e_report
                ),
            },
        ]

        summary = {
            "generated_at": utc_now(),
            "schema_version": SCHEMA_VERSION,
            "operating_mode": OPERATING_MODE,
            "gate_checks": len(gates),
            "gate_checks_passed": sum(
                gate.passed
                for gate in gates
            ),
            "blocking_failures": len(
                blocking_failures
            ),
            "blocking_reasons": [
                gate.message
                for gate in blocking_failures
            ],
            "operational_controls_passed": (
                operational_controls_passed
            ),
            "post_filter_economics_passed": (
                post_filter_economics_passed
            ),
            "evidence_sufficiency_passed": (
                evidence_sufficiency_passed
            ),
            "post_filter_readiness_allowed": (
                live_readiness_allowed
            ),
            "live_readiness_allowed": (
                live_readiness_allowed
            ),
            "promotion_allowed": (
                live_readiness_allowed
            ),
            "final_decision": (
                "ELIGIBLE_FOR_PHASE_14G_REVIEW"
                if live_readiness_allowed
                else "BLOCK_LIVE_EXECUTION"
            ),
            "evidence": {
                "institutional_rows": institutional_rows,
                "institutional_cycles": institutional_cycles,
                "profitable_observations": profitable_observations,
                "verified_live_rows": verified_live_rows,
                "verified_live_cycles": verified_live_cycles,
                "out_of_sample_trades": out_of_sample_trades,
                "accepted_filtered_trades": accepted_trades,
                "filtered_profitable_confirmation_rate": (
                    filtered_profitable_rate
                ),
                "filtered_median_realized_profit_usd": (
                    filtered_median_profit
                ),
                "filtered_single_trade_concentration": (
                    filtered_concentration
                ),
            },
            "safety": {
                "wallet_connected": False,
                "private_key_loaded": False,
                "transaction_signing_enabled": False,
                "transaction_broadcasting_enabled": False,
                "live_execution_enabled": False,
                "automatic_promotion_enabled": False,
            },
            "valid": True,
        }

        return (
            summary,
            gates,
            evidence_rows,
        )


def executive_summary_text(
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "Phase 14F — Consolidated Post-Filter Readiness Gate",
        "=" * 80,
        "",
        f"Final decision: {summary['final_decision']}",
        (
            "Operational controls passed: "
            f"{summary['operational_controls_passed']}"
        ),
        (
            "Post-filter economics passed: "
            f"{summary['post_filter_economics_passed']}"
        ),
        (
            "Evidence sufficiency passed: "
            f"{summary['evidence_sufficiency_passed']}"
        ),
        (
            "Live readiness allowed: "
            f"{summary['live_readiness_allowed']}"
        ),
        "",
        "Evidence",
        "-" * 80,
    ]

    for key, value in summary[
        "evidence"
    ].items():
        lines.append(
            f"{key}: {value}"
        )

    lines.extend(
        [
            "",
            "Blocking reasons",
            "-" * 80,
        ]
    )

    if summary["blocking_reasons"]:
        lines.extend(
            f"- {reason}"
            for reason in summary[
                "blocking_reasons"
            ]
        )
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "Safety",
            "-" * 80,
            "Wallet connected: False",
            "Private key loaded: False",
            "Transaction signing: False",
            "Transaction broadcasting: False",
            "Live execution: False",
            "Automatic promotion: False",
            "",
        ]
    )

    return "\n".join(lines)


def export_results(
    *,
    summary: Mapping[str, Any],
    gates: Sequence[GateCheck],
    evidence_rows: Sequence[Mapping[str, Any]],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    paths = {
        "gates": output / GATES_CSV,
        "evidence": output / EVIDENCE_CSV,
        "report": output / REPORT_JSON,
        "manifest": output / MANIFEST_JSON,
        "summary": output / SUMMARY_TXT,
    }

    if not configuration.overwrite:
        existing = [
            path
            for path in paths.values()
            if path.exists()
        ]

        if existing:
            raise PostFilterReadinessError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    write_csv(
        paths["gates"],
        [
            gate.to_dict()
            for gate in gates
        ],
    )

    write_csv(
        paths["evidence"],
        evidence_rows,
    )

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(configuration),
                    "phase_13f_report": str(
                        configuration.phase_13f_report
                    ),
                    "phase_14a_report": str(
                        configuration.phase_14a_report
                    ),
                    "phase_14b_report": str(
                        configuration.phase_14b_report
                    ),
                    "phase_14c_report": str(
                        configuration.phase_14c_report
                    ),
                    "phase_14d_report": str(
                        configuration.phase_14d_report
                    ),
                    "phase_14e_report": str(
                        configuration.phase_14e_report
                    ),
                    "output_directory": str(
                        configuration.output_directory
                    ),
                },
                "gate_checks": [
                    gate.to_dict()
                    for gate in gates
                ],
                "evidence_summary": list(
                    evidence_rows
                ),
                "governance": {
                    "audit_only": True,
                    "wallet_connection_authorized": False,
                    "private_key_access_authorized": False,
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

    paths["summary"].write_text(
        executive_summary_text(
            summary
        ),
        encoding="utf-8",
    )

    row_counts = {
        "gates": len(gates),
        "evidence": len(evidence_rows),
        "report": None,
        "summary": None,
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

    inputs = {}

    for name, path in {
        "phase_13f": configuration.phase_13f_report,
        "phase_14a": configuration.phase_14a_report,
        "phase_14b": configuration.phase_14b_report,
        "phase_14c": configuration.phase_14c_report,
        "phase_14d": configuration.phase_14d_report,
        "phase_14e": configuration.phase_14e_report,
    }.items():
        inputs[name] = {
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

    paths["manifest"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": utc_now(),
                "summary": dict(summary),
                "inputs": inputs,
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
            "Run Phase 14F consolidated "
            "post-filter readiness gate."
        )
    )

    parser.add_argument(
        "--phase-13f-report",
        default=str(DEFAULT_PHASE_13F),
    )

    parser.add_argument(
        "--phase-14a-report",
        default=str(DEFAULT_PHASE_14A),
    )

    parser.add_argument(
        "--phase-14b-report",
        default=str(DEFAULT_PHASE_14B),
    )

    parser.add_argument(
        "--phase-14c-report",
        default=str(DEFAULT_PHASE_14C),
    )

    parser.add_argument(
        "--phase-14d-report",
        default=str(DEFAULT_PHASE_14D),
    )

    parser.add_argument(
        "--phase-14e-report",
        default=str(DEFAULT_PHASE_14E),
    )

    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
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


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    configuration = Configuration(
        phase_13f_report=Path(
            args.phase_13f_report
        ),
        phase_14a_report=Path(
            args.phase_14a_report
        ),
        phase_14b_report=Path(
            args.phase_14b_report
        ),
        phase_14c_report=Path(
            args.phase_14c_report
        ),
        phase_14d_report=Path(
            args.phase_14d_report
        ),
        phase_14e_report=Path(
            args.phase_14e_report
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
            gates,
            evidence_rows,
        ) = PostFilterReadinessGate(
            configuration
        ).run()

        output_paths = export_results(
            summary=summary,
            gates=gates,
            evidence_rows=evidence_rows,
            configuration=configuration,
        )

    except (
        PostFilterReadinessError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print(
        "\nPhase 14F — Consolidated "
        "Post-Filter Readiness Gate"
    )

    print("=" * 80)

    print(
        f"Operating mode: "
        f"{summary['operating_mode']}"
    )

    print()

    print("Readiness Summary")
    print("-" * 80)

    print(
        "Operational controls passed: "
        f"{summary['operational_controls_passed']}"
    )

    print(
        "Post-filter economics passed: "
        f"{summary['post_filter_economics_passed']}"
    )

    print(
        "Evidence sufficiency passed: "
        f"{summary['evidence_sufficiency_passed']}"
    )

    print(
        "Live readiness allowed: "
        f"{summary['live_readiness_allowed']}"
    )

    print()

    print("Evidence")
    print("-" * 80)

    for key, value in summary[
        "evidence"
    ].items():
        print(
            f"{key}: {value}"
        )

    print()

    print("Gate Checks")
    print("-" * 80)

    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:38} | "
            f"observed={gate.observed} "
            f"{gate.comparison} "
            f"required={gate.required}"
        )

        if not gate.passed:
            print(
                f"       {gate.message}"
            )

    print()

    print(
        "Final decision: "
        f"{summary['final_decision']}"
    )

    if summary[
        "blocking_reasons"
    ]:
        print(
            "Blocking reasons:"
        )

        for reason in summary[
            "blocking_reasons"
        ]:
            print(
                f"  - {reason}"
            )

    print()

    print("Safety")
    print("-" * 80)
    print("Wallet connected: False")
    print("Private key loaded: False")
    print("Transaction signing: False")
    print("Transaction broadcasting: False")
    print("Live execution: False")
    print("Automatic promotion: False")

    print()

    print("Output files")
    print("-" * 80)

    for path in output_paths:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())