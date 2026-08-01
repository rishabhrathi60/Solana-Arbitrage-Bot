"""
Phase 14G — Tiny-Pilot Configuration and Human Approval Audit

Creates an audit-only tiny-pilot configuration and verifies that no live pilot
can be enabled without explicit human approval and all upstream evidence gates.

This phase does not connect a wallet, load a private key, sign a transaction,
broadcast a transaction, or enable live execution.

Inputs
------
research/institutional_promotion_gate/institutional_promotion_decision.json
execution/shadow_results/shadow_execution_report.json
execution/shadow_diagnostics/shadow_execution_diagnostics_report.json
execution/resilience_results/operational_resilience_report.json
execution/live_readiness_audit/live_readiness_audit_report.json
execution/execution_aware_filter/execution_aware_report.json
execution/post_filter_readiness/post_filter_readiness_report.json  (Phase 14F)

Outputs
-------
execution/tiny_pilot_audit/
    tiny_pilot_configuration.json
    tiny_pilot_gate_checks.csv
    human_approval_checklist.csv
    tiny_pilot_audit_report.json
    tiny_pilot_audit_manifest.json
    executive_summary.txt

Governance
----------
- live execution remains disabled;
- pilot approval defaults to false;
- private-key and wallet access are prohibited;
- Phase 14F is required;
- human approval must be explicit and external to this module;
- automatic promotion is impossible;
- a passing result means configuration-review eligibility only.
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

SCHEMA_VERSION = "14G.1.0"
OPERATING_MODE = "TINY_PILOT_AUDIT_ONLY"

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

DEFAULT_PHASE_14F = (
    PROJECT_ROOT
    / "execution"
    / "post_filter_readiness"
    / "post_filter_readiness_report.json"
)

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "execution"
    / "tiny_pilot_audit"
)

CONFIG_JSON = "tiny_pilot_configuration.json"
GATES_CSV = "tiny_pilot_gate_checks.csv"
APPROVAL_CSV = "human_approval_checklist.csv"
REPORT_JSON = "tiny_pilot_audit_report.json"
MANIFEST_JSON = "tiny_pilot_audit_manifest.json"
SUMMARY_TXT = "executive_summary.txt"


class TinyPilotAuditError(RuntimeError):
    """Base exception for Phase 14G failures."""


@dataclass(frozen=True, slots=True)
class Configuration:
    phase_13f_report: Path = DEFAULT_PHASE_13F
    phase_14a_report: Path = DEFAULT_PHASE_14A
    phase_14b_report: Path = DEFAULT_PHASE_14B
    phase_14c_report: Path = DEFAULT_PHASE_14C
    phase_14d_report: Path = DEFAULT_PHASE_14D
    phase_14e_report: Path = DEFAULT_PHASE_14E
    phase_14f_report: Path = DEFAULT_PHASE_14F
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    pilot_enabled: bool = False
    human_approval_granted: bool = False
    wallet_connection_allowed: bool = False
    signing_allowed: bool = False
    broadcasting_allowed: bool = False
    automatic_promotion_allowed: bool = False

    maximum_trade_usd: float = 5.00
    maximum_daily_notional_usd: float = 20.00
    maximum_daily_loss_usd: float = 2.00
    maximum_trades_per_day: int = 4
    maximum_open_positions: int = 1
    maximum_consecutive_losses: int = 2
    maximum_slippage_bps: float = 20.0
    maximum_quote_age_ms: float = 1_000.0
    maximum_confirmation_wait_ms: float = 30_000.0

    require_phase_13f: bool = True
    require_phase_14a: bool = True
    require_phase_14b: bool = True
    require_phase_14c: bool = True
    require_phase_14d: bool = True
    require_phase_14e: bool = True
    require_phase_14f: bool = True

    def validate(self) -> None:
        positive_numbers = (
            "maximum_trade_usd",
            "maximum_daily_notional_usd",
            "maximum_daily_loss_usd",
            "maximum_slippage_bps",
            "maximum_quote_age_ms",
            "maximum_confirmation_wait_ms",
        )

        for name in positive_numbers:
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                raise TinyPilotAuditError(
                    f"{name} must be finite and positive."
                )

        positive_integers = (
            "maximum_trades_per_day",
            "maximum_open_positions",
            "maximum_consecutive_losses",
        )

        for name in positive_integers:
            if int(getattr(self, name)) <= 0:
                raise TinyPilotAuditError(
                    f"{name} must be positive."
                )

        if self.maximum_daily_notional_usd < self.maximum_trade_usd:
            raise TinyPilotAuditError(
                "maximum_daily_notional_usd must be >= maximum_trade_usd."
            )


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    blocking: bool
    observed: Any
    comparison: str
    required: Any
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ApprovalItem:
    item_id: str
    category: str
    description: str
    required: bool
    approved: bool
    approver: str
    approved_at: str
    evidence_reference: str
    blocking: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


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


def safe_float(value: Any, default: float = 0.0) -> float:
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
            raise TinyPilotAuditError(
                f"Required upstream report is missing: {path}"
            )

        return {}

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise TinyPilotAuditError(
            f"Expected a JSON object: {path}"
        )

    return payload


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
) -> None:
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


class TinyPilotAudit:
    def __init__(self, configuration: Configuration) -> None:
        self.config = configuration
        self.config.validate()

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        list[GateCheck],
        list[ApprovalItem],
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
        phase_14f = load_json(
            self.config.phase_14f_report,
            required=self.config.require_phase_14f,
        )

        phase_14a_summary = phase_14a.get("summary", {})
        phase_14b_summary = phase_14b.get("summary", {})
        phase_14c_summary = phase_14c.get("summary", {})
        phase_14d_summary = phase_14d.get("summary", {})
        phase_14e_summary = phase_14e.get("summary", {})
        phase_14f_summary = phase_14f.get("summary", {})

        upstream = {
            "phase_13f_live_readiness_allowed": safe_bool(
                phase_13f.get("live_readiness_allowed")
            ),
            "phase_14a_operational_gate_passed": safe_bool(
                phase_14a_summary.get("operational_gate_passed")
            ),
            "phase_14b_diagnostics_passed": safe_bool(
                phase_14b_summary.get("diagnostics_passed")
            ),
            "phase_14c_resilience_passed": safe_bool(
                phase_14c_summary.get("resilience_passed")
            ),
            "phase_14d_security_audit_passed": safe_bool(
                phase_14d_summary.get("security_audit_passed")
            ),
            "phase_14e_filter_passed": safe_bool(
                phase_14e_summary.get("filter_passed")
            ),
            "phase_14f_readiness_allowed": safe_bool(
                phase_14f_summary.get("live_readiness_allowed")
                or phase_14f_summary.get("post_filter_readiness_allowed")
                or phase_14f_summary.get("promotion_allowed")
            ),
        }

        pilot_configuration = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": utc_now(),
            "operating_mode": OPERATING_MODE,
            "pilot_enabled": False,
            "human_approval_granted": False,
            "wallet_connection_allowed": False,
            "private_key_loading_allowed": False,
            "transaction_signing_allowed": False,
            "transaction_broadcasting_allowed": False,
            "automatic_promotion_allowed": False,
            "limits": {
                "maximum_trade_usd": self.config.maximum_trade_usd,
                "maximum_daily_notional_usd": (
                    self.config.maximum_daily_notional_usd
                ),
                "maximum_daily_loss_usd": (
                    self.config.maximum_daily_loss_usd
                ),
                "maximum_trades_per_day": (
                    self.config.maximum_trades_per_day
                ),
                "maximum_open_positions": (
                    self.config.maximum_open_positions
                ),
                "maximum_consecutive_losses": (
                    self.config.maximum_consecutive_losses
                ),
                "maximum_slippage_bps": (
                    self.config.maximum_slippage_bps
                ),
                "maximum_quote_age_ms": (
                    self.config.maximum_quote_age_ms
                ),
                "maximum_confirmation_wait_ms": (
                    self.config.maximum_confirmation_wait_ms
                ),
            },
            "mandatory_controls": {
                "manual_start_required": True,
                "manual_stop_available": True,
                "emergency_stop_required": True,
                "daily_loss_stop_required": True,
                "consecutive_loss_stop_required": True,
                "duplicate_submission_guard_required": True,
                "quote_expiry_guard_required": True,
                "transaction_simulation_required": True,
                "human_approval_required": True,
                "audit_logging_required": True,
            },
        }

        approval_items = self._approval_items(
            upstream
        )

        gates = [
            GateCheck(
                name="PHASE_13F_LIVE_READINESS",
                passed=upstream[
                    "phase_13f_live_readiness_allowed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_13f_live_readiness_allowed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 13F institutional live readiness remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14A_OPERATIONAL_GATE",
                passed=upstream[
                    "phase_14a_operational_gate_passed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_14a_operational_gate_passed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 14A shadow operational gate remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14B_DIAGNOSTICS_GATE",
                passed=upstream[
                    "phase_14b_diagnostics_passed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_14b_diagnostics_passed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 14B edge diagnostics remain blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14C_RESILIENCE_GATE",
                passed=upstream[
                    "phase_14c_resilience_passed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_14c_resilience_passed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 14C resilience gate must pass."
                ),
            ),
            GateCheck(
                name="PHASE_14D_SECURITY_GATE",
                passed=upstream[
                    "phase_14d_security_audit_passed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_14d_security_audit_passed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 14D security audit remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14E_FILTER_GATE",
                passed=upstream[
                    "phase_14e_filter_passed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_14e_filter_passed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 14E execution-aware filter remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14F_POST_FILTER_GATE",
                passed=upstream[
                    "phase_14f_readiness_allowed"
                ],
                blocking=True,
                observed=upstream[
                    "phase_14f_readiness_allowed"
                ],
                comparison="==",
                required=True,
                message=(
                    "Phase 14F consolidated post-filter readiness "
                    "must pass before pilot review."
                ),
            ),
            GateCheck(
                name="PILOT_DISABLED_BY_DEFAULT",
                passed=not self.config.pilot_enabled,
                blocking=True,
                observed=self.config.pilot_enabled,
                comparison="==",
                required=False,
                message=(
                    "Tiny pilot must remain disabled by default."
                ),
            ),
            GateCheck(
                name="NO_WALLET_ACCESS",
                passed=(
                    not self.config.wallet_connection_allowed
                    and not self.config.signing_allowed
                    and not self.config.broadcasting_allowed
                ),
                blocking=True,
                observed=0,
                comparison="==",
                required=0,
                message=(
                    "Wallet access, signing, and broadcasting must remain "
                    "disabled during audit."
                ),
            ),
            GateCheck(
                name="NO_AUTOMATIC_PROMOTION",
                passed=not self.config.automatic_promotion_allowed,
                blocking=True,
                observed=self.config.automatic_promotion_allowed,
                comparison="==",
                required=False,
                message=(
                    "Automatic promotion must remain disabled."
                ),
            ),
            GateCheck(
                name="HUMAN_APPROVAL_GRANTED",
                passed=self.config.human_approval_granted,
                blocking=True,
                observed=self.config.human_approval_granted,
                comparison="==",
                required=True,
                message=(
                    "Explicit human approval has not been recorded."
                ),
            ),
            GateCheck(
                name="TINY_TRADE_LIMIT",
                passed=self.config.maximum_trade_usd <= 5.00,
                blocking=True,
                observed=self.config.maximum_trade_usd,
                comparison="<=",
                required=5.00,
                message=(
                    "Maximum tiny-pilot trade amount exceeds $5."
                ),
            ),
            GateCheck(
                name="DAILY_NOTIONAL_LIMIT",
                passed=self.config.maximum_daily_notional_usd <= 20.00,
                blocking=True,
                observed=self.config.maximum_daily_notional_usd,
                comparison="<=",
                required=20.00,
                message=(
                    "Maximum daily pilot notional exceeds $20."
                ),
            ),
            GateCheck(
                name="DAILY_LOSS_LIMIT",
                passed=self.config.maximum_daily_loss_usd <= 2.00,
                blocking=True,
                observed=self.config.maximum_daily_loss_usd,
                comparison="<=",
                required=2.00,
                message=(
                    "Maximum daily pilot loss exceeds $2."
                ),
            ),
            GateCheck(
                name="DAILY_TRADE_LIMIT",
                passed=self.config.maximum_trades_per_day <= 4,
                blocking=True,
                observed=self.config.maximum_trades_per_day,
                comparison="<=",
                required=4,
                message=(
                    "Maximum trades per day exceeds four."
                ),
            ),
            GateCheck(
                name="OPEN_POSITION_LIMIT",
                passed=self.config.maximum_open_positions == 1,
                blocking=True,
                observed=self.config.maximum_open_positions,
                comparison="==",
                required=1,
                message=(
                    "Tiny pilot must allow only one open position."
                ),
            ),
        ]

        blocking_failures = [
            gate
            for gate in gates
            if gate.blocking and not gate.passed
        ]

        approval_failures = [
            item
            for item in approval_items
            if item.required
            and item.blocking
            and not item.approved
        ]

        configuration_safe = all(
            gate.passed
            for gate in gates
            if gate.name in {
                "PILOT_DISABLED_BY_DEFAULT",
                "NO_WALLET_ACCESS",
                "NO_AUTOMATIC_PROMOTION",
                "TINY_TRADE_LIMIT",
                "DAILY_NOTIONAL_LIMIT",
                "DAILY_LOSS_LIMIT",
                "DAILY_TRADE_LIMIT",
                "OPEN_POSITION_LIMIT",
            }
        )

        review_eligible = (
            not blocking_failures
            and not approval_failures
        )

        summary = {
            "generated_at": utc_now(),
            "schema_version": SCHEMA_VERSION,
            "operating_mode": OPERATING_MODE,
            "configuration_safe": configuration_safe,
            "human_approval_complete": not approval_failures,
            "upstream_gates_passed": all(upstream.values()),
            "gate_checks": len(gates),
            "gate_checks_passed": sum(
                gate.passed
                for gate in gates
            ),
            "approval_items": len(approval_items),
            "approval_items_approved": sum(
                item.approved
                for item in approval_items
            ),
            "blocking_failures": len(blocking_failures)
            + len(approval_failures),
            "blocking_reasons": (
                [
                    gate.message
                    for gate in blocking_failures
                ]
                + [
                    (
                        "Human approval incomplete: "
                        f"{item.description}"
                    )
                    for item in approval_failures
                ]
            ),
            "tiny_pilot_review_eligible": review_eligible,
            "final_decision": (
                "ELIGIBLE_FOR_MANUAL_TINY_PILOT_REVIEW"
                if review_eligible
                else "BLOCK_TINY_LIVE_PILOT"
            ),
            "upstream": upstream,
            "safety": {
                "pilot_enabled": False,
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
            pilot_configuration,
            gates,
            approval_items,
        )

    @staticmethod
    def _approval_items(
        upstream: Mapping[str, bool],
    ) -> list[ApprovalItem]:
        return [
            ApprovalItem(
                item_id="APPROVAL_001",
                category="RESEARCH",
                description=(
                    "Independent evidence and post-filter readiness "
                    "have passed all required gates."
                ),
                required=True,
                approved=all(upstream.values()),
                approver="",
                approved_at="",
                evidence_reference="Phases 13F and 14A-14F",
                blocking=True,
            ),
            ApprovalItem(
                item_id="APPROVAL_002",
                category="SECURITY",
                description=(
                    "Wallet isolation, secret handling, and kill-switch "
                    "controls have been manually reviewed."
                ),
                required=True,
                approved=False,
                approver="",
                approved_at="",
                evidence_reference="Phase 14D",
                blocking=True,
            ),
            ApprovalItem(
                item_id="APPROVAL_003",
                category="RISK",
                description=(
                    "Maximum $5 trade, $20 daily notional, $2 daily loss, "
                    "and four-trade daily limits are approved."
                ),
                required=True,
                approved=False,
                approver="",
                approved_at="",
                evidence_reference="tiny_pilot_configuration.json",
                blocking=True,
            ),
            ApprovalItem(
                item_id="APPROVAL_004",
                category="OPERATIONS",
                description=(
                    "A named operator is assigned to start, observe, "
                    "and stop the pilot manually."
                ),
                required=True,
                approved=False,
                approver="",
                approved_at="",
                evidence_reference="Manual operating procedure",
                blocking=True,
            ),
            ApprovalItem(
                item_id="APPROVAL_005",
                category="INCIDENT_RESPONSE",
                description=(
                    "Emergency-stop procedure and incident escalation "
                    "contacts are documented and reviewed."
                ),
                required=True,
                approved=False,
                approver="",
                approved_at="",
                evidence_reference="Phase 14C and operating procedure",
                blocking=True,
            ),
            ApprovalItem(
                item_id="APPROVAL_006",
                category="FINAL_AUTHORIZATION",
                description=(
                    "Explicit human authorization for a tiny live pilot "
                    "has been recorded outside the bot."
                ),
                required=True,
                approved=False,
                approver="",
                approved_at="",
                evidence_reference="External signed approval",
                blocking=True,
            ),
        ]


def executive_summary_text(
    summary: Mapping[str, Any],
    pilot_configuration: Mapping[str, Any],
    gates: Sequence[GateCheck],
    approvals: Sequence[ApprovalItem],
) -> str:
    lines = [
        "Phase 14G — Tiny-Pilot Configuration and Human Approval Audit",
        "=" * 80,
        "",
        f"Final decision: {summary['final_decision']}",
        (
            "Tiny-pilot review eligible: "
            f"{summary['tiny_pilot_review_eligible']}"
        ),
        (
            "Configuration safe: "
            f"{summary['configuration_safe']}"
        ),
        (
            "Human approval complete: "
            f"{summary['human_approval_complete']}"
        ),
        "",
        "Pilot limits",
        "-" * 80,
    ]

    limits = pilot_configuration["limits"]

    for key, value in limits.items():
        lines.append(f"{key}: {value}")

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
            for reason in summary["blocking_reasons"]
        )
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "Safety state",
            "-" * 80,
            "Pilot enabled: False",
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
    pilot_configuration: Mapping[str, Any],
    gates: Sequence[GateCheck],
    approvals: Sequence[ApprovalItem],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    paths = {
        "configuration": output / CONFIG_JSON,
        "gates": output / GATES_CSV,
        "approvals": output / APPROVAL_CSV,
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
            raise TinyPilotAuditError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    paths["configuration"].write_text(
        json.dumps(
            pilot_configuration,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    write_csv(
        paths["gates"],
        [
            gate.to_dict()
            for gate in gates
        ],
    )

    write_csv(
        paths["approvals"],
        [
            approval.to_dict()
            for approval in approvals
        ],
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
                    "phase_14f_report": str(
                        configuration.phase_14f_report
                    ),
                    "output_directory": str(
                        configuration.output_directory
                    ),
                },
                "pilot_configuration": dict(
                    pilot_configuration
                ),
                "gate_checks": [
                    gate.to_dict()
                    for gate in gates
                ],
                "human_approval_checklist": [
                    approval.to_dict()
                    for approval in approvals
                ],
                "governance": {
                    "audit_only": True,
                    "pilot_enabled": False,
                    "wallet_connection_authorized": False,
                    "private_key_access_authorized": False,
                    "transaction_signing_enabled": False,
                    "transaction_broadcasting_enabled": False,
                    "live_execution_enabled": False,
                    "automatic_promotion_enabled": False,
                    "external_human_approval_required": True,
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
            summary,
            pilot_configuration,
            gates,
            approvals,
        ),
        encoding="utf-8",
    )

    row_counts = {
        "configuration": None,
        "gates": len(gates),
        "approvals": len(approvals),
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

    input_paths = {
        "phase_13f": configuration.phase_13f_report,
        "phase_14a": configuration.phase_14a_report,
        "phase_14b": configuration.phase_14b_report,
        "phase_14c": configuration.phase_14c_report,
        "phase_14d": configuration.phase_14d_report,
        "phase_14e": configuration.phase_14e_report,
        "phase_14f": configuration.phase_14f_report,
    }

    inputs: dict[str, Any] = {}

    for name, path in input_paths.items():
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
            "Run Phase 14G tiny-pilot configuration "
            "and human approval audit."
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
        "--phase-14f-report",
        default=str(DEFAULT_PHASE_14F),
    )

    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )

    parser.add_argument(
        "--maximum-trade-usd",
        type=float,
        default=5.00,
    )

    parser.add_argument(
        "--maximum-daily-notional-usd",
        type=float,
        default=20.00,
    )

    parser.add_argument(
        "--maximum-daily-loss-usd",
        type=float,
        default=2.00,
    )

    parser.add_argument(
        "--maximum-trades-per-day",
        type=int,
        default=4,
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
        phase_14f_report=Path(
            args.phase_14f_report
        ),
        output_directory=Path(
            args.output_directory
        ),
        overwrite=(
            not args.no_overwrite
        ),
        maximum_trade_usd=(
            args.maximum_trade_usd
        ),
        maximum_daily_notional_usd=(
            args.maximum_daily_notional_usd
        ),
        maximum_daily_loss_usd=(
            args.maximum_daily_loss_usd
        ),
        maximum_trades_per_day=(
            args.maximum_trades_per_day
        ),
    )

    try:
        (
            summary,
            pilot_configuration,
            gates,
            approvals,
        ) = TinyPilotAudit(
            configuration
        ).run()

        output_paths = export_results(
            summary=summary,
            pilot_configuration=pilot_configuration,
            gates=gates,
            approvals=approvals,
            configuration=configuration,
        )

    except (
        TinyPilotAuditError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print(
        "\nPhase 14G — Tiny-Pilot Configuration "
        "and Human Approval Audit"
    )
    print("=" * 80)

    print(
        f"Operating mode: {summary['operating_mode']}"
    )

    print()

    print("Pilot Configuration")
    print("-" * 80)

    limits = pilot_configuration[
        "limits"
    ]

    print(
        f"Pilot enabled: "
        f"{pilot_configuration['pilot_enabled']}"
    )

    print(
        "Maximum trade / daily notional / daily loss: "
        f"${limits['maximum_trade_usd']:.2f} / "
        f"${limits['maximum_daily_notional_usd']:.2f} / "
        f"${limits['maximum_daily_loss_usd']:.2f}"
    )

    print(
        "Maximum trades / open positions / consecutive losses: "
        f"{limits['maximum_trades_per_day']} / "
        f"{limits['maximum_open_positions']} / "
        f"{limits['maximum_consecutive_losses']}"
    )

    print()

    print("Gate Checks")
    print("-" * 80)

    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:32} | "
            f"observed={gate.observed} "
            f"{gate.comparison} "
            f"required={gate.required}"
        )

        if not gate.passed:
            print(
                f"       {gate.message}"
            )

    print()

    print("Human Approval Checklist")
    print("-" * 80)

    for item in approvals:
        print(
            f"{'APPROVED' if item.approved else 'PENDING'} | "
            f"{item.item_id} | "
            f"{item.category} | "
            f"{item.description}"
        )

    print()

    print(
        "Configuration safe: "
        f"{summary['configuration_safe']}"
    )

    print(
        "Human approval complete: "
        f"{summary['human_approval_complete']}"
    )

    print(
        "Tiny-pilot review eligible: "
        f"{summary['tiny_pilot_review_eligible']}"
    )

    print(
        "Final decision: "
        f"{summary['final_decision']}"
    )

    if summary["blocking_reasons"]:
        print("Blocking reasons:")

        for reason in summary["blocking_reasons"]:
            print(f"  - {reason}")

    print()

    print("Safety")
    print("-" * 80)
    print("Pilot enabled: False")
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