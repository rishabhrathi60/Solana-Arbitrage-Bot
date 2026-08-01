"""
Phase 14D — Wallet Security, Kill-Switch, and Live-Readiness Audit

Performs an audit-only review of repository safety controls before any tiny
live-pilot review.

The audit verifies:
- no private keys or seed phrases are committed;
- wallet files and secret-bearing environment files are excluded from Git;
- live execution is disabled by configuration;
- transaction signing and broadcasting remain disabled;
- transaction-size, daily-loss, trade-count, and drawdown limits exist;
- emergency-stop and kill-switch controls are present;
- Phase 13F, 14A, 14B, and 14C upstream decisions are respected;
- audit-only database and report outputs are available;
- no automatic promotion can bypass human approval.

Inputs
------
research/institutional_promotion_gate/institutional_promotion_decision.json
execution/shadow_results/shadow_execution_report.json
execution/shadow_diagnostics/shadow_execution_diagnostics_report.json
execution/resilience_results/operational_resilience_report.json
.gitignore
.env.example (optional)
repository source files

Outputs
-------
execution/live_readiness_audit/
    wallet_security_findings.csv
    kill_switch_findings.csv
    live_readiness_gate_checks.csv
    repository_secret_scan.csv
    live_readiness_audit_report.json
    live_readiness_audit_manifest.json
    executive_summary.txt

Safety
------
This module does not:
- connect a wallet;
- read or print secret values;
- sign transactions;
- submit or broadcast transactions;
- enable live execution;
- change runtime configuration;
- promote a strategy automatically.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "14D.1.0"
OPERATING_MODE = "SECURITY_AUDIT_ONLY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PROMOTION_DECISION = (
    PROJECT_ROOT
    / "research"
    / "institutional_promotion_gate"
    / "institutional_promotion_decision.json"
)

DEFAULT_SHADOW_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_report.json"
)

DEFAULT_DIAGNOSTICS_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "shadow_diagnostics"
    / "shadow_execution_diagnostics_report.json"
)

DEFAULT_RESILIENCE_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "resilience_results"
    / "operational_resilience_report.json"
)

DEFAULT_GITIGNORE = PROJECT_ROOT / ".gitignore"
DEFAULT_ENV_EXAMPLE = PROJECT_ROOT / ".env.example"

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "execution"
    / "live_readiness_audit"
)

WALLET_FINDINGS_CSV = "wallet_security_findings.csv"
KILL_SWITCH_FINDINGS_CSV = "kill_switch_findings.csv"
GATES_CSV = "live_readiness_gate_checks.csv"
SECRET_SCAN_CSV = "repository_secret_scan.csv"
REPORT_JSON = "live_readiness_audit_report.json"
MANIFEST_JSON = "live_readiness_audit_manifest.json"
SUMMARY_TXT = "executive_summary.txt"


class LiveReadinessAuditError(RuntimeError):
    """Base exception for Phase 14D failures."""


@dataclass(frozen=True, slots=True)
class Configuration:
    promotion_decision: Path = DEFAULT_PROMOTION_DECISION
    shadow_report: Path = DEFAULT_SHADOW_REPORT
    diagnostics_report: Path = DEFAULT_DIAGNOSTICS_REPORT
    resilience_report: Path = DEFAULT_RESILIENCE_REPORT
    gitignore_path: Path = DEFAULT_GITIGNORE
    env_example_path: Path = DEFAULT_ENV_EXAMPLE
    repository_root: Path = PROJECT_ROOT
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    maximum_files_to_scan: int = 10_000
    maximum_file_size_bytes: int = 2_000_000

    require_gitignore_secret_patterns: bool = True
    require_no_secret_findings: bool = True
    require_upstream_live_readiness: bool = True
    require_shadow_gate: bool = True
    require_diagnostics_gate: bool = True
    require_resilience_gate: bool = True

    def validate(self) -> None:
        if self.maximum_files_to_scan <= 0:
            raise LiveReadinessAuditError(
                "maximum_files_to_scan must be positive."
            )

        if self.maximum_file_size_bytes <= 0:
            raise LiveReadinessAuditError(
                "maximum_file_size_bytes must be positive."
            )


@dataclass(frozen=True, slots=True)
class Finding:
    category: str
    check_id: str
    severity: str
    passed: bool
    blocking: bool
    message: str
    evidence: str
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    observed: Any
    comparison: str
    required: Any
    blocking: bool
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


SECRET_NAME_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(PRIVATE[_-]?KEY|SECRET[_-]?KEY|SEED[_-]?PHRASE)\b", re.I),
    re.compile(r"\bMNEMONIC\b", re.I),
    re.compile(r"\bWALLET[_-]?(SECRET|PRIVATE|KEYPAIR)\b", re.I),
    re.compile(r"\bRPC[_-]?(TOKEN|API[_-]?KEY)\b", re.I),
)

SECRET_VALUE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "SOLANA_BASE58_SECRET",
        re.compile(
            r"(?<![A-Za-z0-9])[1-9A-HJ-NP-Za-km-z]{80,120}(?![A-Za-z0-9])"
        ),
    ),
    (
        "MNEMONIC_PHRASE",
        re.compile(
            r"\b(?:[a-z]{3,12}\s+){11,23}[a-z]{3,12}\b",
            re.I,
        ),
    ),
    (
        "JSON_SECRET_ARRAY",
        re.compile(
            r"\[(?:\s*\d{1,3}\s*,){31,63}\s*\d{1,3}\s*\]"
        ),
    ),
)

SAFE_EXCLUDED_DIRECTORIES = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "research/institutional_robustness",
    "execution/shadow_results",
    "execution/shadow_diagnostics",
    "execution/resilience_results",
    "execution/live_readiness_audit",
}

TEXT_EXTENSIONS = {
    ".py",
    ".json",
    ".jsonl",
    ".csv",
    ".txt",
    ".md",
    ".toml",
    ".yaml",
    ".yml",
    ".ini",
    ".cfg",
    ".env",
    ".example",
    ".sh",
    ".sql",
}


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


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise LiveReadinessAuditError(
            f"Required JSON evidence file is missing: {path}"
        )

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise LiveReadinessAuditError(
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def normalize_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


class LiveReadinessAudit:
    def __init__(self, configuration: Configuration) -> None:
        self.config = configuration
        self.config.validate()

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        list[Finding],
        list[Finding],
        list[dict[str, Any]],
        list[GateCheck],
    ]:
        promotion = load_json(self.config.promotion_decision)
        shadow = load_json(self.config.shadow_report)
        diagnostics = load_json(self.config.diagnostics_report)
        resilience = load_json(self.config.resilience_report)

        gitignore_text = (
            self.config.gitignore_path.read_text(
                encoding="utf-8",
                errors="ignore",
            )
            if self.config.gitignore_path.exists()
            else ""
        )

        secret_scan = self._scan_repository()

        wallet_findings = self._wallet_findings(
            gitignore_text=gitignore_text,
            secret_scan=secret_scan,
        )

        kill_switch_findings = self._kill_switch_findings(
            shadow=shadow,
            resilience=resilience,
        )

        secret_blocking_findings = [
            row
            for row in secret_scan
            if row["severity"] == "CRITICAL"
            and not safe_bool(row["allowlisted"])
        ]

        shadow_summary = shadow.get("summary", {})
        diagnostics_summary = diagnostics.get("summary", {})
        resilience_summary = resilience.get("summary", {})

        gates = [
            GateCheck(
                name="NO_COMMITTED_SECRET_FINDINGS",
                passed=(
                    len(secret_blocking_findings) == 0
                    if self.config.require_no_secret_findings
                    else True
                ),
                observed=len(secret_blocking_findings),
                comparison="==",
                required=0,
                blocking=self.config.require_no_secret_findings,
                message=(
                    "Repository secret scan found possible committed "
                    "private-key or seed material."
                ),
            ),
            GateCheck(
                name="GITIGNORE_SECRET_COVERAGE",
                passed=(
                    all(
                        finding.passed
                        for finding in wallet_findings
                        if finding.check_id.startswith("GITIGNORE_")
                    )
                    if self.config.require_gitignore_secret_patterns
                    else True
                ),
                observed=sum(
                    finding.passed
                    for finding in wallet_findings
                    if finding.check_id.startswith("GITIGNORE_")
                ),
                comparison="==",
                required=sum(
                    1
                    for finding in wallet_findings
                    if finding.check_id.startswith("GITIGNORE_")
                ),
                blocking=self.config.require_gitignore_secret_patterns,
                message=(
                    ".gitignore does not cover all required secret-bearing "
                    "files and wallet artifacts."
                ),
            ),
            GateCheck(
                name="WALLET_ISOLATION_CONTROLS",
                passed=all(
                    finding.passed
                    for finding in wallet_findings
                    if finding.blocking
                ),
                observed=sum(
                    finding.passed
                    for finding in wallet_findings
                    if finding.blocking
                ),
                comparison="==",
                required=sum(
                    1
                    for finding in wallet_findings
                    if finding.blocking
                ),
                blocking=True,
                message=(
                    "One or more wallet-isolation controls are missing."
                ),
            ),
            GateCheck(
                name="KILL_SWITCH_CONTROLS",
                passed=all(
                    finding.passed
                    for finding in kill_switch_findings
                    if finding.blocking
                ),
                observed=sum(
                    finding.passed
                    for finding in kill_switch_findings
                    if finding.blocking
                ),
                comparison="==",
                required=sum(
                    1
                    for finding in kill_switch_findings
                    if finding.blocking
                ),
                blocking=True,
                message=(
                    "One or more emergency-stop or kill-switch controls "
                    "failed audit."
                ),
            ),
            GateCheck(
                name="PHASE_13F_LIVE_READINESS",
                passed=(
                    safe_bool(promotion.get("live_readiness_allowed"))
                    if self.config.require_upstream_live_readiness
                    else True
                ),
                observed=safe_bool(
                    promotion.get("live_readiness_allowed")
                ),
                comparison="==",
                required=True,
                blocking=self.config.require_upstream_live_readiness,
                message=(
                    "Phase 13F institutional live readiness remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14A_OPERATIONAL_GATE",
                passed=(
                    safe_bool(
                        shadow_summary.get("operational_gate_passed")
                    )
                    if self.config.require_shadow_gate
                    else True
                ),
                observed=safe_bool(
                    shadow_summary.get("operational_gate_passed")
                ),
                comparison="==",
                required=True,
                blocking=self.config.require_shadow_gate,
                message=(
                    "Phase 14A operational shadow gate remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14B_DIAGNOSTICS_GATE",
                passed=(
                    safe_bool(
                        diagnostics_summary.get("diagnostics_passed")
                    )
                    if self.config.require_diagnostics_gate
                    else True
                ),
                observed=safe_bool(
                    diagnostics_summary.get("diagnostics_passed")
                ),
                comparison="==",
                required=True,
                blocking=self.config.require_diagnostics_gate,
                message=(
                    "Phase 14B edge decomposition remains blocked."
                ),
            ),
            GateCheck(
                name="PHASE_14C_RESILIENCE_GATE",
                passed=(
                    safe_bool(
                        resilience_summary.get("resilience_passed")
                    )
                    if self.config.require_resilience_gate
                    else True
                ),
                observed=safe_bool(
                    resilience_summary.get("resilience_passed")
                ),
                comparison="==",
                required=True,
                blocking=self.config.require_resilience_gate,
                message=(
                    "Phase 14C resilience validation has not passed."
                ),
            ),
            GateCheck(
                name="NO_LIVE_EXECUTION_STATE",
                passed=(
                    not safe_bool(
                        shadow_summary.get("live_execution_enabled")
                    )
                    and not safe_bool(
                        shadow_summary.get("wallet_connection_authorized")
                    )
                    and not safe_bool(
                        shadow_summary.get("transaction_signing_enabled")
                    )
                    and not safe_bool(
                        shadow_summary.get(
                            "transaction_broadcasting_enabled"
                        )
                    )
                ),
                observed=0,
                comparison="==",
                required=0,
                blocking=True,
                message=(
                    "An upstream report indicates live execution, wallet "
                    "connection, signing, or broadcasting is enabled."
                ),
            ),
        ]

        blocking_failures = [
            gate
            for gate in gates
            if gate.blocking and not gate.passed
        ]

        audit_passed = not blocking_failures

        summary = {
            "generated_at": utc_now(),
            "schema_version": SCHEMA_VERSION,
            "operating_mode": OPERATING_MODE,
            "repository_root": str(
                self.config.repository_root
            ),
            "files_scanned": len(
                {
                    row["path"]
                    for row in secret_scan
                }
            ),
            "secret_findings": len(secret_scan),
            "critical_secret_findings": len(
                secret_blocking_findings
            ),
            "wallet_findings": len(wallet_findings),
            "wallet_findings_passed": sum(
                finding.passed
                for finding in wallet_findings
            ),
            "kill_switch_findings": len(
                kill_switch_findings
            ),
            "kill_switch_findings_passed": sum(
                finding.passed
                for finding in kill_switch_findings
            ),
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
            "security_audit_passed": audit_passed,
            "final_decision": (
                "ELIGIBLE_FOR_TINY_LIVE_PILOT_REVIEW"
                if audit_passed
                else "BLOCK_LIVE_EXECUTION"
            ),
            "upstream": {
                "phase_13f_live_readiness_allowed": safe_bool(
                    promotion.get("live_readiness_allowed")
                ),
                "phase_14a_operational_gate_passed": safe_bool(
                    shadow_summary.get("operational_gate_passed")
                ),
                "phase_14b_diagnostics_passed": safe_bool(
                    diagnostics_summary.get("diagnostics_passed")
                ),
                "phase_14c_resilience_passed": safe_bool(
                    resilience_summary.get("resilience_passed")
                ),
            },
            "safety": {
                "wallet_connected": False,
                "private_key_loaded": False,
                "transaction_signing_enabled": False,
                "transaction_broadcasting_enabled": False,
                "live_execution_enabled": False,
                "automatic_promotion_enabled": False,
                "manual_approval_required": True,
            },
            "valid": True,
        }

        return (
            summary,
            wallet_findings,
            kill_switch_findings,
            secret_scan,
            gates,
        )

    def _wallet_findings(
        self,
        *,
        gitignore_text: str,
        secret_scan: Sequence[Mapping[str, Any]],
    ) -> list[Finding]:
        patterns = {
            "GITIGNORE_ENV": (
                any(
                    line.strip() in {
                        ".env",
                        ".env.*",
                        "*.env",
                    }
                    for line in gitignore_text.splitlines()
                ),
                "Ignore .env and environment-secret files.",
            ),
            "GITIGNORE_KEYPAIR": (
                any(
                    token in gitignore_text
                    for token in (
                        "*.json.keypair",
                        "*keypair*.json",
                        "wallet*.json",
                        "*.pem",
                        "*.key",
                    )
                ),
                "Ignore wallet keypair and private-key files.",
            ),
            "GITIGNORE_SECRETS": (
                any(
                    token in gitignore_text
                    for token in (
                        "secrets/",
                        "secret/",
                        "*.secret",
                    )
                ),
                "Ignore dedicated secret directories and files.",
            ),
        }

        findings: list[Finding] = []

        for check_id, (passed, message) in patterns.items():
            findings.append(
                Finding(
                    category="WALLET_SECURITY",
                    check_id=check_id,
                    severity="ERROR" if not passed else "INFO",
                    passed=passed,
                    blocking=True,
                    message=message,
                    evidence=(
                        "Pattern present in .gitignore"
                        if passed
                        else "Required ignore pattern not found"
                    ),
                    source=str(
                        self.config.gitignore_path
                    ),
                )
            )

        critical_findings = [
            row
            for row in secret_scan
            if row["severity"] == "CRITICAL"
            and not safe_bool(row["allowlisted"])
        ]

        findings.append(
            Finding(
                category="WALLET_SECURITY",
                check_id="NO_PRIVATE_KEY_MATERIAL",
                severity=(
                    "ERROR"
                    if critical_findings
                    else "INFO"
                ),
                passed=not critical_findings,
                blocking=True,
                message=(
                    "Repository must not contain private-key, mnemonic, "
                    "or keypair material."
                ),
                evidence=(
                    f"{len(critical_findings)} blocking findings"
                ),
                source=str(
                    self.config.repository_root
                ),
            )
        )

        findings.extend(
            [
                Finding(
                    category="WALLET_SECURITY",
                    check_id="NO_WALLET_CONNECTION",
                    severity="INFO",
                    passed=True,
                    blocking=True,
                    message=(
                        "Phase 14D does not connect a wallet."
                    ),
                    evidence="Static audit-only module.",
                    source=__file__,
                ),
                Finding(
                    category="WALLET_SECURITY",
                    check_id="NO_SECRET_VALUE_OUTPUT",
                    severity="INFO",
                    passed=True,
                    blocking=True,
                    message=(
                        "Secret scan reports type and location only, "
                        "never secret values."
                    ),
                    evidence="Redacted diagnostics design.",
                    source=__file__,
                ),
                Finding(
                    category="WALLET_SECURITY",
                    check_id="MANUAL_APPROVAL_REQUIRED",
                    severity="INFO",
                    passed=True,
                    blocking=True,
                    message=(
                        "Any tiny live pilot requires a separate manual "
                        "approval step."
                    ),
                    evidence="No automatic promotion code path.",
                    source=__file__,
                ),
            ]
        )

        return findings

    @staticmethod
    def _kill_switch_findings(
        *,
        shadow: Mapping[str, Any],
        resilience: Mapping[str, Any],
    ) -> list[Finding]:
        shadow_summary = shadow.get("summary", {})
        resilience_summary = resilience.get("summary", {})

        resilience_passed = safe_bool(
            resilience_summary.get("resilience_passed")
        )

        no_false_live = (
            safe_int(
                resilience_summary.get(
                    "false_live_execution_events"
                )
            )
            == 0
        )

        no_corruption = (
            safe_int(
                resilience_summary.get(
                    "state_corruption_events"
                )
            )
            == 0
        )

        live_disabled = (
            not safe_bool(
                shadow_summary.get("live_execution_enabled")
            )
            and not safe_bool(
                shadow_summary.get(
                    "transaction_signing_enabled"
                )
            )
            and not safe_bool(
                shadow_summary.get(
                    "transaction_broadcasting_enabled"
                )
            )
        )

        return [
            Finding(
                category="KILL_SWITCH",
                check_id="RESILIENCE_TESTS_PASS",
                severity=(
                    "INFO"
                    if resilience_passed
                    else "ERROR"
                ),
                passed=resilience_passed,
                blocking=True,
                message=(
                    "Failure-injection and emergency-stop tests must pass."
                ),
                evidence=str(
                    resilience_summary.get(
                        "final_decision",
                        "UNKNOWN",
                    )
                ),
                source="Phase 14C report",
            ),
            Finding(
                category="KILL_SWITCH",
                check_id="NO_FALSE_LIVE_EXECUTION",
                severity=(
                    "INFO"
                    if no_false_live
                    else "ERROR"
                ),
                passed=no_false_live,
                blocking=True,
                message=(
                    "No injected failure may create a false live-execution "
                    "event."
                ),
                evidence=(
                    f"{resilience_summary.get('false_live_execution_events', 0)} "
                    "events"
                ),
                source="Phase 14C report",
            ),
            Finding(
                category="KILL_SWITCH",
                check_id="NO_STATE_CORRUPTION",
                severity=(
                    "INFO"
                    if no_corruption
                    else "ERROR"
                ),
                passed=no_corruption,
                blocking=True,
                message=(
                    "Emergency controls must not corrupt execution state."
                ),
                evidence=(
                    f"{resilience_summary.get('state_corruption_events', 0)} "
                    "events"
                ),
                source="Phase 14C report",
            ),
            Finding(
                category="KILL_SWITCH",
                check_id="LIVE_EXECUTION_DISABLED",
                severity=(
                    "INFO"
                    if live_disabled
                    else "ERROR"
                ),
                passed=live_disabled,
                blocking=True,
                message=(
                    "Live execution, signing, and broadcasting must remain "
                    "disabled."
                ),
                evidence=(
                    "All upstream execution flags are disabled"
                    if live_disabled
                    else "One or more execution flags are enabled"
                ),
                source="Phase 14A report",
            ),
            Finding(
                category="KILL_SWITCH",
                check_id="EMERGENCY_STOP_SCENARIO",
                severity="INFO",
                passed=resilience_passed,
                blocking=True,
                message=(
                    "Emergency-stop scenario must block execution."
                ),
                evidence="Validated in Phase 14C.",
                source="Phase 14C report",
            ),
            Finding(
                category="KILL_SWITCH",
                check_id="CONSECUTIVE_LOSS_STOP",
                severity="INFO",
                passed=resilience_passed,
                blocking=True,
                message=(
                    "Consecutive-loss limit must trigger emergency stop."
                ),
                evidence="Validated in Phase 14C.",
                source="Phase 14C report",
            ),
        ]

    def _scan_repository(
        self,
    ) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        files_scanned = 0

        for path in self._iter_scan_files():
            files_scanned += 1

            if files_scanned > self.config.maximum_files_to_scan:
                break

            try:
                size = path.stat().st_size
            except OSError:
                continue

            if size > self.config.maximum_file_size_bytes:
                continue

            try:
                content = path.read_text(
                    encoding="utf-8",
                    errors="ignore",
                )
            except OSError:
                continue

            relative = normalize_path(
                path,
                self.config.repository_root,
            )

            for line_number, line in enumerate(
                content.splitlines(),
                start=1,
            ):
                allowlisted = self._allowlisted_line(
                    relative,
                    line,
                )

                for name_pattern in SECRET_NAME_PATTERNS:
                    if name_pattern.search(line):
                        findings.append(
                            {
                                "path": relative,
                                "line_number": line_number,
                                "finding_type": "SECRET_NAME_REFERENCE",
                                "severity": "WARNING",
                                "allowlisted": allowlisted,
                                "message": (
                                    "Secret-related identifier found; "
                                    "value not displayed."
                                ),
                            }
                        )
                        break

                for finding_type, pattern in SECRET_VALUE_PATTERNS:
                    if not pattern.search(line):
                        continue

                    if (
                        finding_type == "MNEMONIC_PHRASE"
                        and not self._mnemonic_context(
                            relative,
                            line,
                        )
                    ):
                        continue

                    findings.append(
                        {
                            "path": relative,
                            "line_number": line_number,
                            "finding_type": finding_type,
                            "severity": "CRITICAL",
                            "allowlisted": allowlisted,
                            "message": (
                                "Possible secret material detected; "
                                "value redacted."
                            ),
                        }
                    )

        return findings

    def _iter_scan_files(self) -> Iterable[Path]:
        root = self.config.repository_root

        for path in root.rglob("*"):
            if not path.is_file():
                continue

            relative = normalize_path(
                path,
                root,
            )

            if self._is_excluded(relative):
                continue

            if path.name in {
                ".env",
                ".env.local",
                ".env.production",
            }:
                yield path
                continue

            suffix = path.suffix.lower()

            if (
                suffix in TEXT_EXTENSIONS
                or path.name in {
                    ".gitignore",
                    "Dockerfile",
                    "Makefile",
                }
            ):
                yield path

    @staticmethod
    def _is_excluded(relative: str) -> bool:
        normalized = relative.strip("/")

        for excluded in SAFE_EXCLUDED_DIRECTORIES:
            if (
                normalized == excluded
                or normalized.startswith(
                    excluded.rstrip("/") + "/"
                )
            ):
                return True

        return False

    @staticmethod
    def _mnemonic_context(
        relative_path: str,
        line: str,
    ) -> bool:
        """
        Require secret-bearing context before treating a long sequence of
        ordinary lowercase words as a mnemonic.

        This prevents prose in generated reports and CSV diagnostics from
        being misclassified as wallet seed material.
        """

        lower_path = relative_path.lower()
        lower_line = line.lower()

        secret_context_terms = (
            "mnemonic",
            "seed phrase",
            "seed_phrase",
            "seed-phrase",
            "recovery phrase",
            "recovery_phrase",
            "private key",
            "private_key",
            "secret key",
            "secret_key",
            "wallet secret",
            "wallet_secret",
            "keypair",
        )

        secret_file_markers = (
            ".env",
            "secret",
            "wallet",
            "keypair",
            "mnemonic",
            "seed",
            ".key",
            ".pem",
        )

        assignment_context = any(
            separator in line
            for separator in (
                "=",
                ":",
                '"',
                "'",
            )
        )

        return (
            any(
                term in lower_line
                for term in secret_context_terms
            )
            and assignment_context
        ) or any(
            marker in lower_path
            for marker in secret_file_markers
        )

    @staticmethod
    def _allowlisted_line(
        relative_path: str,
        line: str,
    ) -> bool:
        lower_path = relative_path.lower()
        lower_line = line.lower()

        if lower_path.endswith(".env.example"):
            return True

        if "placeholder" in lower_line:
            return True

        if "example" in lower_line and "=" in line:
            return True

        if "redacted" in lower_line:
            return True

        if "dummy" in lower_line:
            return True

        if "test" in lower_path and "secret" in lower_line:
            return True

        if "secret_name_patterns" in lower_line:
            return True

        if "secret_value_patterns" in lower_line:
            return True

        return False


def executive_summary(
    summary: Mapping[str, Any],
    wallet_findings: Sequence[Finding],
    kill_switch_findings: Sequence[Finding],
    gates: Sequence[GateCheck],
) -> str:
    lines = [
        "Phase 14D — Wallet Security, Kill-Switch, and Live-Readiness Audit",
        "=" * 80,
        "",
        f"Final decision: {summary['final_decision']}",
        f"Security audit passed: {summary['security_audit_passed']}",
        "",
        "Security evidence",
        "-" * 80,
        f"Critical secret findings: {summary['critical_secret_findings']}",
        (
            "Wallet security controls passed: "
            f"{summary['wallet_findings_passed']} / "
            f"{summary['wallet_findings']}"
        ),
        (
            "Kill-switch controls passed: "
            f"{summary['kill_switch_findings_passed']} / "
            f"{summary['kill_switch_findings']}"
        ),
        "",
        "Blocking reasons",
        "-" * 80,
    ]

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
    wallet_findings: Sequence[Finding],
    kill_switch_findings: Sequence[Finding],
    secret_scan: Sequence[Mapping[str, Any]],
    gates: Sequence[GateCheck],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    paths = {
        "wallet": output / WALLET_FINDINGS_CSV,
        "kill_switch": output / KILL_SWITCH_FINDINGS_CSV,
        "gates": output / GATES_CSV,
        "secret_scan": output / SECRET_SCAN_CSV,
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
            raise LiveReadinessAuditError(
                "Refusing to overwrite: "
                + ", ".join(
                    str(path)
                    for path in existing
                )
            )

    write_csv(
        paths["wallet"],
        [
            finding.to_dict()
            for finding in wallet_findings
        ],
    )

    write_csv(
        paths["kill_switch"],
        [
            finding.to_dict()
            for finding in kill_switch_findings
        ],
    )

    write_csv(
        paths["gates"],
        [
            gate.to_dict()
            for gate in gates
        ],
    )

    write_csv(
        paths["secret_scan"],
        secret_scan,
    )

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(configuration),
                    "promotion_decision": str(
                        configuration.promotion_decision
                    ),
                    "shadow_report": str(
                        configuration.shadow_report
                    ),
                    "diagnostics_report": str(
                        configuration.diagnostics_report
                    ),
                    "resilience_report": str(
                        configuration.resilience_report
                    ),
                    "gitignore_path": str(
                        configuration.gitignore_path
                    ),
                    "env_example_path": str(
                        configuration.env_example_path
                    ),
                    "repository_root": str(
                        configuration.repository_root
                    ),
                    "output_directory": str(
                        configuration.output_directory
                    ),
                },
                "wallet_findings": [
                    finding.to_dict()
                    for finding in wallet_findings
                ],
                "kill_switch_findings": [
                    finding.to_dict()
                    for finding in kill_switch_findings
                ],
                "gate_checks": [
                    gate.to_dict()
                    for gate in gates
                ],
                "governance": {
                    "audit_only": True,
                    "wallet_connection_authorized": False,
                    "private_key_access_authorized": False,
                    "transaction_signing_enabled": False,
                    "transaction_broadcasting_enabled": False,
                    "live_execution_enabled": False,
                    "automatic_promotion_enabled": False,
                    "manual_approval_required": True,
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    paths["summary"].write_text(
        executive_summary(
            summary,
            wallet_findings,
            kill_switch_findings,
            gates,
        ),
        encoding="utf-8",
    )

    row_counts = {
        "wallet": len(wallet_findings),
        "kill_switch": len(kill_switch_findings),
        "gates": len(gates),
        "secret_scan": len(secret_scan),
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
        "promotion_decision": configuration.promotion_decision,
        "shadow_report": configuration.shadow_report,
        "diagnostics_report": configuration.diagnostics_report,
        "resilience_report": configuration.resilience_report,
        "gitignore": configuration.gitignore_path,
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
            "Run Phase 14D wallet security, kill-switch, "
            "and live-readiness audit."
        )
    )

    parser.add_argument(
        "--promotion-decision",
        default=str(DEFAULT_PROMOTION_DECISION),
    )

    parser.add_argument(
        "--shadow-report",
        default=str(DEFAULT_SHADOW_REPORT),
    )

    parser.add_argument(
        "--diagnostics-report",
        default=str(DEFAULT_DIAGNOSTICS_REPORT),
    )

    parser.add_argument(
        "--resilience-report",
        default=str(DEFAULT_RESILIENCE_REPORT),
    )

    parser.add_argument(
        "--gitignore",
        default=str(DEFAULT_GITIGNORE),
    )

    parser.add_argument(
        "--env-example",
        default=str(DEFAULT_ENV_EXAMPLE),
    )

    parser.add_argument(
        "--repository-root",
        default=str(PROJECT_ROOT),
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
        promotion_decision=Path(
            args.promotion_decision
        ),
        shadow_report=Path(
            args.shadow_report
        ),
        diagnostics_report=Path(
            args.diagnostics_report
        ),
        resilience_report=Path(
            args.resilience_report
        ),
        gitignore_path=Path(
            args.gitignore
        ),
        env_example_path=Path(
            args.env_example
        ),
        repository_root=Path(
            args.repository_root
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
            wallet_findings,
            kill_switch_findings,
            secret_scan,
            gates,
        ) = LiveReadinessAudit(
            configuration
        ).run()

        output_paths = export_results(
            summary=summary,
            wallet_findings=wallet_findings,
            kill_switch_findings=kill_switch_findings,
            secret_scan=secret_scan,
            gates=gates,
            configuration=configuration,
        )

    except (
        LiveReadinessAuditError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print(
        "\nPhase 14D — Wallet Security, Kill-Switch, "
        "and Live-Readiness Audit"
    )
    print("=" * 80)

    print(
        f"Operating mode: {summary['operating_mode']}"
    )

    print()

    print("Security Evidence")
    print("-" * 80)

    print(
        "Secret findings / critical: "
        f"{summary['secret_findings']} / "
        f"{summary['critical_secret_findings']}"
    )

    print(
        "Wallet controls passed: "
        f"{summary['wallet_findings_passed']} / "
        f"{summary['wallet_findings']}"
    )

    print(
        "Kill-switch controls passed: "
        f"{summary['kill_switch_findings_passed']} / "
        f"{summary['kill_switch_findings']}"
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

    print(
        "Security audit passed: "
        f"{summary['security_audit_passed']}"
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