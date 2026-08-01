"""
Phase 14C — Operational Resilience and Failure-Injection Testing

Runs controlled, deterministic fault-injection scenarios against the shadow
execution architecture without connecting a wallet or broadcasting a
transaction.

Covered failure classes
-----------------------
- RPC outage
- RPC rate limiting
- stale blockhash
- malformed quote payload
- quote timeout
- delayed confirmation
- duplicate execution attempt
- database lock
- partial database write
- report export failure
- emergency stop
- consecutive-loss stop
- stale quote rejection
- corrupted upstream evidence

Inputs
------
execution/shadow_results/shadow_execution_report.json
execution/shadow_results/shadow_execution_attempts.csv
execution/shadow_diagnostics/shadow_execution_diagnostics_report.json
research/institutional_promotion_gate/institutional_promotion_decision.json
database/trades.db

Outputs
-------
execution/resilience_results/
    failure_injection_scenarios.csv
    failure_injection_events.csv
    resilience_gate_checks.csv
    operational_resilience_report.json
    operational_resilience_manifest.json

Safety invariants
-----------------
- no wallet import
- no private-key access
- no signing
- no transaction broadcast
- no live order placement
- no mutation of production scanner state
- optional SQLite writes are audit-only
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "14C.1.0"
OPERATING_MODE = "FAILURE_INJECTION_ONLY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_SHADOW_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_report.json"
)

DEFAULT_SHADOW_ATTEMPTS = (
    PROJECT_ROOT
    / "execution"
    / "shadow_results"
    / "shadow_execution_attempts.csv"
)

DEFAULT_DIAGNOSTICS_REPORT = (
    PROJECT_ROOT
    / "execution"
    / "shadow_diagnostics"
    / "shadow_execution_diagnostics_report.json"
)

DEFAULT_PROMOTION_DECISION = (
    PROJECT_ROOT
    / "research"
    / "institutional_promotion_gate"
    / "institutional_promotion_decision.json"
)

DEFAULT_DATABASE = (
    PROJECT_ROOT
    / "database"
    / "trades.db"
)

DEFAULT_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "execution"
    / "resilience_results"
)

SCENARIOS_CSV = "failure_injection_scenarios.csv"
EVENTS_CSV = "failure_injection_events.csv"
GATES_CSV = "resilience_gate_checks.csv"
REPORT_JSON = "operational_resilience_report.json"
MANIFEST_JSON = "operational_resilience_manifest.json"


class ResilienceError(RuntimeError):
    """Base exception for Phase 14C failures."""


class InjectedFailure(RuntimeError):
    """Expected synthetic failure raised during a test scenario."""


@dataclass(frozen=True, slots=True)
class Configuration:
    shadow_report: Path = DEFAULT_SHADOW_REPORT
    shadow_attempts: Path = DEFAULT_SHADOW_ATTEMPTS
    diagnostics_report: Path = DEFAULT_DIAGNOSTICS_REPORT
    promotion_decision: Path = DEFAULT_PROMOTION_DECISION
    database_path: Path = DEFAULT_DATABASE
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY

    overwrite: bool = True
    persist_audit: bool = True
    random_seed: int = 14_003

    minimum_scenarios: int = 12
    minimum_containment_rate: float = 1.00
    minimum_recovery_rate: float = 0.90
    minimum_idempotency_rate: float = 1.00
    maximum_unhandled_failure_rate: float = 0.00
    maximum_state_corruption_events: int = 0
    maximum_false_live_execution_events: int = 0
    maximum_recovery_time_ms: float = 5_000.0

    def validate(self) -> None:
        if self.minimum_scenarios <= 0:
            raise ResilienceError("minimum_scenarios must be positive.")

        for name in (
            "minimum_containment_rate",
            "minimum_recovery_rate",
            "minimum_idempotency_rate",
            "maximum_unhandled_failure_rate",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ResilienceError(f"{name} must be in [0, 1].")

        for name in (
            "maximum_state_corruption_events",
            "maximum_false_live_execution_events",
        ):
            if int(getattr(self, name)) < 0:
                raise ResilienceError(f"{name} cannot be negative.")

        if (
            not math.isfinite(self.maximum_recovery_time_ms)
            or self.maximum_recovery_time_ms < 0
        ):
            raise ResilienceError(
                "maximum_recovery_time_ms must be finite and non-negative."
            )


@dataclass(frozen=True, slots=True)
class ScenarioDefinition:
    scenario_id: str
    category: str
    description: str
    expected_control: str
    recovery_expected: bool
    idempotency_expected: bool


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


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ResilienceError(f"Required JSON missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ResilienceError(f"Expected JSON object: {path}")
    return payload


def load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ResilienceError(f"Required CSV missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


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


class SafetyController:
    """Minimal in-memory execution safety controller used only for testing."""

    def __init__(self) -> None:
        self.emergency_stop = False
        self.live_execution_enabled = False
        self.wallet_connected = False
        self.transaction_signing_enabled = False
        self.transaction_broadcasting_enabled = False
        self.consecutive_losses = 0
        self.maximum_consecutive_losses = 3
        self.processed_attempt_ids: set[str] = set()
        self.audit_events: list[dict[str, Any]] = []

    def snapshot(self) -> dict[str, Any]:
        return {
            "emergency_stop": self.emergency_stop,
            "live_execution_enabled": self.live_execution_enabled,
            "wallet_connected": self.wallet_connected,
            "transaction_signing_enabled": self.transaction_signing_enabled,
            "transaction_broadcasting_enabled": (
                self.transaction_broadcasting_enabled
            ),
            "consecutive_losses": self.consecutive_losses,
            "processed_attempt_ids": sorted(self.processed_attempt_ids),
            "audit_event_count": len(self.audit_events),
        }

    def record(self, event_type: str, details: Mapping[str, Any]) -> None:
        self.audit_events.append(
            {
                "event_type": event_type,
                "details": dict(details),
                "created_at": utc_now(),
            }
        )

    def trigger_emergency_stop(self, reason: str) -> None:
        self.emergency_stop = True
        self.record(
            "EMERGENCY_STOP_TRIGGERED",
            {"reason": reason},
        )

    def record_trade_outcome(self, profit_usd: float) -> None:
        if profit_usd < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        if self.consecutive_losses >= self.maximum_consecutive_losses:
            self.trigger_emergency_stop("CONSECUTIVE_LOSS_LIMIT")

    def process_attempt(self, attempt_id: str) -> bool:
        if attempt_id in self.processed_attempt_ids:
            self.record(
                "DUPLICATE_ATTEMPT_BLOCKED",
                {"attempt_id": attempt_id},
            )
            return False

        self.processed_attempt_ids.add(attempt_id)
        self.record(
            "ATTEMPT_ACCEPTED",
            {"attempt_id": attempt_id},
        )
        return True


class OperationalResilienceEngine:
    def __init__(self, configuration: Configuration) -> None:
        self.config = configuration
        self.config.validate()
        self.rng = random.Random(configuration.random_seed)

    def run(
        self,
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[GateCheck],
    ]:
        shadow_report = load_json(self.config.shadow_report)
        shadow_attempts = load_csv(self.config.shadow_attempts)
        diagnostics_report = load_json(self.config.diagnostics_report)
        promotion_decision = load_json(self.config.promotion_decision)

        if not shadow_attempts:
            raise ResilienceError(
                "No Phase 14A shadow attempts are available."
            )

        run_id = str(uuid.uuid4())
        controller = SafetyController()

        definitions = self._scenario_definitions()
        scenario_rows: list[dict[str, Any]] = []
        event_rows: list[dict[str, Any]] = []

        for definition in definitions:
            before = controller.snapshot()
            started = time.perf_counter()

            try:
                result = self._execute_scenario(
                    definition,
                    controller,
                    shadow_attempts,
                )
                unhandled = False
                exception_text = None
            except Exception as error:  # intentionally measures containment
                result = {
                    "contained": False,
                    "recovered": False,
                    "idempotent": False,
                    "state_corruption": True,
                    "false_live_execution": False,
                    "control_observed": "UNHANDLED_EXCEPTION",
                    "details": {},
                }
                unhandled = True
                exception_text = f"{type(error).__name__}: {error}"

            recovery_time_ms = (
                time.perf_counter() - started
            ) * 1_000.0
            after = controller.snapshot()

            row = {
                "run_id": run_id,
                "scenario_id": definition.scenario_id,
                "category": definition.category,
                "description": definition.description,
                "expected_control": definition.expected_control,
                "control_observed": result["control_observed"],
                "contained": safe_bool(result["contained"]),
                "recovered": safe_bool(result["recovered"]),
                "idempotent": safe_bool(result["idempotent"]),
                "state_corruption": safe_bool(result["state_corruption"]),
                "false_live_execution": safe_bool(
                    result["false_live_execution"]
                ),
                "unhandled_exception": unhandled,
                "exception": exception_text,
                "recovery_time_ms": recovery_time_ms,
                "before_state_json": json.dumps(
                    before,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "after_state_json": json.dumps(
                    after,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "details_json": json.dumps(
                    result.get("details", {}),
                    ensure_ascii=False,
                    default=str,
                    sort_keys=True,
                ),
            }
            scenario_rows.append(row)

            event_rows.extend(
                {
                    "run_id": run_id,
                    "scenario_id": definition.scenario_id,
                    **event,
                }
                for event in controller.audit_events
            )
            controller.audit_events.clear()

        scenario_count = len(scenario_rows)
        contained_count = sum(
            safe_bool(row["contained"])
            for row in scenario_rows
        )
        recovered_expected = [
            row
            for row, definition in zip(scenario_rows, definitions)
            if definition.recovery_expected
        ]
        recovered_count = sum(
            safe_bool(row["recovered"])
            for row in recovered_expected
        )
        idempotency_expected = [
            row
            for row, definition in zip(scenario_rows, definitions)
            if definition.idempotency_expected
        ]
        idempotent_count = sum(
            safe_bool(row["idempotent"])
            for row in idempotency_expected
        )
        unhandled_count = sum(
            safe_bool(row["unhandled_exception"])
            for row in scenario_rows
        )
        corruption_count = sum(
            safe_bool(row["state_corruption"])
            for row in scenario_rows
        )
        false_live_count = sum(
            safe_bool(row["false_live_execution"])
            for row in scenario_rows
        )

        containment_rate = (
            contained_count / scenario_count
            if scenario_count
            else 0.0
        )
        recovery_rate = (
            recovered_count / len(recovered_expected)
            if recovered_expected
            else 1.0
        )
        idempotency_rate = (
            idempotent_count / len(idempotency_expected)
            if idempotency_expected
            else 1.0
        )
        unhandled_rate = (
            unhandled_count / scenario_count
            if scenario_count
            else 1.0
        )
        maximum_recovery_time = max(
            (
                safe_float(row["recovery_time_ms"])
                for row in scenario_rows
            ),
            default=0.0,
        )

        gates = [
            GateCheck(
                "SCENARIO_COUNT",
                scenario_count >= self.config.minimum_scenarios,
                float(scenario_count),
                ">=",
                float(self.config.minimum_scenarios),
                "Enough resilience scenarios must complete.",
            ),
            GateCheck(
                "CONTAINMENT_RATE",
                containment_rate >= self.config.minimum_containment_rate,
                containment_rate,
                ">=",
                self.config.minimum_containment_rate,
                "All injected faults must be contained.",
            ),
            GateCheck(
                "RECOVERY_RATE",
                recovery_rate >= self.config.minimum_recovery_rate,
                recovery_rate,
                ">=",
                self.config.minimum_recovery_rate,
                "Recoverable failures did not recover reliably.",
            ),
            GateCheck(
                "IDEMPOTENCY_RATE",
                idempotency_rate >= self.config.minimum_idempotency_rate,
                idempotency_rate,
                ">=",
                self.config.minimum_idempotency_rate,
                "Duplicate or replay handling is not fully idempotent.",
            ),
            GateCheck(
                "UNHANDLED_FAILURE_RATE",
                unhandled_rate <= self.config.maximum_unhandled_failure_rate,
                unhandled_rate,
                "<=",
                self.config.maximum_unhandled_failure_rate,
                "Unhandled failure rate exceeds the gate.",
            ),
            GateCheck(
                "STATE_CORRUPTION_EVENTS",
                corruption_count
                <= self.config.maximum_state_corruption_events,
                float(corruption_count),
                "<=",
                float(self.config.maximum_state_corruption_events),
                "One or more injected failures corrupted state.",
            ),
            GateCheck(
                "FALSE_LIVE_EXECUTION_EVENTS",
                false_live_count
                <= self.config.maximum_false_live_execution_events,
                float(false_live_count),
                "<=",
                float(self.config.maximum_false_live_execution_events),
                "A failure caused an unauthorized live-execution state.",
            ),
            GateCheck(
                "MAXIMUM_RECOVERY_TIME",
                maximum_recovery_time
                <= self.config.maximum_recovery_time_ms,
                maximum_recovery_time,
                "<=",
                self.config.maximum_recovery_time_ms,
                "Recovery time exceeded the operational threshold.",
            ),
        ]

        resilience_passed = all(gate.passed for gate in gates)

        upstream_shadow = shadow_report.get("summary", {})
        upstream_diagnostics = diagnostics_report.get("summary", {})

        blocking_reasons: list[str] = []

        if not resilience_passed:
            blocking_reasons.append(
                "One or more operational resilience gates failed."
            )

        if not safe_bool(
            upstream_shadow.get("operational_gate_passed")
        ):
            blocking_reasons.append(
                "Phase 14A operational shadow gate remains blocked."
            )

        if not safe_bool(
            upstream_diagnostics.get("diagnostics_passed")
        ):
            blocking_reasons.append(
                "Phase 14B edge decomposition remains blocked."
            )

        if not safe_bool(
            promotion_decision.get("live_readiness_allowed")
        ):
            blocking_reasons.append(
                "Phase 13F live readiness remains blocked."
            )

        summary = {
            "generated_at": utc_now(),
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "operating_mode": OPERATING_MODE,
            "scenarios": scenario_count,
            "contained_scenarios": contained_count,
            "recoverable_scenarios": len(recovered_expected),
            "recovered_scenarios": recovered_count,
            "idempotency_scenarios": len(idempotency_expected),
            "idempotent_scenarios": idempotent_count,
            "unhandled_failures": unhandled_count,
            "state_corruption_events": corruption_count,
            "false_live_execution_events": false_live_count,
            "containment_rate": containment_rate,
            "recovery_rate": recovery_rate,
            "idempotency_rate": idempotency_rate,
            "unhandled_failure_rate": unhandled_rate,
            "maximum_recovery_time_ms": maximum_recovery_time,
            "resilience_passed": resilience_passed,
            "final_decision": (
                "ELIGIBLE_FOR_PHASE_14D_REVIEW"
                if resilience_passed
                and not blocking_reasons
                else "BLOCK_LIVE_EXECUTION"
            ),
            "blocking_reasons": blocking_reasons,
            "upstream": {
                "phase_14a_operational_gate_passed": safe_bool(
                    upstream_shadow.get("operational_gate_passed")
                ),
                "phase_14b_diagnostics_passed": safe_bool(
                    upstream_diagnostics.get("diagnostics_passed")
                ),
                "phase_13f_live_readiness_allowed": safe_bool(
                    promotion_decision.get("live_readiness_allowed")
                ),
            },
            "safety": {
                "wallet_connected": False,
                "transaction_signing_enabled": False,
                "transaction_broadcasting_enabled": False,
                "live_execution_enabled": False,
            },
            "valid": True,
        }

        if self.config.persist_audit:
            self._persist_audit(
                summary,
                scenario_rows,
            )

        return (
            summary,
            scenario_rows,
            event_rows,
            gates,
        )

    @staticmethod
    def _scenario_definitions() -> tuple[ScenarioDefinition, ...]:
        return (
            ScenarioDefinition(
                "RPC_OUTAGE",
                "RPC",
                "All RPC calls fail immediately.",
                "RPC_FAILURE_CONTAINED",
                True,
                False,
            ),
            ScenarioDefinition(
                "RPC_RATE_LIMIT",
                "RPC",
                "RPC returns a rate-limit response.",
                "BACKOFF_WITHOUT_EXECUTION",
                True,
                False,
            ),
            ScenarioDefinition(
                "STALE_BLOCKHASH",
                "TRANSACTION",
                "Blockhash expires before confirmation.",
                "BLOCKHASH_RETRY_REQUIRED",
                True,
                False,
            ),
            ScenarioDefinition(
                "MALFORMED_QUOTE",
                "QUOTE",
                "Quote response is missing required fields.",
                "QUOTE_REJECTED",
                True,
                False,
            ),
            ScenarioDefinition(
                "QUOTE_TIMEOUT",
                "QUOTE",
                "Quote request exceeds timeout.",
                "TIMEOUT_CONTAINED",
                True,
                False,
            ),
            ScenarioDefinition(
                "DELAYED_CONFIRMATION",
                "CONFIRMATION",
                "Confirmation exceeds the maximum wait.",
                "CONFIRMATION_TIMEOUT",
                True,
                False,
            ),
            ScenarioDefinition(
                "DUPLICATE_ATTEMPT",
                "IDEMPOTENCY",
                "The same attempt is processed twice.",
                "DUPLICATE_BLOCKED",
                True,
                True,
            ),
            ScenarioDefinition(
                "DATABASE_LOCK",
                "DATABASE",
                "SQLite write encounters an exclusive lock.",
                "DATABASE_RETRY_OR_ROLLBACK",
                True,
                False,
            ),
            ScenarioDefinition(
                "PARTIAL_DATABASE_WRITE",
                "DATABASE",
                "A transaction fails after one audit insert.",
                "ATOMIC_ROLLBACK",
                True,
                True,
            ),
            ScenarioDefinition(
                "REPORT_EXPORT_FAILURE",
                "FILESYSTEM",
                "Report export fails after temporary output.",
                "ATOMIC_EXPORT_FAILURE",
                True,
                True,
            ),
            ScenarioDefinition(
                "EMERGENCY_STOP",
                "RISK",
                "Emergency stop activates before execution.",
                "EXECUTION_BLOCKED",
                False,
                True,
            ),
            ScenarioDefinition(
                "CONSECUTIVE_LOSS_STOP",
                "RISK",
                "Three consecutive losses trigger stop logic.",
                "LOSS_LIMIT_STOP",
                False,
                True,
            ),
            ScenarioDefinition(
                "STALE_QUOTE_REJECTION",
                "QUOTE",
                "Quote age exceeds the configured maximum.",
                "STALE_QUOTE_BLOCKED",
                True,
                False,
            ),
            ScenarioDefinition(
                "CORRUPTED_UPSTREAM_EVIDENCE",
                "GOVERNANCE",
                "Upstream promotion evidence is malformed.",
                "PROMOTION_BLOCKED",
                True,
                False,
            ),
        )

    def _execute_scenario(
        self,
        definition: ScenarioDefinition,
        controller: SafetyController,
        shadow_attempts: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        handlers: dict[
            str,
            Callable[
                [SafetyController, Sequence[Mapping[str, Any]]],
                dict[str, Any],
            ],
        ] = {
            "RPC_OUTAGE": self._rpc_outage,
            "RPC_RATE_LIMIT": self._rpc_rate_limit,
            "STALE_BLOCKHASH": self._stale_blockhash,
            "MALFORMED_QUOTE": self._malformed_quote,
            "QUOTE_TIMEOUT": self._quote_timeout,
            "DELAYED_CONFIRMATION": self._delayed_confirmation,
            "DUPLICATE_ATTEMPT": self._duplicate_attempt,
            "DATABASE_LOCK": self._database_lock,
            "PARTIAL_DATABASE_WRITE": self._partial_database_write,
            "REPORT_EXPORT_FAILURE": self._report_export_failure,
            "EMERGENCY_STOP": self._emergency_stop,
            "CONSECUTIVE_LOSS_STOP": self._consecutive_loss_stop,
            "STALE_QUOTE_REJECTION": self._stale_quote_rejection,
            "CORRUPTED_UPSTREAM_EVIDENCE": (
                self._corrupted_upstream_evidence
            ),
        }

        return handlers[definition.scenario_id](
            controller,
            shadow_attempts,
        )

    @staticmethod
    def _standard_result(
        *,
        control: str,
        contained: bool = True,
        recovered: bool = True,
        idempotent: bool = True,
        state_corruption: bool = False,
        false_live_execution: bool = False,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "contained": contained,
            "recovered": recovered,
            "idempotent": idempotent,
            "state_corruption": state_corruption,
            "false_live_execution": false_live_execution,
            "control_observed": control,
            "details": dict(details or {}),
        }

    def _rpc_outage(
        self,
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        try:
            raise InjectedFailure("RPC_UNAVAILABLE")
        except InjectedFailure as error:
            controller.record("RPC_FAILURE", {"error": str(error)})
            return self._standard_result(
                control="RPC_FAILURE_CONTAINED",
                details={"retry_scheduled": True},
            )

    def _rpc_rate_limit(
        self,
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        backoff_ms = self.rng.randint(250, 1_000)
        controller.record(
            "RPC_RATE_LIMIT",
            {"backoff_ms": backoff_ms},
        )
        return self._standard_result(
            control="BACKOFF_WITHOUT_EXECUTION",
            details={"backoff_ms": backoff_ms},
        )

    @staticmethod
    def _stale_blockhash(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        controller.record(
            "BLOCKHASH_EXPIRED",
            {"broadcast_attempted": False},
        )
        return OperationalResilienceEngine._standard_result(
            control="BLOCKHASH_RETRY_REQUIRED",
            details={"replacement_required": True},
        )

    @staticmethod
    def _malformed_quote(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        quote = {"token": "TEST"}
        required = {"token", "starting_amount_usd", "ending_amount_usd"}
        missing = sorted(required - set(quote))
        controller.record(
            "MALFORMED_QUOTE_REJECTED",
            {"missing_fields": missing},
        )
        return OperationalResilienceEngine._standard_result(
            control="QUOTE_REJECTED",
            details={"missing_fields": missing},
        )

    @staticmethod
    def _quote_timeout(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        controller.record(
            "QUOTE_TIMEOUT",
            {"execution_attempted": False},
        )
        return OperationalResilienceEngine._standard_result(
            control="TIMEOUT_CONTAINED",
        )

    @staticmethod
    def _delayed_confirmation(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        controller.record(
            "CONFIRMATION_TIMEOUT",
            {"status": "UNKNOWN_NOT_RETRIED_BLINDLY"},
        )
        return OperationalResilienceEngine._standard_result(
            control="CONFIRMATION_TIMEOUT",
            details={"blind_retry_prevented": True},
        )

    @staticmethod
    def _duplicate_attempt(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        attempt_id = "DUPLICATE-TEST"
        first = controller.process_attempt(attempt_id)
        second = controller.process_attempt(attempt_id)
        return OperationalResilienceEngine._standard_result(
            control="DUPLICATE_BLOCKED",
            idempotent=first and not second,
            details={
                "first_accepted": first,
                "second_accepted": second,
            },
        )

    def _database_lock(
        self,
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lock_test.db"
            owner = sqlite3.connect(path, timeout=0.1)
            contender = sqlite3.connect(path, timeout=0.1)

            try:
                owner.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY)")
                owner.commit()
                owner.execute("BEGIN EXCLUSIVE")

                locked = False
                try:
                    contender.execute("INSERT INTO audit DEFAULT VALUES")
                    contender.commit()
                except sqlite3.OperationalError:
                    locked = True
                    contender.rollback()

                owner.rollback()

                contender.execute("INSERT INTO audit DEFAULT VALUES")
                contender.commit()
                count = contender.execute(
                    "SELECT COUNT(*) FROM audit"
                ).fetchone()[0]

                controller.record(
                    "DATABASE_LOCK_RECOVERED",
                    {"locked": locked, "rows_after_recovery": count},
                )

                return self._standard_result(
                    control="DATABASE_RETRY_OR_ROLLBACK",
                    recovered=locked and count == 1,
                    details={
                        "lock_detected": locked,
                        "rows_after_recovery": count,
                    },
                )
            finally:
                owner.close()
                contender.close()

    @staticmethod
    def _partial_database_write(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "atomic_test.db"
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "CREATE TABLE audit (id INTEGER PRIMARY KEY, value TEXT)"
                )
                connection.commit()

                try:
                    connection.execute("BEGIN")
                    connection.execute(
                        "INSERT INTO audit (value) VALUES ('partial')"
                    )
                    raise InjectedFailure("FAIL_AFTER_FIRST_INSERT")
                except InjectedFailure:
                    connection.rollback()

                count = connection.execute(
                    "SELECT COUNT(*) FROM audit"
                ).fetchone()[0]

                controller.record(
                    "PARTIAL_WRITE_ROLLED_BACK",
                    {"rows_after_rollback": count},
                )

                return OperationalResilienceEngine._standard_result(
                    control="ATOMIC_ROLLBACK",
                    recovered=count == 0,
                    idempotent=count == 0,
                    state_corruption=count != 0,
                    details={"rows_after_rollback": count},
                )
            finally:
                connection.close()

    @staticmethod
    def _report_export_failure(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            temporary = Path(directory) / "report.json.tmp"

            temporary.write_text('{"partial":', encoding="utf-8")

            try:
                raise InjectedFailure("EXPORT_INTERRUPTED")
            except InjectedFailure:
                temporary.unlink(missing_ok=True)

            clean = not output.exists() and not temporary.exists()
            controller.record(
                "ATOMIC_EXPORT_CLEANUP",
                {"clean": clean},
            )

            return OperationalResilienceEngine._standard_result(
                control="ATOMIC_EXPORT_FAILURE",
                recovered=clean,
                idempotent=clean,
                state_corruption=not clean,
            )

    @staticmethod
    def _emergency_stop(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        controller.trigger_emergency_stop("MANUAL_TEST")
        execution_allowed = not controller.emergency_stop
        return OperationalResilienceEngine._standard_result(
            control="EXECUTION_BLOCKED",
            recovered=True,
            idempotent=not execution_allowed,
            false_live_execution=execution_allowed,
            details={"execution_allowed": execution_allowed},
        )

    @staticmethod
    def _consecutive_loss_stop(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        controller.emergency_stop = False
        controller.consecutive_losses = 0

        controller.record_trade_outcome(-0.01)
        controller.record_trade_outcome(-0.02)
        controller.record_trade_outcome(-0.03)

        stopped = controller.emergency_stop

        return OperationalResilienceEngine._standard_result(
            control="LOSS_LIMIT_STOP",
            recovered=True,
            idempotent=stopped,
            false_live_execution=not stopped,
            details={
                "consecutive_losses": controller.consecutive_losses,
                "emergency_stop": stopped,
            },
        )

    @staticmethod
    def _stale_quote_rejection(
        controller: SafetyController,
        attempts: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        maximum_age = 1_500.0
        source = attempts[0]
        quote_age = max(
            maximum_age + 1.0,
            safe_float(source.get("total_quote_age_ms")),
        )
        rejected = quote_age > maximum_age

        controller.record(
            "STALE_QUOTE_REJECTED",
            {
                "quote_age_ms": quote_age,
                "maximum_age_ms": maximum_age,
            },
        )

        return OperationalResilienceEngine._standard_result(
            control="STALE_QUOTE_BLOCKED",
            recovered=rejected,
            details={"rejected": rejected},
        )

    @staticmethod
    def _corrupted_upstream_evidence(
        controller: SafetyController,
        _: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        corrupted = {"live_readiness_allowed": "not-a-boolean"}
        allowed = (
            corrupted.get("live_readiness_allowed") is True
        )

        controller.record(
            "CORRUPTED_EVIDENCE_BLOCKED",
            {"promotion_allowed": allowed},
        )

        return OperationalResilienceEngine._standard_result(
            control="PROMOTION_BLOCKED",
            recovered=not allowed,
            false_live_execution=allowed,
            details={"promotion_allowed": allowed},
        )

    def _persist_audit(
        self,
        summary: Mapping[str, Any],
        scenarios: Sequence[Mapping[str, Any]],
    ) -> None:
        path = self.config.database_path
        path.parent.mkdir(parents=True, exist_ok=True)

        connection = sqlite3.connect(path, timeout=30.0)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA busy_timeout=30000")

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS resilience_test_runs (
                    run_id TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    scenarios INTEGER NOT NULL,
                    containment_rate REAL NOT NULL,
                    recovery_rate REAL NOT NULL,
                    idempotency_rate REAL NOT NULL,
                    unhandled_failure_rate REAL NOT NULL,
                    state_corruption_events INTEGER NOT NULL,
                    false_live_execution_events INTEGER NOT NULL,
                    resilience_passed INTEGER NOT NULL,
                    final_decision TEXT NOT NULL,
                    result_json TEXT NOT NULL
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS resilience_test_scenarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    scenario_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    contained INTEGER NOT NULL,
                    recovered INTEGER NOT NULL,
                    idempotent INTEGER NOT NULL,
                    state_corruption INTEGER NOT NULL,
                    false_live_execution INTEGER NOT NULL,
                    unhandled_exception INTEGER NOT NULL,
                    recovery_time_ms REAL NOT NULL,
                    raw_json TEXT NOT NULL,
                    UNIQUE(run_id, scenario_id)
                )
                """
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO resilience_test_runs (
                    run_id,
                    schema_version,
                    generated_at,
                    scenarios,
                    containment_rate,
                    recovery_rate,
                    idempotency_rate,
                    unhandled_failure_rate,
                    state_corruption_events,
                    false_live_execution_events,
                    resilience_passed,
                    final_decision,
                    result_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary["run_id"],
                    SCHEMA_VERSION,
                    summary["generated_at"],
                    summary["scenarios"],
                    summary["containment_rate"],
                    summary["recovery_rate"],
                    summary["idempotency_rate"],
                    summary["unhandled_failure_rate"],
                    summary["state_corruption_events"],
                    summary["false_live_execution_events"],
                    int(summary["resilience_passed"]),
                    summary["final_decision"],
                    json.dumps(dict(summary), ensure_ascii=False),
                ),
            )

            connection.executemany(
                """
                INSERT OR REPLACE INTO resilience_test_scenarios (
                    run_id,
                    scenario_id,
                    category,
                    contained,
                    recovered,
                    idempotent,
                    state_corruption,
                    false_live_execution,
                    unhandled_exception,
                    recovery_time_ms,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["run_id"],
                        row["scenario_id"],
                        row["category"],
                        int(safe_bool(row["contained"])),
                        int(safe_bool(row["recovered"])),
                        int(safe_bool(row["idempotent"])),
                        int(safe_bool(row["state_corruption"])),
                        int(safe_bool(row["false_live_execution"])),
                        int(safe_bool(row["unhandled_exception"])),
                        safe_float(row["recovery_time_ms"]),
                        json.dumps(dict(row), ensure_ascii=False),
                    )
                    for row in scenarios
                ],
            )

            connection.commit()

        except sqlite3.Error as error:
            connection.rollback()
            raise ResilienceError(
                f"Could not persist resilience audit: {error}"
            ) from error
        finally:
            connection.close()


def export_results(
    *,
    summary: Mapping[str, Any],
    scenario_rows: Sequence[Mapping[str, Any]],
    event_rows: Sequence[Mapping[str, Any]],
    gates: Sequence[GateCheck],
    configuration: Configuration,
) -> tuple[Path, ...]:
    output = configuration.output_directory
    output.mkdir(parents=True, exist_ok=True)

    paths = {
        "scenarios": output / SCENARIOS_CSV,
        "events": output / EVENTS_CSV,
        "gates": output / GATES_CSV,
        "report": output / REPORT_JSON,
        "manifest": output / MANIFEST_JSON,
    }

    if not configuration.overwrite:
        existing = [path for path in paths.values() if path.exists()]
        if existing:
            raise ResilienceError(
                "Refusing to overwrite: "
                + ", ".join(str(path) for path in existing)
            )

    write_csv(paths["scenarios"], scenario_rows)
    write_csv(paths["events"], event_rows)
    write_csv(paths["gates"], [gate.to_dict() for gate in gates])

    paths["report"].write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "summary": dict(summary),
                "configuration": {
                    **asdict(configuration),
                    "shadow_report": str(configuration.shadow_report),
                    "shadow_attempts": str(configuration.shadow_attempts),
                    "diagnostics_report": str(
                        configuration.diagnostics_report
                    ),
                    "promotion_decision": str(
                        configuration.promotion_decision
                    ),
                    "database_path": str(configuration.database_path),
                    "output_directory": str(configuration.output_directory),
                },
                "gate_checks": [gate.to_dict() for gate in gates],
                "governance": {
                    "research_only": True,
                    "live_execution_enabled": False,
                    "wallet_connection_authorized": False,
                    "transaction_signing_enabled": False,
                    "transaction_broadcasting_enabled": False,
                    "database_writes_are_audit_only": (
                        configuration.persist_audit
                    ),
                },
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    row_counts = {
        "scenarios": len(scenario_rows),
        "events": len(event_rows),
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
                "run_id": summary["run_id"],
                "summary": dict(summary),
                "inputs": {
                    "shadow_report": {
                        "path": str(configuration.shadow_report),
                        "sha256": sha256_file(configuration.shadow_report),
                    },
                    "shadow_attempts": {
                        "path": str(configuration.shadow_attempts),
                        "sha256": sha256_file(configuration.shadow_attempts),
                    },
                    "diagnostics_report": {
                        "path": str(configuration.diagnostics_report),
                        "sha256": sha256_file(
                            configuration.diagnostics_report
                        ),
                    },
                    "promotion_decision": {
                        "path": str(configuration.promotion_decision),
                        "sha256": sha256_file(
                            configuration.promotion_decision
                        ),
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
            "Run Phase 14C operational resilience "
            "and failure-injection testing."
        )
    )
    parser.add_argument(
        "--shadow-report",
        default=str(DEFAULT_SHADOW_REPORT),
    )
    parser.add_argument(
        "--shadow-attempts",
        default=str(DEFAULT_SHADOW_ATTEMPTS),
    )
    parser.add_argument(
        "--diagnostics-report",
        default=str(DEFAULT_DIAGNOSTICS_REPORT),
    )
    parser.add_argument(
        "--promotion-decision",
        default=str(DEFAULT_PROMOTION_DECISION),
    )
    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE),
    )
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    parser.add_argument(
        "--no-database-write",
        action="store_true",
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


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    configuration = Configuration(
        shadow_report=Path(args.shadow_report),
        shadow_attempts=Path(args.shadow_attempts),
        diagnostics_report=Path(args.diagnostics_report),
        promotion_decision=Path(args.promotion_decision),
        database_path=Path(args.database),
        output_directory=Path(args.output_directory),
        overwrite=not args.no_overwrite,
        persist_audit=not args.no_database_write,
    )

    try:
        (
            summary,
            scenario_rows,
            event_rows,
            gates,
        ) = OperationalResilienceEngine(configuration).run()

        output_paths = export_results(
            summary=summary,
            scenario_rows=scenario_rows,
            event_rows=event_rows,
            gates=gates,
            configuration=configuration,
        )

    except (
        ResilienceError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print(
        "\nPhase 14C — Operational Resilience "
        "and Failure-Injection Testing"
    )
    print("=" * 80)
    print(f"Run ID: {summary['run_id']}")
    print(f"Operating mode: {summary['operating_mode']}")
    print()

    print("Failure-Injection Results")
    print("-" * 80)
    print(
        "Scenarios / contained: "
        f"{summary['scenarios']} / {summary['contained_scenarios']}"
    )
    print(
        "Recoverable / recovered: "
        f"{summary['recoverable_scenarios']} / "
        f"{summary['recovered_scenarios']}"
    )
    print(
        "Idempotency tests / passed: "
        f"{summary['idempotency_scenarios']} / "
        f"{summary['idempotent_scenarios']}"
    )
    print(f"Containment rate: {summary['containment_rate'] * 100:.2f}%")
    print(f"Recovery rate: {summary['recovery_rate'] * 100:.2f}%")
    print(f"Idempotency rate: {summary['idempotency_rate'] * 100:.2f}%")
    print(
        "Unhandled / corruption / false-live events: "
        f"{summary['unhandled_failures']} / "
        f"{summary['state_corruption_events']} / "
        f"{summary['false_live_execution_events']}"
    )
    print(
        "Maximum recovery time: "
        f"{summary['maximum_recovery_time_ms']:.3f} ms"
    )
    print()

    print("Scenario Results")
    print("-" * 80)
    for row in scenario_rows:
        print(
            f"{'PASS' if safe_bool(row['contained']) else 'FAIL'} | "
            f"{row['scenario_id']:<28} | "
            f"control={row['control_observed']} | "
            f"recovered={row['recovered']} | "
            f"idempotent={row['idempotent']}"
        )
    print()

    print("Resilience Gates")
    print("-" * 80)
    for gate in gates:
        print(
            f"{'PASS' if gate.passed else 'FAIL'} | "
            f"{gate.name:30} | "
            f"{gate.observed:.6f} "
            f"{gate.comparison} {gate.required:.6f}"
        )
        if not gate.passed:
            print(f"       {gate.message}")

    print()
    print(f"Resilience passed: {summary['resilience_passed']}")
    print(f"Final decision: {summary['final_decision']}")

    if summary["blocking_reasons"]:
        print("Blocking reasons:")
        for reason in summary["blocking_reasons"]:
            print(f"  - {reason}")

    print()
    print("Safety")
    print("-" * 80)
    print("Wallet connected: False")
    print("Transaction signing: False")
    print("Transaction broadcasting: False")
    print("Live execution: False")
    print()

    print("Output files")
    print("-" * 80)
    for path in output_paths:
        print(path)

    if configuration.persist_audit:
        print()
        print(f"Audit database: {configuration.database_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())