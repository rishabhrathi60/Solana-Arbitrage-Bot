import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)

OPERATING_MODE = "PAPER_RISK_CONTROL"

# ---------------------------------------------------------
# Paper-mode risk and discipline limits
# ---------------------------------------------------------

STARTING_PAPER_CAPITAL_USD = 1000.0

MAXIMUM_RISK_PER_TRADE_PERCENT = 0.25
MAXIMUM_POSITION_PERCENT = 5.0

MAXIMUM_DAILY_LOSS_USD = 10.0
MAXIMUM_DAILY_DRAWDOWN_USD = 15.0
MAXIMUM_CONSECUTIVE_LOSSES = 3
MAXIMUM_TRADES_PER_DAY = 10

COOLDOWN_MINUTES_AFTER_LOSS_LIMIT = 120
COOLDOWN_MINUTES_AFTER_DRAWDOWN = 240

MINIMUM_DECISION_SCORE = 72.0
MINIMUM_DECISION_CONFIDENCE = 35.0
MAXIMUM_DECISION_RISK = 35.0
MINIMUM_EXPECTED_PROFIT_USD = 0.000001
MINIMUM_EXECUTION_VOTES = 4

MINIMUM_REWARD_TO_RISK_RATIO = 1.20
MINIMUM_EDGE_TO_COST_RATIO = 1.10

DEFAULT_ESTIMATED_LOSS_USD = 0.05
DEFAULT_ESTIMATED_COST_USD = 0.001

PROCESS_SCORE_REQUIRED = 80.0


def get_database_connection():
    """
    Create a SQLite connection with dictionary-like rows.
    """

    connection = sqlite3.connect(
        DATABASE_FILE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row

    return connection


def current_timestamp():
    """
    Return the current local timestamp.
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def current_date():
    """
    Return the current local calendar date.
    """

    return datetime.now().strftime(
        "%Y-%m-%d"
    )


def safe_float(value):
    """
    Convert a value to float safely.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    """
    Convert a value to integer safely.
    """

    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def clamp(
    value,
    minimum=0.0,
    maximum=100.0,
):
    """
    Restrict a numeric value to a safe range.
    """

    return max(
        minimum,
        min(maximum, safe_float(value)),
    )


def initialize_risk_manager():
    """
    Create all Phase 9 risk-control and audit tables.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),

                starting_capital_usd REAL NOT NULL,
                current_capital_usd REAL NOT NULL,
                peak_capital_usd REAL NOT NULL,

                consecutive_losses INTEGER NOT NULL DEFAULT 0,
                lifetime_trades INTEGER NOT NULL DEFAULT 0,
                lifetime_wins INTEGER NOT NULL DEFAULT 0,
                lifetime_losses INTEGER NOT NULL DEFAULT 0,
                lifetime_profit_usd REAL NOT NULL DEFAULT 0,

                cooldown_until TEXT,
                emergency_stop INTEGER NOT NULL DEFAULT 0,
                emergency_reason TEXT,

                operating_mode TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_risk_state (
                trade_date TEXT PRIMARY KEY,

                starting_capital_usd REAL NOT NULL,
                current_capital_usd REAL NOT NULL,
                peak_capital_usd REAL NOT NULL,

                trades_taken INTEGER NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,

                realized_profit_usd REAL NOT NULL DEFAULT 0,
                maximum_drawdown_usd REAL NOT NULL DEFAULT 0,

                blocked_decisions INTEGER NOT NULL DEFAULT 0,
                approved_decisions INTEGER NOT NULL DEFAULT 0,

                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_decision_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                cycle_id TEXT NOT NULL,
                mint TEXT,
                symbol TEXT NOT NULL,

                recommendation_before_risk TEXT NOT NULL,
                approved INTEGER NOT NULL DEFAULT 0,
                risk_action TEXT NOT NULL,

                process_score REAL NOT NULL DEFAULT 0,
                proposed_position_usd REAL NOT NULL DEFAULT 0,
                maximum_loss_usd REAL NOT NULL DEFAULT 0,
                reward_to_risk_ratio REAL NOT NULL DEFAULT 0,
                edge_to_cost_ratio REAL NOT NULL DEFAULT 0,

                decision_score REAL NOT NULL DEFAULT 0,
                decision_confidence REAL NOT NULL DEFAULT 0,
                decision_risk REAL NOT NULL DEFAULT 0,
                expected_profit_usd REAL NOT NULL DEFAULT 0,
                execution_votes INTEGER NOT NULL DEFAULT 0,

                daily_loss_usd REAL NOT NULL DEFAULT 0,
                daily_drawdown_usd REAL NOT NULL DEFAULT 0,
                consecutive_losses INTEGER NOT NULL DEFAULT 0,
                trades_today INTEGER NOT NULL DEFAULT 0,

                blocked_reasons_json TEXT NOT NULL,
                explanation TEXT NOT NULL,

                actual_profit_usd REAL,
                outcome_recorded INTEGER NOT NULL DEFAULT 0,

                operating_mode TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                UNIQUE(cycle_id, mint, symbol)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_risk_audit_cycle
            ON risk_decision_audit(cycle_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_risk_audit_approved
            ON risk_decision_audit(approved)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_risk_audit_created
            ON risk_decision_audit(created_at)
            """
        )

        cursor.execute(
            """
            INSERT OR IGNORE INTO risk_state (
                id,
                starting_capital_usd,
                current_capital_usd,
                peak_capital_usd,
                operating_mode,
                created_at,
                updated_at
            )
            VALUES (
                1, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                STARTING_PAPER_CAPITAL_USD,
                STARTING_PAPER_CAPITAL_USD,
                STARTING_PAPER_CAPITAL_USD,
                OPERATING_MODE,
                current_timestamp(),
                current_timestamp(),
            ),
        )

        cursor.execute(
            """
            SELECT current_capital_usd
            FROM risk_state
            WHERE id = 1
            """
        )

        state = cursor.fetchone()

        capital = safe_float(
            state["current_capital_usd"]
            if state
            else STARTING_PAPER_CAPITAL_USD
        )

        cursor.execute(
            """
            INSERT OR IGNORE INTO daily_risk_state (
                trade_date,
                starting_capital_usd,
                current_capital_usd,
                peak_capital_usd,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                current_date(),
                capital,
                capital,
                capital,
                current_timestamp(),
                current_timestamp(),
            ),
        )

        connection.commit()

    finally:
        connection.close()


def get_risk_state():
    """
    Return current lifetime and daily risk state.
    """

    initialize_risk_manager()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM risk_state
            WHERE id = 1
            """
        )

        lifetime = cursor.fetchone()

        cursor.execute(
            """
            SELECT *
            FROM daily_risk_state
            WHERE trade_date = ?
            """,
            (current_date(),),
        )

        daily = cursor.fetchone()

    finally:
        connection.close()

    return {
        "lifetime": (
            dict(lifetime)
            if lifetime
            else {}
        ),
        "daily": (
            dict(daily)
            if daily
            else {}
        ),
    }


def _cooldown_active(
    cooldown_until,
):
    """
    Return whether the stored cooldown is currently active.
    """

    if not cooldown_until:
        return False

    try:
        cooldown_time = datetime.strptime(
            cooldown_until,
            "%Y-%m-%d %H:%M:%S",
        )
    except (TypeError, ValueError):
        return False

    return datetime.now() < cooldown_time


def _estimated_maximum_loss(
    result,
):
    """
    Estimate the maximum paper loss for position sizing.
    """

    estimated_cost = abs(
        safe_float(
            result.get("estimated_cost")
        )
    )

    downside_risk = clamp(
        result.get("downside_risk_score")
        or result.get("risk_score")
        or 50.0
    )

    expected_profit = abs(
        safe_float(
            result.get("expected_profit_usd")
        )
    )

    risk_based_loss = (
        max(
            expected_profit,
            DEFAULT_ESTIMATED_LOSS_USD,
        )
        * max(
            0.10,
            downside_risk / 100.0,
        )
    )

    return max(
        DEFAULT_ESTIMATED_COST_USD,
        estimated_cost,
        risk_based_loss,
    )


def _position_size(
    capital_usd,
    maximum_loss_usd,
    decision_confidence,
    decision_risk,
):
    """
    Calculate a conservative paper position size.

    The result is bounded by both risk-per-trade and maximum
    portfolio-position limits.
    """

    capital_usd = max(
        0.0,
        safe_float(capital_usd),
    )

    risk_budget = (
        capital_usd
        * MAXIMUM_RISK_PER_TRADE_PERCENT
        / 100.0
    )

    maximum_position = (
        capital_usd
        * MAXIMUM_POSITION_PERCENT
        / 100.0
    )

    if maximum_loss_usd <= 0:
        return 0.0

    confidence_factor = clamp(
        decision_confidence,
        0.0,
        100.0,
    ) / 100.0

    risk_factor = (
        1.0
        - clamp(
            decision_risk,
            0.0,
            100.0,
        ) / 100.0
    )

    quality_factor = clamp(
        confidence_factor
        * risk_factor,
        0.10,
        1.0,
    )

    risk_scaled_position = (
        risk_budget
        / maximum_loss_usd
        * quality_factor
    )

    return max(
        0.0,
        min(
            maximum_position,
            risk_scaled_position,
        ),
    )


def _process_score(
    result,
    reward_to_risk_ratio,
    edge_to_cost_ratio,
):
    """
    Score rule-following quality independently of trade outcome.
    """

    recommendation_ok = (
        str(
            result.get(
                "final_recommendation"
            )
            or ""
        ).upper()
        == "EXECUTE"
    )

    eligible = bool(
        result.get("eligible")
    )

    quote_successful = bool(
        result.get("quote_successful")
        or (
            result.get("decision")
            != "⚠️ QUOTE ERROR"
        )
    )

    decision_score = clamp(
        result.get("final_decision_score")
    )

    confidence = clamp(
        result.get("decision_confidence")
        or result.get(
            "combined_confidence"
        )
    )

    risk = clamp(
        result.get("downside_risk_score")
        or 50.0
    )

    votes = safe_int(
        result.get("decision_votes")
    )

    score = 0.0

    score += 15.0 if recommendation_ok else 0.0
    score += 15.0 if eligible else 0.0
    score += 10.0 if quote_successful else 0.0
    score += min(
        15.0,
        decision_score
        / MINIMUM_DECISION_SCORE
        * 15.0,
    )
    score += min(
        15.0,
        confidence
        / MINIMUM_DECISION_CONFIDENCE
        * 15.0,
    )
    score += (
        10.0
        if risk <= MAXIMUM_DECISION_RISK
        else max(
            0.0,
            10.0
            - (
                risk
                - MAXIMUM_DECISION_RISK
            ),
        )
    )
    score += min(
        10.0,
        votes
        / max(
            1,
            MINIMUM_EXECUTION_VOTES,
        )
        * 10.0,
    )
    score += min(
        5.0,
        reward_to_risk_ratio
        / MINIMUM_REWARD_TO_RISK_RATIO
        * 5.0,
    )
    score += min(
        5.0,
        edge_to_cost_ratio
        / MINIMUM_EDGE_TO_COST_RATIO
        * 5.0,
    )

    return clamp(score)


def evaluate_risk(
    result,
    cycle_id=None,
):
    """
    Apply disciplined risk gates to one proposed paper execution.
    """

    initialize_risk_manager()

    cycle_id = (
        cycle_id
        or current_timestamp()
    )

    state = get_risk_state()

    lifetime = state["lifetime"]
    daily = state["daily"]

    recommendation = str(
        result.get("final_recommendation")
        or "SKIP"
    ).upper()

    decision_score = clamp(
        result.get("final_decision_score")
    )

    decision_confidence = clamp(
        result.get("decision_confidence")
        or result.get(
            "combined_confidence"
        )
    )

    decision_risk = clamp(
        result.get("downside_risk_score")
        or 50.0
    )

    expected_profit_usd = safe_float(
        result.get("expected_profit_usd")
    )

    execution_votes = safe_int(
        result.get("decision_votes")
    )

    maximum_loss_usd = (
        _estimated_maximum_loss(result)
    )

    estimated_cost = max(
        DEFAULT_ESTIMATED_COST_USD,
        abs(
            safe_float(
                result.get("estimated_cost")
            )
        ),
    )

    reward_to_risk_ratio = (
        expected_profit_usd
        / maximum_loss_usd
        if maximum_loss_usd > 0
        else 0.0
    )

    edge_to_cost_ratio = (
        expected_profit_usd
        / estimated_cost
        if estimated_cost > 0
        else 0.0
    )

    process_score = _process_score(
        result=result,
        reward_to_risk_ratio=(
            reward_to_risk_ratio
        ),
        edge_to_cost_ratio=(
            edge_to_cost_ratio
        ),
    )

    current_capital = safe_float(
        lifetime.get(
            "current_capital_usd"
        )
    )

    proposed_position_usd = (
        _position_size(
            capital_usd=current_capital,
            maximum_loss_usd=(
                maximum_loss_usd
            ),
            decision_confidence=(
                decision_confidence
            ),
            decision_risk=(
                decision_risk
            ),
        )
    )

    realized_today = safe_float(
        daily.get("realized_profit_usd")
    )

    daily_loss_usd = max(
        0.0,
        -realized_today,
    )

    daily_drawdown_usd = safe_float(
        daily.get(
            "maximum_drawdown_usd"
        )
    )

    consecutive_losses = safe_int(
        lifetime.get(
            "consecutive_losses"
        )
    )

    trades_today = safe_int(
        daily.get("trades_taken")
    )

    blocked_reasons = []

    if safe_int(
        lifetime.get("emergency_stop")
    ):
        blocked_reasons.append(
            "Emergency stop is active."
        )

    if _cooldown_active(
        lifetime.get("cooldown_until")
    ):
        blocked_reasons.append(
            "Risk cooldown is active."
        )

    if recommendation != "EXECUTE":
        blocked_reasons.append(
            "Decision Engine did not recommend EXECUTE."
        )

    if not bool(
        result.get("eligible")
    ):
        blocked_reasons.append(
            "Scanner result is not eligible."
        )

    quote_successful = bool(
        result.get("quote_successful")
        or (
            result.get("decision")
            != "⚠️ QUOTE ERROR"
        )
    )

    if not quote_successful:
        blocked_reasons.append(
            "Quote was unsuccessful."
        )

    if (
        decision_score
        < MINIMUM_DECISION_SCORE
    ):
        blocked_reasons.append(
            "Decision score is below the minimum."
        )

    if (
        decision_confidence
        < MINIMUM_DECISION_CONFIDENCE
    ):
        blocked_reasons.append(
            "Decision confidence is below the minimum."
        )

    if (
        decision_risk
        > MAXIMUM_DECISION_RISK
    ):
        blocked_reasons.append(
            "Decision risk exceeds the maximum."
        )

    if (
        expected_profit_usd
        <= MINIMUM_EXPECTED_PROFIT_USD
    ):
        blocked_reasons.append(
            "Expected profit is not positive enough."
        )

    if (
        execution_votes
        < MINIMUM_EXECUTION_VOTES
    ):
        blocked_reasons.append(
            "Insufficient model agreement."
        )

    if (
        reward_to_risk_ratio
        < MINIMUM_REWARD_TO_RISK_RATIO
    ):
        blocked_reasons.append(
            "Reward-to-risk ratio is below the minimum."
        )

    if (
        edge_to_cost_ratio
        < MINIMUM_EDGE_TO_COST_RATIO
    ):
        blocked_reasons.append(
            "Expected edge does not sufficiently exceed cost."
        )

    if (
        process_score
        < PROCESS_SCORE_REQUIRED
    ):
        blocked_reasons.append(
            "Process-quality score is below the minimum."
        )

    if (
        daily_loss_usd
        >= MAXIMUM_DAILY_LOSS_USD
    ):
        blocked_reasons.append(
            "Maximum daily loss has been reached."
        )

    if (
        daily_drawdown_usd
        >= MAXIMUM_DAILY_DRAWDOWN_USD
    ):
        blocked_reasons.append(
            "Maximum daily drawdown has been reached."
        )

    if (
        consecutive_losses
        >= MAXIMUM_CONSECUTIVE_LOSSES
    ):
        blocked_reasons.append(
            "Maximum consecutive losses has been reached."
        )

    if (
        trades_today
        >= MAXIMUM_TRADES_PER_DAY
    ):
        blocked_reasons.append(
            "Maximum daily trade count has been reached."
        )

    if proposed_position_usd <= 0:
        blocked_reasons.append(
            "Calculated position size is zero."
        )

    approved = int(
        len(blocked_reasons) == 0
    )

    risk_action = (
        "APPROVE_PAPER_TRADE"
        if approved
        else "BLOCK"
    )

    explanation = (
        "All discipline and risk gates passed."
        if approved
        else " | ".join(blocked_reasons)
    )

    return {
        "cycle_id": cycle_id,
        "approved": approved,
        "risk_action": risk_action,
        "process_score": round(
            process_score,
            4,
        ),
        "proposed_position_usd": round(
            proposed_position_usd,
            6,
        ),
        "maximum_loss_usd": round(
            maximum_loss_usd,
            6,
        ),
        "reward_to_risk_ratio": round(
            reward_to_risk_ratio,
            4,
        ),
        "edge_to_cost_ratio": round(
            edge_to_cost_ratio,
            4,
        ),
        "decision_score": round(
            decision_score,
            4,
        ),
        "decision_confidence": round(
            decision_confidence,
            4,
        ),
        "decision_risk": round(
            decision_risk,
            4,
        ),
        "expected_profit_usd": round(
            expected_profit_usd,
            8,
        ),
        "execution_votes": (
            execution_votes
        ),
        "daily_loss_usd": round(
            daily_loss_usd,
            6,
        ),
        "daily_drawdown_usd": round(
            daily_drawdown_usd,
            6,
        ),
        "consecutive_losses": (
            consecutive_losses
        ),
        "trades_today": trades_today,
        "blocked_reasons": (
            blocked_reasons
        ),
        "explanation": explanation,
    }


def save_risk_audit(
    result,
    risk_result,
):
    """
    Save one permanent risk-decision audit row.
    """

    initialize_risk_manager()

    connection = get_database_connection()
    cursor = connection.cursor()

    symbol = str(
        result.get("token")
        or result.get("symbol")
        or "UNKNOWN"
    ).strip().upper()

    mint = str(
        result.get("mint")
        or result.get("token_mint")
        or ""
    ).strip() or None

    try:
        cursor.execute(
            """
            INSERT INTO risk_decision_audit (
                cycle_id,
                mint,
                symbol,
                recommendation_before_risk,
                approved,
                risk_action,
                process_score,
                proposed_position_usd,
                maximum_loss_usd,
                reward_to_risk_ratio,
                edge_to_cost_ratio,
                decision_score,
                decision_confidence,
                decision_risk,
                expected_profit_usd,
                execution_votes,
                daily_loss_usd,
                daily_drawdown_usd,
                consecutive_losses,
                trades_today,
                blocked_reasons_json,
                explanation,
                operating_mode,
                created_at,
                updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
            ON CONFLICT(cycle_id, mint, symbol)
            DO UPDATE SET
                recommendation_before_risk =
                    excluded.recommendation_before_risk,
                approved =
                    excluded.approved,
                risk_action =
                    excluded.risk_action,
                process_score =
                    excluded.process_score,
                proposed_position_usd =
                    excluded.proposed_position_usd,
                maximum_loss_usd =
                    excluded.maximum_loss_usd,
                reward_to_risk_ratio =
                    excluded.reward_to_risk_ratio,
                edge_to_cost_ratio =
                    excluded.edge_to_cost_ratio,
                decision_score =
                    excluded.decision_score,
                decision_confidence =
                    excluded.decision_confidence,
                decision_risk =
                    excluded.decision_risk,
                expected_profit_usd =
                    excluded.expected_profit_usd,
                execution_votes =
                    excluded.execution_votes,
                daily_loss_usd =
                    excluded.daily_loss_usd,
                daily_drawdown_usd =
                    excluded.daily_drawdown_usd,
                consecutive_losses =
                    excluded.consecutive_losses,
                trades_today =
                    excluded.trades_today,
                blocked_reasons_json =
                    excluded.blocked_reasons_json,
                explanation =
                    excluded.explanation,
                operating_mode =
                    excluded.operating_mode,
                updated_at =
                    excluded.updated_at
            """,
            (
                risk_result["cycle_id"],
                mint,
                symbol,
                str(
                    result.get(
                        "final_recommendation"
                    )
                    or "SKIP"
                ),
                risk_result["approved"],
                risk_result["risk_action"],
                risk_result["process_score"],
                risk_result[
                    "proposed_position_usd"
                ],
                risk_result[
                    "maximum_loss_usd"
                ],
                risk_result[
                    "reward_to_risk_ratio"
                ],
                risk_result[
                    "edge_to_cost_ratio"
                ],
                risk_result[
                    "decision_score"
                ],
                risk_result[
                    "decision_confidence"
                ],
                risk_result[
                    "decision_risk"
                ],
                risk_result[
                    "expected_profit_usd"
                ],
                risk_result[
                    "execution_votes"
                ],
                risk_result[
                    "daily_loss_usd"
                ],
                risk_result[
                    "daily_drawdown_usd"
                ],
                risk_result[
                    "consecutive_losses"
                ],
                risk_result["trades_today"],
                json.dumps(
                    risk_result[
                        "blocked_reasons"
                    ],
                    sort_keys=True,
                ),
                risk_result["explanation"],
                OPERATING_MODE,
                current_timestamp(),
                current_timestamp(),
            ),
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def evaluate_cycle_risk(
    results,
    cycle_id=None,
):
    """
    Evaluate and save risk decisions for one complete scanner cycle.
    """

    initialize_risk_manager()

    if not results:
        return {
            "cycle_id": cycle_id,
            "evaluated": 0,
            "approved": 0,
            "blocked": 0,
            "results": results,
        }

    cycle_id = (
        cycle_id
        or current_timestamp()
    )

    approved_count = 0
    blocked_count = 0

    for result in results:
        risk_result = evaluate_risk(
            result=result,
            cycle_id=cycle_id,
        )

        save_risk_audit(
            result=result,
            risk_result=risk_result,
        )

        result["risk_approved"] = (
            risk_result["approved"]
        )

        result["risk_action"] = (
            risk_result["risk_action"]
        )

        result["process_score"] = (
            risk_result["process_score"]
        )

        result["proposed_position_usd"] = (
            risk_result[
                "proposed_position_usd"
            ]
        )

        result["risk_blocked_reasons"] = (
            risk_result[
                "blocked_reasons"
            ]
        )

        if risk_result["approved"]:
            approved_count += 1
        else:
            blocked_count += 1

    return {
        "cycle_id": cycle_id,
        "evaluated": len(results),
        "approved": approved_count,
        "blocked": blocked_count,
        "results": results,
    }


def record_trade_outcome(
    cycle_id,
    result,
    actual_profit_usd,
):
    """
    Record the outcome of one approved paper trade.

    Good process and trade outcome are stored separately.
    """

    initialize_risk_manager()

    symbol = str(
        result.get("token")
        or result.get("symbol")
        or "UNKNOWN"
    ).strip().upper()

    mint = str(
        result.get("mint")
        or result.get("token_mint")
        or ""
    ).strip() or None

    actual_profit_usd = safe_float(
        actual_profit_usd
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM risk_state
            WHERE id = 1
            """
        )

        lifetime = cursor.fetchone()

        cursor.execute(
            """
            SELECT *
            FROM daily_risk_state
            WHERE trade_date = ?
            """,
            (current_date(),),
        )

        daily = cursor.fetchone()

        if not lifetime or not daily:
            raise RuntimeError(
                "Risk state was not initialized."
            )

        current_capital = (
            safe_float(
                lifetime[
                    "current_capital_usd"
                ]
            )
            + actual_profit_usd
        )

        peak_capital = max(
            safe_float(
                lifetime[
                    "peak_capital_usd"
                ]
            ),
            current_capital,
        )

        if actual_profit_usd > 0:
            consecutive_losses = 0
            lifetime_wins = (
                safe_int(
                    lifetime[
                        "lifetime_wins"
                    ]
                )
                + 1
            )
            lifetime_losses = safe_int(
                lifetime[
                    "lifetime_losses"
                ]
            )
        else:
            consecutive_losses = (
                safe_int(
                    lifetime[
                        "consecutive_losses"
                    ]
                )
                + 1
            )
            lifetime_wins = safe_int(
                lifetime[
                    "lifetime_wins"
                ]
            )
            lifetime_losses = (
                safe_int(
                    lifetime[
                        "lifetime_losses"
                    ]
                )
                + 1
            )

        cooldown_until = (
            lifetime["cooldown_until"]
        )

        if (
            consecutive_losses
            >= MAXIMUM_CONSECUTIVE_LOSSES
        ):
            cooldown_until = (
                datetime.now()
                + timedelta(
                    minutes=(
                        COOLDOWN_MINUTES_AFTER_LOSS_LIMIT
                    )
                )
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        cursor.execute(
            """
            UPDATE risk_state
            SET
                current_capital_usd = ?,
                peak_capital_usd = ?,
                consecutive_losses = ?,
                lifetime_trades =
                    lifetime_trades + 1,
                lifetime_wins = ?,
                lifetime_losses = ?,
                lifetime_profit_usd =
                    lifetime_profit_usd + ?,
                cooldown_until = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                current_capital,
                peak_capital,
                consecutive_losses,
                lifetime_wins,
                lifetime_losses,
                actual_profit_usd,
                cooldown_until,
                current_timestamp(),
            ),
        )

        daily_current_capital = (
            safe_float(
                daily[
                    "current_capital_usd"
                ]
            )
            + actual_profit_usd
        )

        daily_peak_capital = max(
            safe_float(
                daily[
                    "peak_capital_usd"
                ]
            ),
            daily_current_capital,
        )

        daily_drawdown = max(
            safe_float(
                daily[
                    "maximum_drawdown_usd"
                ]
            ),
            daily_peak_capital
            - daily_current_capital,
        )

        daily_profit = (
            safe_float(
                daily[
                    "realized_profit_usd"
                ]
            )
            + actual_profit_usd
        )

        if (
            daily_drawdown
            >= MAXIMUM_DAILY_DRAWDOWN_USD
        ):
            cooldown_until = (
                datetime.now()
                + timedelta(
                    minutes=(
                        COOLDOWN_MINUTES_AFTER_DRAWDOWN
                    )
                )
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            cursor.execute(
                """
                UPDATE risk_state
                SET
                    cooldown_until = ?,
                    updated_at = ?
                WHERE id = 1
                """,
                (
                    cooldown_until,
                    current_timestamp(),
                ),
            )

        cursor.execute(
            """
            UPDATE daily_risk_state
            SET
                current_capital_usd = ?,
                peak_capital_usd = ?,
                trades_taken =
                    trades_taken + 1,
                wins =
                    wins + ?,
                losses =
                    losses + ?,
                realized_profit_usd = ?,
                maximum_drawdown_usd = ?,
                updated_at = ?
            WHERE trade_date = ?
            """,
            (
                daily_current_capital,
                daily_peak_capital,
                int(actual_profit_usd > 0),
                int(actual_profit_usd <= 0),
                daily_profit,
                daily_drawdown,
                current_timestamp(),
                current_date(),
            ),
        )

        cursor.execute(
            """
            UPDATE risk_decision_audit
            SET
                actual_profit_usd = ?,
                outcome_recorded = 1,
                updated_at = ?
            WHERE cycle_id = ?
              AND COALESCE(mint, '') =
                  COALESCE(?, '')
              AND symbol = ?
            """,
            (
                actual_profit_usd,
                current_timestamp(),
                cycle_id,
                mint,
                symbol,
            ),
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def get_risk_summary():
    """
    Return current paper risk and discipline statistics.
    """

    initialize_risk_manager()

    state = get_risk_state()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_audits,
                COALESCE(
                    SUM(approved),
                    0
                ) AS approved,
                SUM(
                    CASE
                        WHEN approved = 0
                        THEN 1
                        ELSE 0
                    END
                ) AS blocked,
                AVG(process_score)
                    AS average_process_score,
                AVG(
                    CASE
                        WHEN approved = 1
                        THEN proposed_position_usd
                        ELSE NULL
                    END
                ) AS average_approved_position,
                MAX(updated_at)
                    AS last_updated_at
            FROM risk_decision_audit
            """
        )

        audit = cursor.fetchone()

    finally:
        connection.close()

    result = dict(audit) if audit else {}

    result["lifetime"] = state["lifetime"]
    result["daily"] = state["daily"]

    for key in (
        "total_audits",
        "approved",
        "blocked",
    ):
        result[key] = safe_int(
            result.get(key)
        )

    for key in (
        "average_process_score",
        "average_approved_position",
    ):
        result[key] = safe_float(
            result.get(key)
        )

    result.setdefault(
        "last_updated_at",
        None,
    )

    return result


def set_emergency_stop(
    enabled,
    reason="",
):
    """
    Manually enable or disable the paper emergency stop.
    """

    initialize_risk_manager()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            UPDATE risk_state
            SET
                emergency_stop = ?,
                emergency_reason = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                int(bool(enabled)),
                str(reason or ""),
                current_timestamp(),
            ),
        )

        connection.commit()

    finally:
        connection.close()


if __name__ == "__main__":
    initialize_risk_manager()

    summary = get_risk_summary()

    print("\nRisk and Discipline Manager ready.")
    print(
        "Operating mode: "
        f"{OPERATING_MODE}"
    )
    print(
        "Paper capital: "
        f"${safe_float(summary['lifetime'].get('current_capital_usd')):.2f}"
    )
    print(
        "Emergency stop: "
        f"{bool(safe_int(summary['lifetime'].get('emergency_stop')))}"
    )