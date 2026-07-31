import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path

from database.context_engine import (
    get_context_summary,
)
from database.reinforcement_learning import (
    get_champion_config,
)


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)

OPERATING_MODE = "PAPER_DECISION_AUDIT"

EXECUTE_SCORE_THRESHOLD = 72.0
WATCH_SCORE_THRESHOLD = 55.0
MINIMUM_EXECUTE_CONFIDENCE = 35.0
MAXIMUM_EXECUTE_RISK = 35.0
MINIMUM_EXECUTE_EXPECTED_PROFIT_USD = 0.0
MINIMUM_EXECUTE_VOTES = 4

VOTE_NAMES = (
    "market",
    "intelligence",
    "prediction",
    "pattern",
    "context",
    "risk",
)


def get_database_connection():
    connection = sqlite3.connect(
        DATABASE_FILE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    return connection


def current_timestamp():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def clamp(
    value,
    minimum=0.0,
    maximum=100.0,
):
    return max(
        minimum,
        min(maximum, safe_float(value)),
    )


def initialize_decision_engine():
    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS decision_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                mint TEXT,
                symbol TEXT NOT NULL,

                decision_score REAL NOT NULL DEFAULT 0,
                execution_confidence REAL NOT NULL DEFAULT 0,
                risk_score REAL NOT NULL DEFAULT 0,
                expected_profit_usd REAL NOT NULL DEFAULT 0,

                recommendation TEXT NOT NULL,
                safety_gate_passed INTEGER NOT NULL DEFAULT 0,
                votes_for_execute INTEGER NOT NULL DEFAULT 0,
                votes_total INTEGER NOT NULL DEFAULT 0,
                votes_json TEXT NOT NULL,
                explanation TEXT NOT NULL,

                market_score REAL NOT NULL DEFAULT 0,
                intelligence_score REAL NOT NULL DEFAULT 0,
                prediction_score REAL NOT NULL DEFAULT 0,
                pattern_score REAL NOT NULL DEFAULT 0,
                context_score REAL NOT NULL DEFAULT 0,
                reinforcement_score REAL NOT NULL DEFAULT 0,

                actual_profit_usd REAL NOT NULL DEFAULT 0,
                actual_profitable INTEGER NOT NULL DEFAULT 0,
                quote_successful INTEGER NOT NULL DEFAULT 0,
                eligible INTEGER NOT NULL DEFAULT 0,

                operating_mode TEXT NOT NULL,
                created_at TEXT NOT NULL,

                UNIQUE(cycle_id, mint, symbol)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_decision_history_cycle
            ON decision_history(cycle_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_decision_history_recommendation
            ON decision_history(recommendation)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_decision_history_score
            ON decision_history(decision_score)
            """
        )

        connection.commit()

    finally:
        connection.close()


def _normalized_expected_profit(
    expected_profit_usd,
):
    """
    Convert expected profit to a bounded decision component.
    """

    value = safe_float(
        expected_profit_usd
    )

    return clamp(
        50.0
        + math.tanh(
            value / 0.01
        )
        * 50.0
    )


def _context_score():
    try:
        summary = get_context_summary()
    except Exception:
        return 50.0

    if not summary:
        return 50.0

    average_quality = safe_float(
        summary.get("avg_market_quality")
    )

    average_success = safe_float(
        summary.get("avg_success")
    )

    average_profit = safe_float(
        summary.get("avg_profit")
    )

    profit_component = clamp(
        50.0
        + math.tanh(
            average_profit / 0.01
        )
        * 50.0
    )

    return clamp(
        average_quality * 0.55
        + average_success * 0.25
        + profit_component * 0.20
    )


def _champion_score(
    result,
):
    """
    Use the current champion's bounded scoring function when
    available. Failure falls back to the scanner AI score.
    """

    fallback = clamp(
        result.get("ai_opportunity_score")
        or result.get("prediction_ai_priority")
        or result.get("intelligence_score")
    )

    try:
        champion = get_champion_config()

        if not champion:
            return fallback

        from database.reinforcement_learning import (
            calculate_model_score,
        )

        return clamp(
            calculate_model_score(
                champion,
                result,
            )
        )

    except Exception:
        return fallback


def _build_votes(
    market_score,
    intelligence_score,
    opportunity_probability,
    expected_profit_usd,
    pattern_score,
    context_score,
    risk_score,
):
    votes = {
        "market": (
            market_score >= 65.0
        ),
        "intelligence": (
            intelligence_score >= 65.0
        ),
        "prediction": (
            opportunity_probability >= 50.0
            and expected_profit_usd > 0
        ),
        "pattern": (
            pattern_score >= 60.0
        ),
        "context": (
            context_score >= 60.0
        ),
        "risk": (
            risk_score <= MAXIMUM_EXECUTE_RISK
        ),
    }

    return votes


def calculate_decision(
    result,
    context_score=None,
):
    """
    Calculate one auditable paper-mode execution recommendation.
    """

    market_score = clamp(
        result.get("market_score")
    )

    intelligence_score = clamp(
        result.get("intelligence_score")
        or market_score
    )

    prediction_score = clamp(
        result.get("prediction_ai_priority")
        or result.get("ai_opportunity_score")
    )

    opportunity_probability = clamp(
        result.get(
            "opportunity_probability"
        )
    )

    prediction_confidence = clamp(
        result.get(
            "prediction_confidence"
        )
    )

    combined_confidence = clamp(
        result.get("combined_confidence")
        or prediction_confidence
    )

    expected_profit_usd = safe_float(
        result.get("expected_profit_usd")
    )

    expected_profit_score = (
        _normalized_expected_profit(
            expected_profit_usd
        )
    )

    trend_score = clamp(
        result.get("trend_score"),
        0.0,
        100.0,
    )

    stability_score = clamp(
        result.get("stability_score"),
        0.0,
        100.0,
    )

    pattern_score = clamp(
        result.get("pattern_score")
        or 50.0
    )

    risk_score = clamp(
        result.get("downside_risk_score")
        or 50.0
    )

    context_score = clamp(
        context_score
        if context_score is not None
        else _context_score()
    )

    reinforcement_score = (
        _champion_score(result)
    )

    raw_score = (
        market_score * 0.12
        + intelligence_score * 0.15
        + prediction_score * 0.16
        + opportunity_probability * 0.12
        + expected_profit_score * 0.13
        + trend_score * 0.06
        + stability_score * 0.06
        + pattern_score * 0.08
        + context_score * 0.05
        + reinforcement_score * 0.07
    )

    decision_score = clamp(
        raw_score
        - risk_score * 0.22
    )

    votes = _build_votes(
        market_score=market_score,
        intelligence_score=(
            intelligence_score
        ),
        opportunity_probability=(
            opportunity_probability
        ),
        expected_profit_usd=(
            expected_profit_usd
        ),
        pattern_score=pattern_score,
        context_score=context_score,
        risk_score=risk_score,
    )

    votes_for_execute = sum(
        int(value)
        for value in votes.values()
    )

    quote_successful = int(
        bool(
            result.get("quote_successful")
            or (
                result.get("decision")
                != "⚠️ QUOTE ERROR"
            )
        )
    )

    eligible = int(
        quote_successful
        and bool(result.get("eligible"))
    )

    safety_gate_passed = int(
        quote_successful == 1
        and eligible == 1
        and expected_profit_usd
        > MINIMUM_EXECUTE_EXPECTED_PROFIT_USD
        and combined_confidence
        >= MINIMUM_EXECUTE_CONFIDENCE
        and risk_score
        <= MAXIMUM_EXECUTE_RISK
        and votes_for_execute
        >= MINIMUM_EXECUTE_VOTES
    )

    if (
        safety_gate_passed
        and decision_score
        >= EXECUTE_SCORE_THRESHOLD
    ):
        recommendation = "EXECUTE"

    elif (
        quote_successful
        and decision_score
        >= WATCH_SCORE_THRESHOLD
    ):
        recommendation = "WATCH"

    else:
        recommendation = "SKIP"

    explanation_parts = [
        (
            f"{votes_for_execute}/"
            f"{len(votes)} model votes support execution"
        ),
        (
            f"decision score "
            f"{decision_score:.2f}/100"
        ),
        (
            f"confidence "
            f"{combined_confidence:.2f}/100"
        ),
        (
            f"risk "
            f"{risk_score:.2f}/100"
        ),
        (
            f"expected profit "
            f"${expected_profit_usd:.6f}"
        ),
    ]

    if not safety_gate_passed:
        explanation_parts.append(
            "one or more paper safety gates failed"
        )

    return {
        "decision_score": round(
            decision_score,
            4,
        ),
        "execution_confidence": round(
            combined_confidence,
            4,
        ),
        "risk_score": round(
            risk_score,
            4,
        ),
        "expected_profit_usd": round(
            expected_profit_usd,
            8,
        ),
        "recommendation": recommendation,
        "safety_gate_passed": (
            safety_gate_passed
        ),
        "votes_for_execute": (
            votes_for_execute
        ),
        "votes_total": len(votes),
        "votes": votes,
        "explanation": "; ".join(
            explanation_parts
        ),
        "market_score": market_score,
        "intelligence_score": (
            intelligence_score
        ),
        "prediction_score": (
            prediction_score
        ),
        "pattern_score": pattern_score,
        "context_score": context_score,
        "reinforcement_score": (
            reinforcement_score
        ),
    }


def evaluate_decisions(
    results,
    cycle_id=None,
):
    """
    Evaluate and save all scanner results.

    The returned scanner-result dictionaries are enriched in place
    with decision fields. This remains paper-only.
    """

    initialize_decision_engine()

    if not results:
        return {
            "cycle_id": cycle_id,
            "decisions_saved": 0,
            "execute": 0,
            "watch": 0,
            "skip": 0,
            "results": results,
        }

    cycle_id = (
        cycle_id
        or current_timestamp()
    )

    context_score = _context_score()

    connection = get_database_connection()
    cursor = connection.cursor()

    counts = {
        "EXECUTE": 0,
        "WATCH": 0,
        "SKIP": 0,
    }

    saved_count = 0

    try:
        for result in results:
            decision = calculate_decision(
                result,
                context_score=context_score,
            )

            result[
                "final_decision_score"
            ] = decision[
                "decision_score"
            ]

            result[
                "final_recommendation"
            ] = decision[
                "recommendation"
            ]

            result[
                "decision_confidence"
            ] = decision[
                "execution_confidence"
            ]

            result[
                "decision_votes"
            ] = decision[
                "votes_for_execute"
            ]

            result[
                "decision_safety_gate"
            ] = decision[
                "safety_gate_passed"
            ]

            result[
                "decision_explanation"
            ] = decision[
                "explanation"
            ]

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

            actual_profit = safe_float(
                result.get("net_profit")
            )

            quote_successful = int(
                bool(
                    result.get("quote_successful")
                    or (
                        result.get("decision")
                        != "⚠️ QUOTE ERROR"
                    )
                )
            )

            actual_profitable = int(
                quote_successful
                and actual_profit > 0
            )

            eligible = int(
                quote_successful
                and bool(result.get("eligible"))
            )

            cursor.execute(
                """
                INSERT INTO decision_history (
                    cycle_id,
                    mint,
                    symbol,
                    decision_score,
                    execution_confidence,
                    risk_score,
                    expected_profit_usd,
                    recommendation,
                    safety_gate_passed,
                    votes_for_execute,
                    votes_total,
                    votes_json,
                    explanation,
                    market_score,
                    intelligence_score,
                    prediction_score,
                    pattern_score,
                    context_score,
                    reinforcement_score,
                    actual_profit_usd,
                    actual_profitable,
                    quote_successful,
                    eligible,
                    operating_mode,
                    created_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
                ON CONFLICT(cycle_id, mint, symbol)
                DO UPDATE SET
                    decision_score =
                        excluded.decision_score,
                    execution_confidence =
                        excluded.execution_confidence,
                    risk_score =
                        excluded.risk_score,
                    expected_profit_usd =
                        excluded.expected_profit_usd,
                    recommendation =
                        excluded.recommendation,
                    safety_gate_passed =
                        excluded.safety_gate_passed,
                    votes_for_execute =
                        excluded.votes_for_execute,
                    votes_total =
                        excluded.votes_total,
                    votes_json =
                        excluded.votes_json,
                    explanation =
                        excluded.explanation,
                    actual_profit_usd =
                        excluded.actual_profit_usd,
                    actual_profitable =
                        excluded.actual_profitable,
                    quote_successful =
                        excluded.quote_successful,
                    eligible =
                        excluded.eligible,
                    created_at =
                        excluded.created_at
                """,
                (
                    cycle_id,
                    mint,
                    symbol,
                    decision[
                        "decision_score"
                    ],
                    decision[
                        "execution_confidence"
                    ],
                    decision["risk_score"],
                    decision[
                        "expected_profit_usd"
                    ],
                    decision[
                        "recommendation"
                    ],
                    decision[
                        "safety_gate_passed"
                    ],
                    decision[
                        "votes_for_execute"
                    ],
                    decision["votes_total"],
                    json.dumps(
                        decision["votes"],
                        sort_keys=True,
                    ),
                    decision["explanation"],
                    decision["market_score"],
                    decision[
                        "intelligence_score"
                    ],
                    decision[
                        "prediction_score"
                    ],
                    decision["pattern_score"],
                    decision["context_score"],
                    decision[
                        "reinforcement_score"
                    ],
                    actual_profit,
                    actual_profitable,
                    quote_successful,
                    eligible,
                    OPERATING_MODE,
                    current_timestamp(),
                ),
            )

            counts[
                decision["recommendation"]
            ] += 1

            saved_count += 1

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return {
        "cycle_id": cycle_id,
        "decisions_saved": saved_count,
        "execute": counts["EXECUTE"],
        "watch": counts["WATCH"],
        "skip": counts["SKIP"],
        "context_score": context_score,
        "results": results,
    }


def get_decision_summary():
    initialize_decision_engine()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_decisions,
                COUNT(DISTINCT cycle_id)
                    AS decision_cycles,
                SUM(
                    CASE
                        WHEN recommendation = 'EXECUTE'
                        THEN 1
                        ELSE 0
                    END
                ) AS execute_decisions,
                SUM(
                    CASE
                        WHEN recommendation = 'WATCH'
                        THEN 1
                        ELSE 0
                    END
                ) AS watch_decisions,
                SUM(
                    CASE
                        WHEN recommendation = 'SKIP'
                        THEN 1
                        ELSE 0
                    END
                ) AS skip_decisions,
                AVG(decision_score)
                    AS average_decision_score,
                AVG(execution_confidence)
                    AS average_confidence,
                AVG(risk_score)
                    AS average_risk,
                AVG(
                    CASE
                        WHEN recommendation = 'EXECUTE'
                        THEN actual_profitable
                        ELSE NULL
                    END
                ) * 100.0
                    AS execute_profitable_rate,
                AVG(
                    CASE
                        WHEN recommendation = 'EXECUTE'
                        THEN actual_profit_usd
                        ELSE NULL
                    END
                ) AS execute_average_profit,
                MAX(created_at)
                    AS last_updated_at
            FROM decision_history
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    result = dict(row) if row else {}

    for key in (
        "total_decisions",
        "decision_cycles",
        "execute_decisions",
        "watch_decisions",
        "skip_decisions",
    ):
        result[key] = safe_int(
            result.get(key)
        )

    for key in (
        "average_decision_score",
        "average_confidence",
        "average_risk",
        "execute_profitable_rate",
        "execute_average_profit",
    ):
        result[key] = safe_float(
            result.get(key)
        )

    result.setdefault(
        "last_updated_at",
        None,
    )

    return result


def get_top_decisions(
    limit=10,
):
    initialize_decision_engine()

    limit = max(
        1,
        int(limit),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM decision_history
            ORDER BY
                decision_score DESC,
                execution_confidence DESC,
                expected_profit_usd DESC
            LIMIT ?
            """,
            (limit,),
        )

        return [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()


if __name__ == "__main__":
    initialize_decision_engine()

    summary = get_decision_summary()

    print("\nDecision Engine ready.")
    print(
        "Operating mode: "
        f"{OPERATING_MODE}"
    )
    print(
        "Total decisions: "
        f"{summary['total_decisions']:,}"
    )
