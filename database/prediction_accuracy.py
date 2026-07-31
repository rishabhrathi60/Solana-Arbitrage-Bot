import json
import math
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from statistics import mean


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)

POSITIVE_PROFIT_THRESHOLD_USD = 0.0
DEFAULT_RECENT_LIMIT = 500


def get_database_connection():
    """
    Create a SQLite connection that returns dictionary-like rows.
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


def clamp(value, minimum=0.0, maximum=100.0):
    """
    Restrict a numeric value to a safe range.
    """

    return max(
        minimum,
        min(maximum, safe_float(value)),
    )


def normalize_symbol(value):
    """
    Normalize a token symbol for fallback matching.
    """

    return str(value or "").strip().upper()


def initialize_prediction_accuracy_tables():
    """
    Create the prediction snapshot and prediction outcome tables.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prediction_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT NOT NULL,
                mint TEXT,
                symbol TEXT NOT NULL,
                name TEXT,
                opportunity_probability REAL NOT NULL DEFAULT 0,
                expected_profit_usd REAL NOT NULL DEFAULT 0,
                expected_profit_score REAL NOT NULL DEFAULT 0,
                trend_score REAL NOT NULL DEFAULT 50,
                stability_score REAL NOT NULL DEFAULT 0,
                downside_risk_score REAL NOT NULL DEFAULT 0,
                prediction_confidence REAL NOT NULL DEFAULT 0,
                ai_priority REAL NOT NULL DEFAULT 0,
                prediction_updated_at TEXT,
                captured_at TEXT NOT NULL,
                UNIQUE(snapshot_id, mint, symbol)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prediction_accuracy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT NOT NULL,
                mint TEXT,
                symbol TEXT NOT NULL,

                predicted_probability REAL NOT NULL DEFAULT 0,
                predicted_profit_usd REAL NOT NULL DEFAULT 0,
                prediction_confidence REAL NOT NULL DEFAULT 0,
                ai_priority REAL NOT NULL DEFAULT 0,

                actual_profit_usd REAL NOT NULL DEFAULT 0,
                actual_profitable INTEGER NOT NULL DEFAULT 0,
                actual_eligible INTEGER NOT NULL DEFAULT 0,
                quote_successful INTEGER NOT NULL DEFAULT 0,

                probability_error REAL NOT NULL DEFAULT 0,
                absolute_probability_error REAL NOT NULL DEFAULT 0,
                brier_score REAL NOT NULL DEFAULT 0,
                profit_error_usd REAL NOT NULL DEFAULT 0,
                absolute_profit_error_usd REAL NOT NULL DEFAULT 0,

                predicted_positive INTEGER NOT NULL DEFAULT 0,
                classification_correct INTEGER NOT NULL DEFAULT 0,
                false_positive INTEGER NOT NULL DEFAULT 0,
                false_negative INTEGER NOT NULL DEFAULT 0,

                confidence_band TEXT NOT NULL,
                prediction_captured_at TEXT,
                outcome_scanned_at TEXT,
                graded_at TEXT NOT NULL,

                UNIQUE(snapshot_id, mint, symbol)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_prediction_snapshots_snapshot
            ON prediction_snapshots(snapshot_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_prediction_snapshots_symbol
            ON prediction_snapshots(symbol)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_prediction_accuracy_snapshot
            ON prediction_accuracy(snapshot_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_prediction_accuracy_symbol
            ON prediction_accuracy(symbol)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_prediction_accuracy_graded
            ON prediction_accuracy(graded_at)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_prediction_accuracy_confidence_band
            ON prediction_accuracy(confidence_band)
            """
        )

        connection.commit()

    finally:
        connection.close()


def get_confidence_band(confidence):
    """
    Convert confidence into a stable reporting band.
    """

    confidence = clamp(confidence)

    if confidence < 20:
        return "00-19"
    if confidence < 40:
        return "20-39"
    if confidence < 60:
        return "40-59"
    if confidence < 80:
        return "60-79"

    return "80-100"


def capture_prediction_snapshot():
    """
    Capture the exact prediction state before a scanner cycle.

    The returned snapshot_id must be passed to
    grade_prediction_snapshot() after the scan completes.
    """

    initialize_prediction_accuracy_tables()

    snapshot_id = uuid.uuid4().hex
    captured_at = current_timestamp()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                mint,
                symbol,
                name,
                opportunity_probability,
                expected_profit_usd,
                expected_profit_score,
                trend_score,
                stability_score,
                downside_risk_score,
                prediction_confidence,
                ai_priority,
                prediction_updated_at
            FROM token_predictions
            """
        )

        predictions = [
            dict(row)
            for row in cursor.fetchall()
        ]

        for prediction in predictions:
            cursor.execute(
                """
                INSERT OR REPLACE INTO prediction_snapshots (
                    snapshot_id,
                    mint,
                    symbol,
                    name,
                    opportunity_probability,
                    expected_profit_usd,
                    expected_profit_score,
                    trend_score,
                    stability_score,
                    downside_risk_score,
                    prediction_confidence,
                    ai_priority,
                    prediction_updated_at,
                    captured_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    snapshot_id,
                    prediction.get("mint"),
                    normalize_symbol(
                        prediction.get("symbol")
                    ) or "UNKNOWN",
                    prediction.get("name"),
                    safe_float(
                        prediction.get(
                            "opportunity_probability"
                        )
                    ),
                    safe_float(
                        prediction.get(
                            "expected_profit_usd"
                        )
                    ),
                    safe_float(
                        prediction.get(
                            "expected_profit_score"
                        )
                    ),
                    safe_float(
                        prediction.get("trend_score")
                    ),
                    safe_float(
                        prediction.get("stability_score")
                    ),
                    safe_float(
                        prediction.get(
                            "downside_risk_score"
                        )
                    ),
                    safe_float(
                        prediction.get(
                            "prediction_confidence"
                        )
                    ),
                    safe_float(
                        prediction.get("ai_priority")
                    ),
                    prediction.get(
                        "prediction_updated_at"
                    ),
                    captured_at,
                ),
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return {
        "snapshot_id": snapshot_id,
        "predictions_captured": len(predictions),
        "captured_at": captured_at,
    }


def _load_snapshot_predictions(snapshot_id):
    """
    Load one prediction snapshot.
    """

    initialize_prediction_accuracy_tables()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM prediction_snapshots
            WHERE snapshot_id = ?
            ORDER BY id ASC
            """,
            (snapshot_id,),
        )

        return [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()


def _build_snapshot_indexes(snapshot_rows):
    """
    Build mint and symbol indexes for safe result matching.
    """

    by_mint = {}
    by_symbol = {}

    for row in snapshot_rows:
        mint = str(
            row.get("mint") or ""
        ).strip()

        symbol = normalize_symbol(
            row.get("symbol")
        )

        if mint:
            by_mint[mint] = row

        if symbol:
            by_symbol.setdefault(
                symbol,
                [],
            ).append(row)

    return by_mint, by_symbol


def _match_prediction(
    result,
    predictions_by_mint,
    predictions_by_symbol,
):
    """
    Match a scanner result to its pre-scan prediction.

    Mint matching is authoritative. Symbol matching is used only
    when exactly one prediction has that symbol.
    """

    mint = str(
        result.get("mint")
        or result.get("token_mint")
        or ""
    ).strip()

    if mint and mint in predictions_by_mint:
        return predictions_by_mint[mint]

    symbol = normalize_symbol(
        result.get("token")
        or result.get("symbol")
    )

    candidates = (
        predictions_by_symbol.get(symbol)
        or []
    )

    if len(candidates) == 1:
        return candidates[0]

    return None


def grade_prediction_snapshot(
    snapshot_id,
    results,
):
    """
    Compare pre-scan predictions with actual scanner outcomes.

    Predictions are graded only for scanner results that can be
    matched safely by mint or by an unambiguous symbol.
    """

    initialize_prediction_accuracy_tables()

    if not snapshot_id:
        raise ValueError(
            "snapshot_id is required."
        )

    if not results:
        return {
            "snapshot_id": snapshot_id,
            "results_received": 0,
            "predictions_graded": 0,
            "unmatched_results": 0,
            "quote_failures_graded": 0,
            "graded_at": current_timestamp(),
        }

    snapshot_rows = _load_snapshot_predictions(
        snapshot_id
    )

    predictions_by_mint, predictions_by_symbol = (
        _build_snapshot_indexes(snapshot_rows)
    )

    graded_at = current_timestamp()
    graded_count = 0
    unmatched_count = 0
    quote_failures_graded = 0

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        for result in results:
            prediction = _match_prediction(
                result=result,
                predictions_by_mint=predictions_by_mint,
                predictions_by_symbol=predictions_by_symbol,
            )

            if prediction is None:
                unmatched_count += 1
                continue

            quote_successful = int(
                bool(
                    result.get(
                        "quote_successful",
                        not bool(
                            str(
                                result.get("error")
                                or ""
                            ).strip()
                        ),
                    )
                )
            )

            if not quote_successful:
                quote_failures_graded += 1

            actual_profit = safe_float(
                result.get("net_profit")
            )

            actual_profitable = int(
                quote_successful == 1
                and actual_profit
                > POSITIVE_PROFIT_THRESHOLD_USD
            )

            actual_eligible = int(
                quote_successful == 1
                and bool(result.get("eligible"))
            )

            predicted_probability = clamp(
                prediction.get(
                    "opportunity_probability"
                )
            )

            predicted_probability_ratio = (
                predicted_probability / 100.0
            )

            predicted_profit = safe_float(
                prediction.get(
                    "expected_profit_usd"
                )
            )

            predicted_positive = int(
                predicted_probability >= 50.0
            )

            classification_correct = int(
                predicted_positive
                == actual_profitable
            )

            false_positive = int(
                predicted_positive == 1
                and actual_profitable == 0
            )

            false_negative = int(
                predicted_positive == 0
                and actual_profitable == 1
            )

            probability_error = (
                predicted_probability
                - actual_profitable * 100.0
            )

            absolute_probability_error = abs(
                probability_error
            )

            brier_score = (
                predicted_probability_ratio
                - float(actual_profitable)
            ) ** 2

            profit_error = (
                predicted_profit
                - actual_profit
            )

            absolute_profit_error = abs(
                profit_error
            )

            mint = str(
                result.get("mint")
                or result.get("token_mint")
                or prediction.get("mint")
                or ""
            ).strip() or None

            symbol = normalize_symbol(
                result.get("token")
                or result.get("symbol")
                or prediction.get("symbol")
            ) or "UNKNOWN"

            outcome_scanned_at = (
                result.get("scanned_at")
                or graded_at
            )

            cursor.execute(
                """
                INSERT INTO prediction_accuracy (
                    snapshot_id,
                    mint,
                    symbol,
                    predicted_probability,
                    predicted_profit_usd,
                    prediction_confidence,
                    ai_priority,
                    actual_profit_usd,
                    actual_profitable,
                    actual_eligible,
                    quote_successful,
                    probability_error,
                    absolute_probability_error,
                    brier_score,
                    profit_error_usd,
                    absolute_profit_error_usd,
                    predicted_positive,
                    classification_correct,
                    false_positive,
                    false_negative,
                    confidence_band,
                    prediction_captured_at,
                    outcome_scanned_at,
                    graded_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(snapshot_id, mint, symbol)
                DO UPDATE SET
                    predicted_probability =
                        excluded.predicted_probability,
                    predicted_profit_usd =
                        excluded.predicted_profit_usd,
                    prediction_confidence =
                        excluded.prediction_confidence,
                    ai_priority =
                        excluded.ai_priority,
                    actual_profit_usd =
                        excluded.actual_profit_usd,
                    actual_profitable =
                        excluded.actual_profitable,
                    actual_eligible =
                        excluded.actual_eligible,
                    quote_successful =
                        excluded.quote_successful,
                    probability_error =
                        excluded.probability_error,
                    absolute_probability_error =
                        excluded.absolute_probability_error,
                    brier_score =
                        excluded.brier_score,
                    profit_error_usd =
                        excluded.profit_error_usd,
                    absolute_profit_error_usd =
                        excluded.absolute_profit_error_usd,
                    predicted_positive =
                        excluded.predicted_positive,
                    classification_correct =
                        excluded.classification_correct,
                    false_positive =
                        excluded.false_positive,
                    false_negative =
                        excluded.false_negative,
                    confidence_band =
                        excluded.confidence_band,
                    prediction_captured_at =
                        excluded.prediction_captured_at,
                    outcome_scanned_at =
                        excluded.outcome_scanned_at,
                    graded_at =
                        excluded.graded_at
                """,
                (
                    snapshot_id,
                    mint,
                    symbol,
                    predicted_probability,
                    predicted_profit,
                    safe_float(
                        prediction.get(
                            "prediction_confidence"
                        )
                    ),
                    safe_float(
                        prediction.get("ai_priority")
                    ),
                    actual_profit,
                    actual_profitable,
                    actual_eligible,
                    quote_successful,
                    probability_error,
                    absolute_probability_error,
                    brier_score,
                    profit_error,
                    absolute_profit_error,
                    predicted_positive,
                    classification_correct,
                    false_positive,
                    false_negative,
                    get_confidence_band(
                        prediction.get(
                            "prediction_confidence"
                        )
                    ),
                    prediction.get("captured_at"),
                    str(outcome_scanned_at),
                    graded_at,
                ),
            )

            graded_count += 1

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return {
        "snapshot_id": snapshot_id,
        "results_received": len(results),
        "predictions_graded": graded_count,
        "unmatched_results": unmatched_count,
        "quote_failures_graded": quote_failures_graded,
        "graded_at": graded_at,
    }


def get_prediction_accuracy_summary(
    recent_limit=None,
):
    """
    Return overall or recent prediction-accuracy statistics.
    """

    initialize_prediction_accuracy_tables()

    where_clause = ""
    parameters = ()

    if recent_limit is not None:
        recent_limit = max(
            1,
            int(recent_limit),
        )

        where_clause = """
            WHERE id IN (
                SELECT id
                FROM prediction_accuracy
                ORDER BY id DESC
                LIMIT ?
            )
        """
        parameters = (recent_limit,)

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS total_graded_predictions,
                COUNT(DISTINCT snapshot_id)
                    AS graded_scan_cycles,
                COUNT(DISTINCT symbol)
                    AS unique_tokens,

                COALESCE(
                    SUM(quote_successful),
                    0
                ) AS successful_quotes,

                COALESCE(
                    SUM(actual_profitable),
                    0
                ) AS profitable_outcomes,

                COALESCE(
                    SUM(classification_correct),
                    0
                ) AS correct_classifications,

                COALESCE(
                    SUM(false_positive),
                    0
                ) AS false_positives,

                COALESCE(
                    SUM(false_negative),
                    0
                ) AS false_negatives,

                AVG(absolute_probability_error)
                    AS mean_absolute_probability_error,

                AVG(brier_score)
                    AS average_brier_score,

                AVG(absolute_profit_error_usd)
                    AS mean_absolute_profit_error_usd,

                AVG(predicted_probability)
                    AS average_predicted_probability,

                AVG(actual_profitable) * 100.0
                    AS actual_profitable_rate,

                AVG(prediction_confidence)
                    AS average_prediction_confidence,

                MAX(graded_at)
                    AS last_graded_at

            FROM prediction_accuracy
            {where_clause}
            """,
            parameters,
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    result = dict(row) if row else {}

    total = safe_int(
        result.get("total_graded_predictions")
    )

    correct = safe_int(
        result.get("correct_classifications")
    )

    result["classification_accuracy"] = (
        correct / total * 100.0
        if total > 0
        else 0.0
    )

    integer_fields = (
        "total_graded_predictions",
        "graded_scan_cycles",
        "unique_tokens",
        "successful_quotes",
        "profitable_outcomes",
        "correct_classifications",
        "false_positives",
        "false_negatives",
    )

    float_fields = (
        "mean_absolute_probability_error",
        "average_brier_score",
        "mean_absolute_profit_error_usd",
        "average_predicted_probability",
        "actual_profitable_rate",
        "average_prediction_confidence",
        "classification_accuracy",
    )

    for key in integer_fields:
        result[key] = safe_int(
            result.get(key)
        )

    for key in float_fields:
        result[key] = safe_float(
            result.get(key)
        )

    result.setdefault(
        "last_graded_at",
        None,
    )

    return result


def get_accuracy_by_confidence_band():
    """
    Return calibration statistics grouped by confidence band.
    """

    initialize_prediction_accuracy_tables()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                confidence_band,
                COUNT(*) AS predictions,
                AVG(predicted_probability)
                    AS average_predicted_probability,
                AVG(actual_profitable) * 100.0
                    AS actual_profitable_rate,
                AVG(classification_correct) * 100.0
                    AS classification_accuracy,
                AVG(absolute_probability_error)
                    AS mean_absolute_probability_error,
                AVG(brier_score)
                    AS average_brier_score,
                AVG(absolute_profit_error_usd)
                    AS mean_absolute_profit_error_usd
            FROM prediction_accuracy
            GROUP BY confidence_band
            ORDER BY confidence_band ASC
            """
        )

        rows = [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()

    for row in rows:
        row["predictions"] = safe_int(
            row.get("predictions")
        )

        for key in (
            "average_predicted_probability",
            "actual_profitable_rate",
            "classification_accuracy",
            "mean_absolute_probability_error",
            "average_brier_score",
            "mean_absolute_profit_error_usd",
        ):
            row[key] = safe_float(
                row.get(key)
            )

    return rows


def get_token_accuracy(
    minimum_predictions=3,
    limit=100,
):
    """
    Return token-level prediction accuracy.
    """

    initialize_prediction_accuracy_tables()

    minimum_predictions = max(
        1,
        int(minimum_predictions),
    )
    limit = max(
        1,
        int(limit),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                symbol,
                COUNT(*) AS predictions,
                AVG(predicted_probability)
                    AS average_predicted_probability,
                AVG(actual_profitable) * 100.0
                    AS actual_profitable_rate,
                AVG(classification_correct) * 100.0
                    AS classification_accuracy,
                AVG(absolute_probability_error)
                    AS mean_absolute_probability_error,
                AVG(brier_score)
                    AS average_brier_score,
                AVG(absolute_profit_error_usd)
                    AS mean_absolute_profit_error_usd,
                SUM(false_positive)
                    AS false_positives,
                SUM(false_negative)
                    AS false_negatives,
                MAX(graded_at)
                    AS last_graded_at
            FROM prediction_accuracy
            GROUP BY symbol
            HAVING COUNT(*) >= ?
            ORDER BY
                average_brier_score ASC,
                mean_absolute_profit_error_usd ASC,
                predictions DESC
            LIMIT ?
            """,
            (
                minimum_predictions,
                limit,
            ),
        )

        rows = [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()

    for row in rows:
        for key in (
            "predictions",
            "false_positives",
            "false_negatives",
        ):
            row[key] = safe_int(
                row.get(key)
            )

        for key in (
            "average_predicted_probability",
            "actual_profitable_rate",
            "classification_accuracy",
            "mean_absolute_probability_error",
            "average_brier_score",
            "mean_absolute_profit_error_usd",
        ):
            row[key] = safe_float(
                row.get(key)
            )

    return rows


def delete_old_snapshots(
    keep_latest=100,
):
    """
    Delete old unneeded snapshot rows while preserving graded data.
    """

    initialize_prediction_accuracy_tables()

    keep_latest = max(
        1,
        int(keep_latest),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT DISTINCT snapshot_id
            FROM prediction_snapshots
            ORDER BY id DESC
            """
        )

        snapshot_ids = [
            row["snapshot_id"]
            for row in cursor.fetchall()
        ]

        removable_ids = snapshot_ids[
            keep_latest:
        ]

        deleted = 0

        for snapshot_id in removable_ids:
            cursor.execute(
                """
                DELETE FROM prediction_snapshots
                WHERE snapshot_id = ?
                """,
                (snapshot_id,),
            )

            deleted += cursor.rowcount

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return deleted


if __name__ == "__main__":
    initialize_prediction_accuracy_tables()

    summary = get_prediction_accuracy_summary()

    print("\nPrediction Accuracy Engine ready.")
    print(
        "Graded predictions: "
        f"{summary['total_graded_predictions']:,}"
    )
    print(
        "Classification accuracy: "
        f"{summary['classification_accuracy']:.2f}%"
    )
    print(
        "Average Brier score: "
        f"{summary['average_brier_score']:.4f}"
    )
    print(
        "Mean absolute profit error: "
        f"${summary['mean_absolute_profit_error_usd']:.6f}"
    )