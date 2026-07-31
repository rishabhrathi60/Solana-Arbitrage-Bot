import json
import math
import random
import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)


# ---------------------------------------------------------
# Reinforcement-learning safety settings
# ---------------------------------------------------------

OPERATING_MODE = "PAPER"

ALLOWED_OPERATING_MODES = (
    "PAPER",
    "SHADOW",
    "CANARY_LIVE",
    "GUARDED_LIVE",
)

MINIMUM_PROMOTION_CYCLES = 20
MINIMUM_PROMOTION_OBSERVATIONS = 500
MINIMUM_PROMOTION_ADVANTAGE = 0.03
MAXIMUM_ACCEPTABLE_DRAWDOWN_USD = 0.50
MAXIMUM_ACCEPTABLE_FALSE_POSITIVE_RATE = 80.0

CHALLENGER_MUTATION_SIZE = 0.025
MINIMUM_COMPONENT_WEIGHT = 0.03
MAXIMUM_COMPONENT_WEIGHT = 0.40

MINIMUM_EXPLORATION_RATIO = 0.15
MAXIMUM_EXPLORATION_RATIO = 0.45

MODEL_STATUS_CHAMPION = "CHAMPION"
MODEL_STATUS_CHALLENGER = "CHALLENGER"
MODEL_STATUS_ARCHIVED = "ARCHIVED"


DEFAULT_MODEL = {
    "market_weight": 0.13,
    "intelligence_weight": 0.17,
    "prediction_weight": 0.25,
    "opportunity_weight": 0.10,
    "expected_profit_weight": 0.08,
    "trend_weight": 0.07,
    "stability_weight": 0.05,
    "pattern_weight": 0.15,
    "risk_penalty_weight": 0.20,
    "intelligence_confidence_weight": 0.40,
    "prediction_confidence_weight": 0.60,
    "exploration_ratio": 0.30,
}


COMPONENT_WEIGHT_KEYS = (
    "market_weight",
    "intelligence_weight",
    "prediction_weight",
    "opportunity_weight",
    "expected_profit_weight",
    "trend_weight",
    "stability_weight",
    "pattern_weight",
)


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


def clamp(value, minimum, maximum):
    """
    Restrict a value to a numeric range.
    """

    return max(
        minimum,
        min(maximum, safe_float(value)),
    )


def normalize_component_weights(model):
    """
    Normalize positive component weights so they total 1.00.
    """

    normalized = dict(model)

    for key in COMPONENT_WEIGHT_KEYS:
        normalized[key] = clamp(
            normalized.get(key),
            MINIMUM_COMPONENT_WEIGHT,
            MAXIMUM_COMPONENT_WEIGHT,
        )

    total = sum(
        normalized[key]
        for key in COMPONENT_WEIGHT_KEYS
    )

    if total <= 0:
        return dict(DEFAULT_MODEL)

    for key in COMPONENT_WEIGHT_KEYS:
        normalized[key] = (
            normalized[key] / total
        )

    normalized["risk_penalty_weight"] = clamp(
        normalized.get("risk_penalty_weight"),
        0.05,
        0.40,
    )

    intelligence_confidence = clamp(
        normalized.get(
            "intelligence_confidence_weight"
        ),
        0.10,
        0.90,
    )

    prediction_confidence = clamp(
        normalized.get(
            "prediction_confidence_weight"
        ),
        0.10,
        0.90,
    )

    confidence_total = (
        intelligence_confidence
        + prediction_confidence
    )

    normalized[
        "intelligence_confidence_weight"
    ] = (
        intelligence_confidence
        / confidence_total
    )

    normalized[
        "prediction_confidence_weight"
    ] = (
        prediction_confidence
        / confidence_total
    )

    normalized["exploration_ratio"] = clamp(
        normalized.get("exploration_ratio"),
        MINIMUM_EXPLORATION_RATIO,
        MAXIMUM_EXPLORATION_RATIO,
    )

    return normalized


def initialize_reinforcement_learning():
    """
    Create model, cycle-evaluation and event tables.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
            reinforcement_models (
                model_id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                status TEXT NOT NULL,
                operating_mode TEXT NOT NULL,

                market_weight REAL NOT NULL,
                intelligence_weight REAL NOT NULL,
                prediction_weight REAL NOT NULL,
                opportunity_weight REAL NOT NULL,
                expected_profit_weight REAL NOT NULL,
                trend_weight REAL NOT NULL,
                stability_weight REAL NOT NULL,
                pattern_weight REAL NOT NULL,

                risk_penalty_weight REAL NOT NULL,
                intelligence_confidence_weight REAL NOT NULL,
                prediction_confidence_weight REAL NOT NULL,
                exploration_ratio REAL NOT NULL,

                evaluation_cycles INTEGER NOT NULL DEFAULT 0,
                evaluation_observations INTEGER NOT NULL DEFAULT 0,
                cumulative_reward REAL NOT NULL DEFAULT 0,
                average_reward REAL NOT NULL DEFAULT 0,
                cumulative_realized_profit REAL NOT NULL DEFAULT 0,
                average_realized_profit REAL NOT NULL DEFAULT 0,
                false_positive_rate REAL NOT NULL DEFAULT 0,
                profitable_hit_rate REAL NOT NULL DEFAULT 0,
                maximum_drawdown_usd REAL NOT NULL DEFAULT 0,
                fitness_score REAL NOT NULL DEFAULT 0,

                parent_model_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                promoted_at TEXT,
                archived_at TEXT
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
            reinforcement_cycle_evaluations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                model_id INTEGER NOT NULL,
                observations INTEGER NOT NULL DEFAULT 0,
                successful_quotes INTEGER NOT NULL DEFAULT 0,
                profitable_quotes INTEGER NOT NULL DEFAULT 0,
                false_positives INTEGER NOT NULL DEFAULT 0,
                cycle_realized_profit REAL NOT NULL DEFAULT 0,
                cycle_reward REAL NOT NULL DEFAULT 0,
                top_quartile_profit REAL NOT NULL DEFAULT 0,
                whole_batch_profit REAL NOT NULL DEFAULT 0,
                ranking_lift REAL NOT NULL DEFAULT 0,
                drawdown_usd REAL NOT NULL DEFAULT 0,
                evaluated_at TEXT NOT NULL,
                UNIQUE(cycle_id, model_id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
            reinforcement_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                model_id INTEGER,
                related_model_id INTEGER,
                reason TEXT,
                details_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_reinforcement_models_status
            ON reinforcement_models(status)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_reinforcement_cycles_model
            ON reinforcement_cycle_evaluations(model_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_reinforcement_events_type
            ON reinforcement_events(event_type)
            """
        )

        connection.commit()

    finally:
        connection.close()

    ensure_champion_exists()


def insert_model(
    model_name,
    status,
    settings,
    parent_model_id=None,
):
    """
    Insert one bounded model configuration.
    """

    settings = normalize_component_weights(
        settings
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO reinforcement_models (
                model_name,
                status,
                operating_mode,
                market_weight,
                intelligence_weight,
                prediction_weight,
                opportunity_weight,
                expected_profit_weight,
                trend_weight,
                stability_weight,
                pattern_weight,
                risk_penalty_weight,
                intelligence_confidence_weight,
                prediction_confidence_weight,
                exploration_ratio,
                parent_model_id,
                created_at,
                updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                model_name,
                status,
                OPERATING_MODE,
                settings["market_weight"],
                settings["intelligence_weight"],
                settings["prediction_weight"],
                settings["opportunity_weight"],
                settings[
                    "expected_profit_weight"
                ],
                settings["trend_weight"],
                settings["stability_weight"],
                settings["pattern_weight"],
                settings[
                    "risk_penalty_weight"
                ],
                settings[
                    "intelligence_confidence_weight"
                ],
                settings[
                    "prediction_confidence_weight"
                ],
                settings["exploration_ratio"],
                parent_model_id,
                current_timestamp(),
                current_timestamp(),
            ),
        )

        model_id = cursor.lastrowid
        connection.commit()
        return model_id

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def ensure_champion_exists():
    """
    Create the initial champion when no model exists.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT model_id
            FROM reinforcement_models
            WHERE status = ?
            LIMIT 1
            """,
            (MODEL_STATUS_CHAMPION,),
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    if row:
        return int(row["model_id"])

    model_id = insert_model(
        model_name="Champion v1",
        status=MODEL_STATUS_CHAMPION,
        settings=DEFAULT_MODEL,
    )

    record_event(
        event_type="CHAMPION_CREATED",
        model_id=model_id,
        reason=(
            "Initial bounded paper-mode "
            "champion created."
        ),
        details=DEFAULT_MODEL,
    )

    return model_id


def row_to_model(row):
    """
    Convert a database row into a model dictionary.
    """

    if not row:
        return None

    model = dict(row)

    for key in (
        *COMPONENT_WEIGHT_KEYS,
        "risk_penalty_weight",
        "intelligence_confidence_weight",
        "prediction_confidence_weight",
        "exploration_ratio",
        "average_reward",
        "average_realized_profit",
        "false_positive_rate",
        "profitable_hit_rate",
        "maximum_drawdown_usd",
        "fitness_score",
    ):
        model[key] = safe_float(
            model.get(key)
        )

    for key in (
        "model_id",
        "evaluation_cycles",
        "evaluation_observations",
    ):
        model[key] = safe_int(
            model.get(key)
        )

    return model


def get_champion_config():
    """
    Return the current champion model configuration.
    """

    initialize_reinforcement_learning()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM reinforcement_models
            WHERE status = ?
            ORDER BY model_id DESC
            LIMIT 1
            """,
            (MODEL_STATUS_CHAMPION,),
        )

        return row_to_model(
            cursor.fetchone()
        )

    finally:
        connection.close()


def get_active_challenger():
    """
    Return the current shadow challenger, if present.
    """

    initialize_reinforcement_learning()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM reinforcement_models
            WHERE status = ?
            ORDER BY model_id DESC
            LIMIT 1
            """,
            (MODEL_STATUS_CHALLENGER,),
        )

        return row_to_model(
            cursor.fetchone()
        )

    finally:
        connection.close()


def record_event(
    event_type,
    model_id=None,
    related_model_id=None,
    reason="",
    details=None,
):
    """
    Write a permanent reinforcement-learning audit event.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO reinforcement_events (
                event_type,
                model_id,
                related_model_id,
                reason,
                details_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_type,
                model_id,
                related_model_id,
                str(reason or ""),
                json.dumps(
                    details or {},
                    sort_keys=True,
                ),
                current_timestamp(),
            ),
        )

        connection.commit()

    finally:
        connection.close()


def create_challenger():
    """
    Create one deterministic bounded challenger.

    The challenger runs only in shadow evaluation. It cannot
    change live execution or bypass risk controls.
    """

    initialize_reinforcement_learning()

    existing = get_active_challenger()

    if existing:
        return existing

    champion = get_champion_config()

    if not champion:
        raise RuntimeError(
            "A champion model is required."
        )

    seed = (
        champion["model_id"]
        + champion["evaluation_cycles"]
        + champion["evaluation_observations"]
    )

    random_generator = random.Random(
        seed
    )

    challenger_settings = {
        key: champion[key]
        for key in (
            *COMPONENT_WEIGHT_KEYS,
            "risk_penalty_weight",
            "intelligence_confidence_weight",
            "prediction_confidence_weight",
            "exploration_ratio",
        )
    }

    keys_to_mutate = random_generator.sample(
        list(COMPONENT_WEIGHT_KEYS),
        k=2,
    )

    direction = (
        1.0
        if seed % 2 == 0
        else -1.0
    )

    challenger_settings[
        keys_to_mutate[0]
    ] += (
        CHALLENGER_MUTATION_SIZE
        * direction
    )

    challenger_settings[
        keys_to_mutate[1]
    ] -= (
        CHALLENGER_MUTATION_SIZE
        * direction
    )

    challenger_settings[
        "risk_penalty_weight"
    ] += random_generator.choice(
        (-0.02, 0.02)
    )

    challenger_settings[
        "exploration_ratio"
    ] += random_generator.choice(
        (-0.025, 0.025)
    )

    challenger_settings = (
        normalize_component_weights(
            challenger_settings
        )
    )

    challenger_id = insert_model(
        model_name=(
            f"Challenger of "
            f"{champion['model_name']}"
        ),
        status=MODEL_STATUS_CHALLENGER,
        settings=challenger_settings,
        parent_model_id=(
            champion["model_id"]
        ),
    )

    record_event(
        event_type="CHALLENGER_CREATED",
        model_id=challenger_id,
        related_model_id=(
            champion["model_id"]
        ),
        reason=(
            "Bounded shadow challenger "
            "created automatically."
        ),
        details=challenger_settings,
    )

    return get_active_challenger()


def calculate_model_score(
    model,
    result,
):
    """
    Score one historical scanner result with a model.
    """

    market_score = clamp(
        result.get("market_score"),
        0,
        100,
    )

    intelligence_score = clamp(
        result.get("intelligence_score"),
        0,
        100,
    )

    prediction_score = clamp(
        result.get(
            "prediction_ai_priority"
        )
        or result.get(
            "ai_opportunity_score"
        ),
        0,
        100,
    )

    opportunity_probability = clamp(
        result.get(
            "opportunity_probability"
        ),
        0,
        100,
    )

    expected_profit_usd = safe_float(
        result.get("expected_profit_usd")
    )

    expected_profit_score = clamp(
        50.0
        + expected_profit_usd
        / 0.05
        * 50.0,
        0,
        100,
    )

    trend_score = clamp(
        result.get("trend_score"),
        0,
        100,
    )

    stability_score = clamp(
        result.get("stability_score"),
        0,
        100,
    )

    pattern_score = clamp(
        result.get("pattern_score")
        or 50.0,
        0,
        100,
    )

    downside_risk = clamp(
        result.get(
            "downside_risk_score"
        ),
        0,
        100,
    )

    raw_score = (
        market_score
        * model["market_weight"]
        + intelligence_score
        * model["intelligence_weight"]
        + prediction_score
        * model["prediction_weight"]
        + opportunity_probability
        * model["opportunity_weight"]
        + expected_profit_score
        * model["expected_profit_weight"]
        + trend_score
        * model["trend_weight"]
        + stability_score
        * model["stability_weight"]
        + pattern_score
        * model["pattern_weight"]
    )

    final_score = (
        raw_score
        - downside_risk
        * model["risk_penalty_weight"]
    )

    return clamp(
        final_score,
        0,
        100,
    )


def calculate_realized_reward(result):
    """
    Convert one realized scanner outcome into a bounded reward.
    """

    if not bool(
        result.get("quote_successful")
        or (
            result.get("decision")
            != "⚠️ QUOTE ERROR"
        )
    ):
        return -0.25

    net_profit = safe_float(
        result.get("net_profit")
    )

    normalized_profit = clamp(
        net_profit / 0.05,
        -2.0,
        2.0,
    )

    eligible_bonus = (
        0.15
        if bool(result.get("eligible"))
        else 0.0
    )

    profitable_bonus = (
        0.20
        if net_profit > 0
        else 0.0
    )

    return (
        normalized_profit
        + eligible_bonus
        + profitable_bonus
    )


def evaluate_model_on_cycle(
    model,
    results,
):
    """
    Evaluate one model in shadow mode on a completed scan batch.
    """

    scored_results = []

    for result in results:
        scored_results.append(
            {
                "score": calculate_model_score(
                    model,
                    result,
                ),
                "reward": (
                    calculate_realized_reward(
                        result
                    )
                ),
                "profit": safe_float(
                    result.get("net_profit")
                ),
                "quote_successful": bool(
                    result.get(
                        "quote_successful"
                    )
                    or (
                        result.get("decision")
                        != "⚠️ QUOTE ERROR"
                    )
                ),
            }
        )

    if not scored_results:
        return None

    scored_results.sort(
        key=lambda item: item["score"],
        reverse=True,
    )

    observations = len(scored_results)

    top_count = max(
        1,
        math.ceil(
            observations * 0.25
        ),
    )

    top_group = scored_results[
        :top_count
    ]

    whole_batch_profit = sum(
        item["profit"]
        for item in scored_results
        if item["quote_successful"]
    )

    top_quartile_profit = sum(
        item["profit"]
        for item in top_group
        if item["quote_successful"]
    )

    whole_average = (
        whole_batch_profit
        / max(
            1,
            sum(
                item["quote_successful"]
                for item in scored_results
            ),
        )
    )

    top_average = (
        top_quartile_profit
        / max(
            1,
            sum(
                item["quote_successful"]
                for item in top_group
            ),
        )
    )

    ranking_lift = (
        top_average - whole_average
    )

    successful_quotes = sum(
        item["quote_successful"]
        for item in scored_results
    )

    profitable_quotes = sum(
        item["quote_successful"]
        and item["profit"] > 0
        for item in scored_results
    )

    high_score_results = [
        item
        for item in scored_results
        if item["score"] >= 60.0
    ]

    false_positives = sum(
        item["quote_successful"]
        and item["profit"] <= 0
        for item in high_score_results
    )

    false_positive_rate = (
        false_positives
        / len(high_score_results)
        * 100.0
        if high_score_results
        else 0.0
    )

    running_profit = 0.0
    peak_profit = 0.0
    maximum_drawdown = 0.0

    for item in scored_results:
        if not item["quote_successful"]:
            continue

        running_profit += item["profit"]
        peak_profit = max(
            peak_profit,
            running_profit,
        )
        maximum_drawdown = max(
            maximum_drawdown,
            peak_profit - running_profit,
        )

    average_reward = sum(
        item["reward"]
        * (
            0.25
            + item["score"] / 100.0
        )
        for item in scored_results
    ) / observations

    cycle_reward = (
        average_reward
        + ranking_lift * 20.0
        - maximum_drawdown * 2.0
        - false_positive_rate / 500.0
    )

    return {
        "observations": observations,
        "successful_quotes": (
            successful_quotes
        ),
        "profitable_quotes": (
            profitable_quotes
        ),
        "false_positives": (
            false_positives
        ),
        "false_positive_rate": (
            false_positive_rate
        ),
        "cycle_realized_profit": (
            whole_batch_profit
        ),
        "cycle_reward": cycle_reward,
        "top_quartile_profit": (
            top_quartile_profit
        ),
        "whole_batch_profit": (
            whole_batch_profit
        ),
        "ranking_lift": ranking_lift,
        "drawdown_usd": (
            maximum_drawdown
        ),
    }


def save_cycle_evaluation(
    cycle_id,
    model,
    evaluation,
):
    """
    Persist one model's shadow evaluation and update totals.
    """

    if not evaluation:
        return False

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO
            reinforcement_cycle_evaluations (
                cycle_id,
                model_id,
                observations,
                successful_quotes,
                profitable_quotes,
                false_positives,
                cycle_realized_profit,
                cycle_reward,
                top_quartile_profit,
                whole_batch_profit,
                ranking_lift,
                drawdown_usd,
                evaluated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
            """,
            (
                cycle_id,
                model["model_id"],
                evaluation["observations"],
                evaluation[
                    "successful_quotes"
                ],
                evaluation[
                    "profitable_quotes"
                ],
                evaluation[
                    "false_positives"
                ],
                evaluation[
                    "cycle_realized_profit"
                ],
                evaluation["cycle_reward"],
                evaluation[
                    "top_quartile_profit"
                ],
                evaluation[
                    "whole_batch_profit"
                ],
                evaluation["ranking_lift"],
                evaluation["drawdown_usd"],
                current_timestamp(),
            ),
        )

        inserted = (
            cursor.rowcount > 0
        )

        if not inserted:
            connection.commit()
            return False

        cursor.execute(
            """
            SELECT
                COUNT(*) AS cycles,
                COALESCE(
                    SUM(observations),
                    0
                ) AS observations,
                COALESCE(
                    SUM(cycle_reward),
                    0
                ) AS cumulative_reward,
                COALESCE(
                    AVG(cycle_reward),
                    0
                ) AS average_reward,
                COALESCE(
                    SUM(cycle_realized_profit),
                    0
                ) AS cumulative_profit,
                COALESCE(
                    AVG(cycle_realized_profit),
                    0
                ) AS average_profit,
                COALESCE(
                    SUM(false_positives)
                    * 100.0
                    / NULLIF(
                        SUM(observations),
                        0
                    ),
                    0
                ) AS false_positive_rate,
                COALESCE(
                    SUM(profitable_quotes)
                    * 100.0
                    / NULLIF(
                        SUM(successful_quotes),
                        0
                    ),
                    0
                ) AS profitable_hit_rate,
                COALESCE(
                    MAX(drawdown_usd),
                    0
                ) AS maximum_drawdown
            FROM reinforcement_cycle_evaluations
            WHERE model_id = ?
            """,
            (model["model_id"],),
        )

        totals = cursor.fetchone()

        fitness_score = (
            safe_float(
                totals["average_reward"]
            )
            + safe_float(
                totals[
                    "profitable_hit_rate"
                ]
            ) / 100.0
            - safe_float(
                totals[
                    "false_positive_rate"
                ]
            ) / 200.0
            - safe_float(
                totals["maximum_drawdown"]
            )
        )

        cursor.execute(
            """
            UPDATE reinforcement_models
            SET
                evaluation_cycles = ?,
                evaluation_observations = ?,
                cumulative_reward = ?,
                average_reward = ?,
                cumulative_realized_profit = ?,
                average_realized_profit = ?,
                false_positive_rate = ?,
                profitable_hit_rate = ?,
                maximum_drawdown_usd = ?,
                fitness_score = ?,
                updated_at = ?
            WHERE model_id = ?
            """,
            (
                safe_int(totals["cycles"]),
                safe_int(
                    totals["observations"]
                ),
                safe_float(
                    totals[
                        "cumulative_reward"
                    ]
                ),
                safe_float(
                    totals["average_reward"]
                ),
                safe_float(
                    totals[
                        "cumulative_profit"
                    ]
                ),
                safe_float(
                    totals["average_profit"]
                ),
                safe_float(
                    totals[
                        "false_positive_rate"
                    ]
                ),
                safe_float(
                    totals[
                        "profitable_hit_rate"
                    ]
                ),
                safe_float(
                    totals[
                        "maximum_drawdown"
                    ]
                ),
                fitness_score,
                current_timestamp(),
                model["model_id"],
            ),
        )

        connection.commit()
        return True

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def archive_model(
    model_id,
    reason,
):
    """
    Archive a challenger and retain its full audit history.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            UPDATE reinforcement_models
            SET
                status = ?,
                archived_at = ?,
                updated_at = ?
            WHERE model_id = ?
            """,
            (
                MODEL_STATUS_ARCHIVED,
                current_timestamp(),
                current_timestamp(),
                model_id,
            ),
        )

        connection.commit()

    finally:
        connection.close()

    record_event(
        event_type="MODEL_ARCHIVED",
        model_id=model_id,
        reason=reason,
    )


def maybe_promote_challenger():
    """
    Promote only a proven paper-mode challenger.

    Live modes are intentionally ineligible for autonomous model
    promotion until a separate execution-safety gate exists.
    """

    champion = get_champion_config()
    challenger = get_active_challenger()

    if not champion or not challenger:
        return {
            "action": "NO_CHALLENGER"
        }

    if OPERATING_MODE != "PAPER":
        return {
            "action": "PAPER_ONLY"
        }

    if (
        challenger["evaluation_cycles"]
        < MINIMUM_PROMOTION_CYCLES
        or challenger[
            "evaluation_observations"
        ]
        < MINIMUM_PROMOTION_OBSERVATIONS
    ):
        return {
            "action": "KEEP_TESTING",
            "cycles": challenger[
                "evaluation_cycles"
            ],
            "observations": challenger[
                "evaluation_observations"
            ],
        }

    if (
        challenger[
            "maximum_drawdown_usd"
        ]
        > MAXIMUM_ACCEPTABLE_DRAWDOWN_USD
        or challenger[
            "false_positive_rate"
        ]
        > MAXIMUM_ACCEPTABLE_FALSE_POSITIVE_RATE
    ):
        archive_model(
            challenger["model_id"],
            reason=(
                "Challenger failed drawdown "
                "or false-positive safety gate."
            ),
        )

        create_challenger()

        return {
            "action": "ARCHIVED_UNSAFE"
        }

    required_fitness = (
        champion["fitness_score"]
        + MINIMUM_PROMOTION_ADVANTAGE
    )

    if (
        challenger["fitness_score"]
        <= required_fitness
    ):
        archive_model(
            challenger["model_id"],
            reason=(
                "Challenger completed evaluation "
                "without sufficient advantage."
            ),
        )

        create_challenger()

        return {
            "action": "ARCHIVED_WEAK"
        }

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            UPDATE reinforcement_models
            SET
                status = ?,
                archived_at = ?,
                updated_at = ?
            WHERE model_id = ?
            """,
            (
                MODEL_STATUS_ARCHIVED,
                current_timestamp(),
                current_timestamp(),
                champion["model_id"],
            ),
        )

        cursor.execute(
            """
            UPDATE reinforcement_models
            SET
                status = ?,
                promoted_at = ?,
                updated_at = ?
            WHERE model_id = ?
            """,
            (
                MODEL_STATUS_CHAMPION,
                current_timestamp(),
                current_timestamp(),
                challenger["model_id"],
            ),
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    record_event(
        event_type="CHALLENGER_PROMOTED",
        model_id=challenger["model_id"],
        related_model_id=(
            champion["model_id"]
        ),
        reason=(
            "Challenger passed minimum sample, "
            "risk and fitness gates."
        ),
        details={
            "champion_fitness": (
                champion["fitness_score"]
            ),
            "challenger_fitness": (
                challenger["fitness_score"]
            ),
        },
    )

    create_challenger()

    return {
        "action": "PROMOTED",
        "new_champion_model_id": (
            challenger["model_id"]
        ),
    }


def run_reinforcement_cycle(
    results,
    cycle_id=None,
):
    """
    Evaluate champion and challenger on one completed scan cycle.
    """

    initialize_reinforcement_learning()

    if not results:
        return {
            "evaluated_models": 0,
            "promotion_action": "NO_RESULTS",
        }

    champion = get_champion_config()
    challenger = (
        get_active_challenger()
        or create_challenger()
    )

    cycle_id = (
        cycle_id
        or current_timestamp()
    )

    evaluated_models = 0

    for model in (
        champion,
        challenger,
    ):
        if not model:
            continue

        evaluation = (
            evaluate_model_on_cycle(
                model,
                results,
            )
        )

        if save_cycle_evaluation(
            cycle_id,
            model,
            evaluation,
        ):
            evaluated_models += 1

    promotion_result = (
        maybe_promote_challenger()
    )

    return {
        "evaluated_models": (
            evaluated_models
        ),
        "cycle_id": cycle_id,
        "promotion_action": (
            promotion_result["action"]
        ),
        "champion": (
            get_champion_config()
        ),
        "challenger": (
            get_active_challenger()
        ),
    }


def get_reinforcement_summary():
    """
    Return champion and challenger learning status.
    """

    initialize_reinforcement_learning()

    return {
        "operating_mode": OPERATING_MODE,
        "champion": get_champion_config(),
        "challenger": (
            get_active_challenger()
        ),
        "promotion_requirements": {
            "minimum_cycles": (
                MINIMUM_PROMOTION_CYCLES
            ),
            "minimum_observations": (
                MINIMUM_PROMOTION_OBSERVATIONS
            ),
            "minimum_advantage": (
                MINIMUM_PROMOTION_ADVANTAGE
            ),
        },
    }


# =========================================================
# Composite fitness upgrade
# =========================================================

COMPOSITE_PROFIT_WEIGHT = 0.30
COMPOSITE_RANKING_WEIGHT = 0.20
COMPOSITE_ACCURACY_WEIGHT = 0.20
COMPOSITE_CALIBRATION_WEIGHT = 0.15
COMPOSITE_STABILITY_WEIGHT = 0.10
COMPOSITE_RISK_WEIGHT = 0.05

_legacy_initialize_reinforcement_learning = (
    initialize_reinforcement_learning
)


def _table_columns(cursor, table_name):
    cursor.execute(
        f"PRAGMA table_info({table_name})"
    )
    return {
        row[1]
        for row in cursor.fetchall()
    }


def _add_missing_columns(
    cursor,
    table_name,
    definitions,
):
    existing = _table_columns(
        cursor,
        table_name,
    )

    for column_name, definition in definitions.items():
        if column_name in existing:
            continue

        cursor.execute(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMN {column_name} {definition}
            """
        )


def initialize_reinforcement_learning():
    """
    Initialize the original controller and migrate it to the
    composite-fitness schema without deleting prior learning.
    """

    _legacy_initialize_reinforcement_learning()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        _add_missing_columns(
            cursor,
            "reinforcement_models",
            {
                "classification_accuracy":
                    "REAL NOT NULL DEFAULT 0",
                "average_brier_score":
                    "REAL NOT NULL DEFAULT 1",
                "reward_stability_score":
                    "REAL NOT NULL DEFAULT 0",
                "profit_component":
                    "REAL NOT NULL DEFAULT 0",
                "ranking_component":
                    "REAL NOT NULL DEFAULT 0",
                "accuracy_component":
                    "REAL NOT NULL DEFAULT 0",
                "calibration_component":
                    "REAL NOT NULL DEFAULT 0",
                "stability_component":
                    "REAL NOT NULL DEFAULT 0",
                "risk_component":
                    "REAL NOT NULL DEFAULT 0",
            },
        )

        _add_missing_columns(
            cursor,
            "reinforcement_cycle_evaluations",
            {
                "classification_correct":
                    "INTEGER NOT NULL DEFAULT 0",
                "classification_accuracy":
                    "REAL NOT NULL DEFAULT 0",
                "brier_score":
                    "REAL NOT NULL DEFAULT 1",
                "reward_standard_deviation":
                    "REAL NOT NULL DEFAULT 0",
                "composite_fitness":
                    "REAL NOT NULL DEFAULT 0",
            },
        )

        connection.commit()

    finally:
        connection.close()


def row_to_model(row):
    """
    Convert a model row while supporting both legacy and upgraded
    composite-fitness columns.
    """

    if not row:
        return None

    model = dict(row)

    float_keys = (
        *COMPONENT_WEIGHT_KEYS,
        "risk_penalty_weight",
        "intelligence_confidence_weight",
        "prediction_confidence_weight",
        "exploration_ratio",
        "average_reward",
        "average_realized_profit",
        "false_positive_rate",
        "profitable_hit_rate",
        "maximum_drawdown_usd",
        "fitness_score",
        "classification_accuracy",
        "average_brier_score",
        "reward_stability_score",
        "profit_component",
        "ranking_component",
        "accuracy_component",
        "calibration_component",
        "stability_component",
        "risk_component",
    )

    for key in float_keys:
        model[key] = safe_float(
            model.get(key)
        )

    for key in (
        "model_id",
        "evaluation_cycles",
        "evaluation_observations",
    ):
        model[key] = safe_int(
            model.get(key)
        )

    return model


def _standard_deviation(values):
    if len(values) < 2:
        return 0.0

    average = sum(values) / len(values)

    variance = sum(
        (value - average) ** 2
        for value in values
    ) / len(values)

    return math.sqrt(variance)


def _normalized_profit_component(value):
    """
    Convert average realized cycle profit to a bounded -1..1 score.
    """

    return math.tanh(
        safe_float(value) / 0.05
    )


def _normalized_ranking_component(value):
    """
    Convert top-quartile ranking lift to a bounded -1..1 score.
    """

    return math.tanh(
        safe_float(value) / 0.01
    )


def evaluate_model_on_cycle(
    model,
    results,
):
    """
    Evaluate ranking quality, realized profit, classification,
    calibration, stability and drawdown for one completed cycle.
    """

    scored_results = []

    for result in results:
        score = calculate_model_score(
            model,
            result,
        )

        quote_successful = bool(
            result.get("quote_successful")
            or (
                result.get("decision")
                != "⚠️ QUOTE ERROR"
            )
        )

        profit = safe_float(
            result.get("net_profit")
        )

        actual_profitable = int(
            quote_successful
            and profit > 0
        )

        predicted_probability = (
            clamp(score, 0, 100) / 100.0
        )

        predicted_positive = int(
            score >= 60.0
        )

        reward = calculate_realized_reward(
            result
        )

        scored_results.append(
            {
                "score": score,
                "reward": reward,
                "profit": profit,
                "quote_successful": (
                    quote_successful
                ),
                "actual_profitable": (
                    actual_profitable
                ),
                "classification_correct": int(
                    predicted_positive
                    == actual_profitable
                ),
                "brier_score": (
                    predicted_probability
                    - actual_profitable
                ) ** 2,
            }
        )

    if not scored_results:
        return None

    scored_results.sort(
        key=lambda item: item["score"],
        reverse=True,
    )

    observations = len(scored_results)
    top_count = max(
        1,
        math.ceil(
            observations * 0.25
        ),
    )

    top_group = scored_results[
        :top_count
    ]

    successful_items = [
        item
        for item in scored_results
        if item["quote_successful"]
    ]

    successful_top_items = [
        item
        for item in top_group
        if item["quote_successful"]
    ]

    whole_batch_profit = sum(
        item["profit"]
        for item in successful_items
    )

    top_quartile_profit = sum(
        item["profit"]
        for item in successful_top_items
    )

    whole_average = (
        whole_batch_profit
        / max(1, len(successful_items))
    )

    top_average = (
        top_quartile_profit
        / max(1, len(successful_top_items))
    )

    ranking_lift = (
        top_average - whole_average
    )

    successful_quotes = len(
        successful_items
    )

    profitable_quotes = sum(
        item["actual_profitable"]
        for item in scored_results
    )

    high_score_results = [
        item
        for item in scored_results
        if item["score"] >= 60.0
    ]

    false_positives = sum(
        item["quote_successful"]
        and item["profit"] <= 0
        for item in high_score_results
    )

    false_positive_rate = (
        false_positives
        / len(high_score_results)
        * 100.0
        if high_score_results
        else 0.0
    )

    running_profit = 0.0
    peak_profit = 0.0
    maximum_drawdown = 0.0

    for item in scored_results:
        if not item["quote_successful"]:
            continue

        running_profit += item["profit"]
        peak_profit = max(
            peak_profit,
            running_profit,
        )
        maximum_drawdown = max(
            maximum_drawdown,
            peak_profit - running_profit,
        )

    weighted_rewards = [
        item["reward"]
        * (
            0.25
            + item["score"] / 100.0
        )
        for item in scored_results
    ]

    average_reward = (
        sum(weighted_rewards)
        / observations
    )

    reward_standard_deviation = (
        _standard_deviation(
            weighted_rewards
        )
    )

    classification_correct = sum(
        item["classification_correct"]
        for item in scored_results
    )

    classification_accuracy = (
        classification_correct
        / observations
        * 100.0
    )

    average_brier_score = (
        sum(
            item["brier_score"]
            for item in scored_results
        )
        / observations
    )

    profit_component = (
        _normalized_profit_component(
            whole_average
        )
    )

    ranking_component = (
        _normalized_ranking_component(
            ranking_lift
        )
    )

    accuracy_component = (
        classification_accuracy
        / 100.0
    )

    calibration_component = clamp(
        1.0 - average_brier_score,
        0.0,
        1.0,
    )

    stability_component = clamp(
        1.0
        - reward_standard_deviation / 2.0,
        0.0,
        1.0,
    )

    risk_component = clamp(
        1.0
        - maximum_drawdown
        / max(
            MAXIMUM_ACCEPTABLE_DRAWDOWN_USD,
            0.000001,
        ),
        0.0,
        1.0,
    )

    composite_fitness = (
        profit_component
        * COMPOSITE_PROFIT_WEIGHT
        + ranking_component
        * COMPOSITE_RANKING_WEIGHT
        + accuracy_component
        * COMPOSITE_ACCURACY_WEIGHT
        + calibration_component
        * COMPOSITE_CALIBRATION_WEIGHT
        + stability_component
        * COMPOSITE_STABILITY_WEIGHT
        + risk_component
        * COMPOSITE_RISK_WEIGHT
    )

    cycle_reward = (
        average_reward
        + composite_fitness
        - false_positive_rate / 500.0
    )

    return {
        "observations": observations,
        "successful_quotes": (
            successful_quotes
        ),
        "profitable_quotes": (
            profitable_quotes
        ),
        "false_positives": (
            false_positives
        ),
        "false_positive_rate": (
            false_positive_rate
        ),
        "cycle_realized_profit": (
            whole_batch_profit
        ),
        "cycle_reward": cycle_reward,
        "top_quartile_profit": (
            top_quartile_profit
        ),
        "whole_batch_profit": (
            whole_batch_profit
        ),
        "ranking_lift": ranking_lift,
        "drawdown_usd": (
            maximum_drawdown
        ),
        "classification_correct": (
            classification_correct
        ),
        "classification_accuracy": (
            classification_accuracy
        ),
        "brier_score": (
            average_brier_score
        ),
        "reward_standard_deviation": (
            reward_standard_deviation
        ),
        "composite_fitness": (
            composite_fitness
        ),
    }


def save_cycle_evaluation(
    cycle_id,
    model,
    evaluation,
):
    """
    Persist one upgraded shadow evaluation and rebuild cumulative
    model statistics from all stored evaluation cycles.
    """

    if not evaluation:
        return False

    initialize_reinforcement_learning()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO
            reinforcement_cycle_evaluations (
                cycle_id,
                model_id,
                observations,
                successful_quotes,
                profitable_quotes,
                false_positives,
                cycle_realized_profit,
                cycle_reward,
                top_quartile_profit,
                whole_batch_profit,
                ranking_lift,
                drawdown_usd,
                classification_correct,
                classification_accuracy,
                brier_score,
                reward_standard_deviation,
                composite_fitness,
                evaluated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                cycle_id,
                model["model_id"],
                evaluation["observations"],
                evaluation[
                    "successful_quotes"
                ],
                evaluation[
                    "profitable_quotes"
                ],
                evaluation[
                    "false_positives"
                ],
                evaluation[
                    "cycle_realized_profit"
                ],
                evaluation["cycle_reward"],
                evaluation[
                    "top_quartile_profit"
                ],
                evaluation[
                    "whole_batch_profit"
                ],
                evaluation["ranking_lift"],
                evaluation["drawdown_usd"],
                evaluation[
                    "classification_correct"
                ],
                evaluation[
                    "classification_accuracy"
                ],
                evaluation["brier_score"],
                evaluation[
                    "reward_standard_deviation"
                ],
                evaluation[
                    "composite_fitness"
                ],
                current_timestamp(),
            ),
        )

        inserted = cursor.rowcount > 0

        if not inserted:
            connection.commit()
            return False

        cursor.execute(
            """
            SELECT
                COUNT(*) AS cycles,
                COALESCE(
                    SUM(observations),
                    0
                ) AS observations,
                COALESCE(
                    SUM(cycle_reward),
                    0
                ) AS cumulative_reward,
                COALESCE(
                    AVG(cycle_reward),
                    0
                ) AS average_reward,
                COALESCE(
                    SUM(cycle_realized_profit),
                    0
                ) AS cumulative_profit,
                COALESCE(
                    AVG(cycle_realized_profit),
                    0
                ) AS average_profit,
                COALESCE(
                    SUM(false_positives)
                    * 100.0
                    / NULLIF(
                        SUM(observations),
                        0
                    ),
                    0
                ) AS false_positive_rate,
                COALESCE(
                    SUM(profitable_quotes)
                    * 100.0
                    / NULLIF(
                        SUM(successful_quotes),
                        0
                    ),
                    0
                ) AS profitable_hit_rate,
                COALESCE(
                    MAX(drawdown_usd),
                    0
                ) AS maximum_drawdown,
                COALESCE(
                    SUM(classification_correct)
                    * 100.0
                    / NULLIF(
                        SUM(observations),
                        0
                    ),
                    0
                ) AS classification_accuracy,
                COALESCE(
                    AVG(brier_score),
                    1
                ) AS average_brier_score,
                COALESCE(
                    AVG(
                        reward_standard_deviation
                    ),
                    0
                ) AS reward_std,
                COALESCE(
                    AVG(ranking_lift),
                    0
                ) AS average_ranking_lift
            FROM reinforcement_cycle_evaluations
            WHERE model_id = ?
            """,
            (model["model_id"],),
        )

        totals = cursor.fetchone()

        average_profit = safe_float(
            totals["average_profit"]
        )

        classification_accuracy = (
            safe_float(
                totals[
                    "classification_accuracy"
                ]
            )
        )

        average_brier_score = safe_float(
            totals["average_brier_score"]
        )

        reward_std = safe_float(
            totals["reward_std"]
        )

        maximum_drawdown = safe_float(
            totals["maximum_drawdown"]
        )

        profit_component = (
            _normalized_profit_component(
                average_profit
            )
        )

        ranking_component = (
            _normalized_ranking_component(
                totals["average_ranking_lift"]
            )
        )

        accuracy_component = (
            classification_accuracy
            / 100.0
        )

        calibration_component = clamp(
            1.0 - average_brier_score,
            0.0,
            1.0,
        )

        stability_component = clamp(
            1.0 - reward_std / 2.0,
            0.0,
            1.0,
        )

        risk_component = clamp(
            1.0
            - maximum_drawdown
            / max(
                MAXIMUM_ACCEPTABLE_DRAWDOWN_USD,
                0.000001,
            ),
            0.0,
            1.0,
        )

        fitness_score = (
            profit_component
            * COMPOSITE_PROFIT_WEIGHT
            + ranking_component
            * COMPOSITE_RANKING_WEIGHT
            + accuracy_component
            * COMPOSITE_ACCURACY_WEIGHT
            + calibration_component
            * COMPOSITE_CALIBRATION_WEIGHT
            + stability_component
            * COMPOSITE_STABILITY_WEIGHT
            + risk_component
            * COMPOSITE_RISK_WEIGHT
        )

        cursor.execute(
            """
            UPDATE reinforcement_models
            SET
                evaluation_cycles = ?,
                evaluation_observations = ?,
                cumulative_reward = ?,
                average_reward = ?,
                cumulative_realized_profit = ?,
                average_realized_profit = ?,
                false_positive_rate = ?,
                profitable_hit_rate = ?,
                maximum_drawdown_usd = ?,
                classification_accuracy = ?,
                average_brier_score = ?,
                reward_stability_score = ?,
                profit_component = ?,
                ranking_component = ?,
                accuracy_component = ?,
                calibration_component = ?,
                stability_component = ?,
                risk_component = ?,
                fitness_score = ?,
                updated_at = ?
            WHERE model_id = ?
            """,
            (
                safe_int(totals["cycles"]),
                safe_int(
                    totals["observations"]
                ),
                safe_float(
                    totals[
                        "cumulative_reward"
                    ]
                ),
                safe_float(
                    totals["average_reward"]
                ),
                safe_float(
                    totals[
                        "cumulative_profit"
                    ]
                ),
                average_profit,
                safe_float(
                    totals[
                        "false_positive_rate"
                    ]
                ),
                safe_float(
                    totals[
                        "profitable_hit_rate"
                    ]
                ),
                maximum_drawdown,
                classification_accuracy,
                average_brier_score,
                stability_component * 100.0,
                profit_component,
                ranking_component,
                accuracy_component,
                calibration_component,
                stability_component,
                risk_component,
                fitness_score,
                current_timestamp(),
                model["model_id"],
            ),
        )

        connection.commit()
        return True

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    initialize_reinforcement_learning()

    summary = get_reinforcement_summary()

    print(
        "\nReinforcement Learning Controller ready."
    )
    print(
        "Operating mode: "
        f"{summary['operating_mode']}"
    )

    champion = summary["champion"]
    challenger = summary["challenger"]

    print(
        "Champion: "
        f"{champion['model_name']} "
        f"(model {champion['model_id']})"
    )

    if challenger:
        print(
            "Challenger: "
            f"{challenger['model_name']} "
            f"(model {challenger['model_id']})"
        )
    else:
        challenger = create_challenger()

        print(
            "Challenger created: "
            f"{challenger['model_name']} "
            f"(model {challenger['model_id']})"
        )