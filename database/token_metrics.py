import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE = Path(__file__).resolve().parent / "trades.db"

SCANNER_OFFSET_KEY = "filtered_scanner_offset"

LIQUIDITY_SCORE_WEIGHT = 0.45
VOLUME_SCORE_WEIGHT = 0.40
PAIR_SCORE_WEIGHT = 0.15


def get_database_connection():
    """
    Create a SQLite connection that returns rows as dictionaries.
    """

    connection = sqlite3.connect(
        DATABASE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row

    return connection


def current_timestamp():
    """
    Return the current local timestamp in database format.
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def initialize_token_metrics_table():
    """
    Create the token metrics and scanner state tables.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS token_metrics (
            mint TEXT PRIMARY KEY,
            price_usd REAL DEFAULT 0,
            liquidity_usd REAL DEFAULT 0,
            volume_24h_usd REAL DEFAULT 0,
            pair_count INTEGER DEFAULT 0,
            best_pair_address TEXT,
            best_dex TEXT,
            metrics_updated_at TEXT,
            FOREIGN KEY (mint)
                REFERENCES token_universe(mint)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scanner_state (
            state_key TEXT PRIMARY KEY,
            state_value INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_token_metrics_liquidity
        ON token_metrics(liquidity_usd)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_token_metrics_volume
        ON token_metrics(volume_24h_usd)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_token_metrics_updated_at
        ON token_metrics(metrics_updated_at)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_token_metrics_pair_count
        ON token_metrics(pair_count)
        """
    )

    cursor.execute(
        """
        INSERT OR IGNORE INTO scanner_state (
            state_key,
            state_value,
            updated_at
        )
        VALUES (?, 0, ?)
        """,
        (
            SCANNER_OFFSET_KEY,
            current_timestamp(),
        ),
    )

    connection.commit()
    connection.close()


def save_token_metrics(
    mint,
    price_usd,
    liquidity_usd,
    volume_24h_usd,
    pair_count,
    best_pair_address=None,
    best_dex=None,
):
    """
    Insert or update market metrics for one token.
    """

    if not mint:
        raise ValueError("A token mint is required.")

    initialize_token_metrics_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO token_metrics (
            mint,
            price_usd,
            liquidity_usd,
            volume_24h_usd,
            pair_count,
            best_pair_address,
            best_dex,
            metrics_updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mint) DO UPDATE SET
            price_usd = excluded.price_usd,
            liquidity_usd = excluded.liquidity_usd,
            volume_24h_usd = excluded.volume_24h_usd,
            pair_count = excluded.pair_count,
            best_pair_address = excluded.best_pair_address,
            best_dex = excluded.best_dex,
            metrics_updated_at = excluded.metrics_updated_at
        """,
        (
            mint,
            float(price_usd or 0),
            float(liquidity_usd or 0),
            float(volume_24h_usd or 0),
            int(pair_count or 0),
            best_pair_address,
            best_dex,
            current_timestamp(),
        ),
    )

    connection.commit()
    connection.close()


def get_token_metrics(mint):
    """
    Return stored metrics for one token.
    """

    if not mint:
        return None

    initialize_token_metrics_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM token_metrics
        WHERE mint = ?
        """,
        (mint,),
    )

    row = cursor.fetchone()
    connection.close()

    return dict(row) if row else None


def get_token_metrics_batch(batch_size=20):
    """
    Return the next rotating metrics-update batch.

    Priority:
    1. Tokens that have never had metrics downloaded.
    2. Tokens with the oldest metrics timestamp.
    """

    initialize_token_metrics_table()

    batch_size = max(
        1,
        int(batch_size),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            token_universe.mint,
            token_universe.symbol,
            token_universe.name,
            token_universe.decimals,
            token_metrics.metrics_updated_at
        FROM token_universe
        LEFT JOIN token_metrics
            ON token_universe.mint =
               token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(
                token_universe.failed_scans,
                0
              ) < 3
        ORDER BY
            CASE
                WHEN token_metrics.metrics_updated_at
                     IS NULL
                THEN 0
                ELSE 1
            END ASC,
            token_metrics.metrics_updated_at ASC,
            token_universe.mint ASC
        LIMIT ?
        """,
        (batch_size,),
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def get_metrics_progress():
    """
    Return metrics-population progress.
    """

    initialize_token_metrics_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS total_enabled
        FROM token_universe
        WHERE enabled = 1
          AND COALESCE(failed_scans, 0) < 3
        """
    )

    total_enabled = cursor.fetchone()[
        "total_enabled"
    ]

    cursor.execute(
        """
        SELECT COUNT(*) AS tokens_with_metrics
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint =
               token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(
                token_universe.failed_scans,
                0
              ) < 3
        """
    )

    tokens_with_metrics = cursor.fetchone()[
        "tokens_with_metrics"
    ]

    connection.close()

    tokens_remaining = max(
        total_enabled - tokens_with_metrics,
        0,
    )

    return {
        "total_enabled": total_enabled,
        "tokens_with_metrics": tokens_with_metrics,
        "tokens_remaining": tokens_remaining,
    }


def validate_scanner_filters(
    minimum_liquidity_usd,
    minimum_volume_24h_usd,
):
    """
    Validate and normalize scanner filter values.
    """

    minimum_liquidity_usd = max(
        0.0,
        float(minimum_liquidity_usd),
    )

    minimum_volume_24h_usd = max(
        0.0,
        float(minimum_volume_24h_usd),
    )

    return (
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    )


def get_scored_token_query():
    """
    Return the shared SQL query used to score eligible tokens.

    Scores are percentile-based:

    - Liquidity score: 0 to 100
    - Volume score: 0 to 100
    - Pair score: 0 to 100
    - Market score: weighted total from 0 to 100

    Percentile scoring prevents one extremely large token from
    compressing every other token's score.
    """

    return """
        WITH eligible_tokens AS (
            SELECT
                token_universe.mint,
                token_universe.symbol,
                token_universe.name,
                token_universe.decimals,
                token_metrics.price_usd,
                token_metrics.liquidity_usd,
                token_metrics.volume_24h_usd,
                token_metrics.pair_count,
                token_metrics.best_pair_address,
                token_metrics.best_dex,
                token_metrics.metrics_updated_at
            FROM token_universe
            INNER JOIN token_metrics
                ON token_universe.mint =
                   token_metrics.mint
            WHERE token_universe.enabled = 1
              AND COALESCE(
                    token_universe.failed_scans,
                    0
                  ) < 3
              AND token_metrics.pair_count > 0
              AND token_metrics.liquidity_usd >= ?
              AND token_metrics.volume_24h_usd >= ?
        ),
        percentile_scores AS (
            SELECT
                eligible_tokens.*,

                PERCENT_RANK() OVER (
                    ORDER BY liquidity_usd ASC
                ) * 100.0 AS liquidity_score,

                PERCENT_RANK() OVER (
                    ORDER BY volume_24h_usd ASC
                ) * 100.0 AS volume_score,

                PERCENT_RANK() OVER (
                    ORDER BY pair_count ASC
                ) * 100.0 AS pair_score

            FROM eligible_tokens
        ),
        scored_tokens AS (
            SELECT
                percentile_scores.*,

                (
                    liquidity_score * ?
                    + volume_score * ?
                    + pair_score * ?
                ) AS market_score

            FROM percentile_scores
        )
        SELECT
            mint,
            symbol,
            name,
            decimals,
            price_usd,
            liquidity_usd,
            volume_24h_usd,
            pair_count,
            best_pair_address,
            best_dex,
            metrics_updated_at,
            ROUND(liquidity_score, 2)
                AS liquidity_score,
            ROUND(volume_score, 2)
                AS volume_score,
            ROUND(pair_score, 2)
                AS pair_score,
            ROUND(market_score, 2)
                AS market_score
        FROM scored_tokens
    """


def get_scoring_parameters(
    minimum_liquidity_usd,
    minimum_volume_24h_usd,
):
    """
    Return SQL parameters for the scored token query.
    """

    return (
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
        LIQUIDITY_SCORE_WEIGHT,
        VOLUME_SCORE_WEIGHT,
        PAIR_SCORE_WEIGHT,
    )


def get_liquid_tokens(
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
    limit=500,
):
    """
    Return eligible tokens ordered by market score.

    This function does not change scanner rotation.
    """

    initialize_token_metrics_table()

    (
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    ) = validate_scanner_filters(
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    )

    limit = max(
        1,
        int(limit),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    query = (
        get_scored_token_query()
        + """
        ORDER BY
            market_score DESC,
            liquidity_usd DESC,
            volume_24h_usd DESC,
            pair_count DESC,
            symbol ASC,
            mint ASC
        LIMIT ?
        """
    )

    cursor.execute(
        query,
        (
            *get_scoring_parameters(
                minimum_liquidity_usd,
                minimum_volume_24h_usd,
            ),
            limit,
        ),
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def count_scanner_tokens(
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
):
    """
    Count tokens currently eligible for scanning.
    """

    initialize_token_metrics_table()

    (
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    ) = validate_scanner_filters(
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS token_count
        FROM token_universe
        INNER JOIN token_metrics
            ON token_universe.mint =
               token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(
                token_universe.failed_scans,
                0
              ) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.liquidity_usd >= ?
          AND token_metrics.volume_24h_usd >= ?
        """,
        (
            minimum_liquidity_usd,
            minimum_volume_24h_usd,
        ),
    )

    token_count = cursor.fetchone()[
        "token_count"
    ]

    connection.close()

    return token_count


def get_scanner_tokens(
    batch_size=20,
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
):
    """
    Return the next rotating batch of filtered tokens.

    Eligible tokens are ranked by market score before applying
    the persistent scanner rotation.

    Market score:
    - 45% liquidity percentile
    - 40% 24-hour volume percentile
    - 15% trading-pair percentile

    The scanner position is stored in SQLite, so rotation
    continues after restarting the bot.
    """

    initialize_token_metrics_table()

    batch_size = max(
        1,
        int(batch_size),
    )

    (
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    ) = validate_scanner_filters(
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    )

    connection = get_database_connection()

    try:
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT COUNT(*) AS token_count
            FROM token_universe
            INNER JOIN token_metrics
                ON token_universe.mint =
                   token_metrics.mint
            WHERE token_universe.enabled = 1
              AND COALESCE(
                    token_universe.failed_scans,
                    0
                  ) < 3
              AND token_metrics.pair_count > 0
              AND token_metrics.liquidity_usd >= ?
              AND token_metrics.volume_24h_usd >= ?
            """,
            (
                minimum_liquidity_usd,
                minimum_volume_24h_usd,
            ),
        )

        total_eligible = cursor.fetchone()[
            "token_count"
        ]

        if total_eligible <= 0:
            cursor.execute(
                """
                INSERT INTO scanner_state (
                    state_key,
                    state_value,
                    updated_at
                )
                VALUES (?, 0, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    state_value = 0,
                    updated_at = excluded.updated_at
                """,
                (
                    SCANNER_OFFSET_KEY,
                    current_timestamp(),
                ),
            )

            connection.commit()
            return []

        cursor.execute(
            """
            SELECT state_value
            FROM scanner_state
            WHERE state_key = ?
            """,
            (SCANNER_OFFSET_KEY,),
        )

        state_row = cursor.fetchone()

        current_offset = (
            int(state_row["state_value"])
            if state_row
            else 0
        )

        current_offset %= total_eligible

        number_to_return = min(
            batch_size,
            total_eligible,
        )

        scored_query = (
            get_scored_token_query()
            + """
            ORDER BY
                market_score DESC,
                liquidity_usd DESC,
                volume_24h_usd DESC,
                pair_count DESC,
                symbol ASC,
                mint ASC
            LIMIT ?
            OFFSET ?
            """
        )

        cursor.execute(
            scored_query,
            (
                *get_scoring_parameters(
                    minimum_liquidity_usd,
                    minimum_volume_24h_usd,
                ),
                number_to_return,
                current_offset,
            ),
        )

        rows = list(
            cursor.fetchall()
        )

        remaining_needed = (
            number_to_return - len(rows)
        )

        if remaining_needed > 0:
            cursor.execute(
                scored_query,
                (
                    *get_scoring_parameters(
                        minimum_liquidity_usd,
                        minimum_volume_24h_usd,
                    ),
                    remaining_needed,
                    0,
                ),
            )

            rows.extend(
                cursor.fetchall()
            )

        next_offset = (
            current_offset + len(rows)
        ) % total_eligible

        cursor.execute(
            """
            INSERT INTO scanner_state (
                state_key,
                state_value,
                updated_at
            )
            VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at
            """,
            (
                SCANNER_OFFSET_KEY,
                next_offset,
                current_timestamp(),
            ),
        )

        connection.commit()

        return [
            dict(row)
            for row in rows
        ]

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def get_top_scored_tokens(
    minimum_liquidity_usd=50_000,
    minimum_volume_24h_usd=10_000,
    limit=20,
):
    """
    Return the highest-scoring eligible tokens without changing
    the scanner rotation.
    """

    return get_liquid_tokens(
        minimum_liquidity_usd=minimum_liquidity_usd,
        minimum_volume_24h_usd=minimum_volume_24h_usd,
        limit=limit,
    )


def get_scanner_rotation_status(
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
):
    """
    Return scanner offset, eligible count and progress.
    """

    initialize_token_metrics_table()

    eligible_count = count_scanner_tokens(
        minimum_liquidity_usd,
        minimum_volume_24h_usd,
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT state_value, updated_at
        FROM scanner_state
        WHERE state_key = ?
        """,
        (SCANNER_OFFSET_KEY,),
    )

    row = cursor.fetchone()
    connection.close()

    current_offset = (
        int(row["state_value"])
        if row
        else 0
    )

    if eligible_count > 0:
        current_offset %= eligible_count

        rotation_percentage = (
            current_offset
            / eligible_count
            * 100
        )
    else:
        current_offset = 0
        rotation_percentage = 0.0

    return {
        "current_offset": current_offset,
        "eligible_tokens": eligible_count,
        "rotation_percentage": (
            rotation_percentage
        ),
        "updated_at": (
            row["updated_at"]
            if row
            else None
        ),
    }


def reset_scanner_rotation():
    """
    Reset filtered scanner rotation to the first ranked token.
    """

    initialize_token_metrics_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO scanner_state (
            state_key,
            state_value,
            updated_at
        )
        VALUES (?, 0, ?)
        ON CONFLICT(state_key) DO UPDATE SET
            state_value = 0,
            updated_at = excluded.updated_at
        """,
        (
            SCANNER_OFFSET_KEY,
            current_timestamp(),
        ),
    )

    connection.commit()
    connection.close()