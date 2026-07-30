import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE = Path(__file__).resolve().parent / "trades.db"


def get_database_connection():
    """
    Create a SQLite connection that returns rows as dictionaries.
    """

    connection = sqlite3.connect(DATABASE)
    connection.row_factory = sqlite3.Row

    return connection


def initialize_token_metrics_table():
    """
    Create the token_metrics table and indexes if they do not exist.
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

    updated_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

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
            updated_at,
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
    Return the next rotating batch of enabled tokens.

    Priority:
    1. Tokens that have never had metrics downloaded.
    2. Tokens with the oldest metrics update timestamp.
    """

    initialize_token_metrics_table()

    batch_size = max(1, int(batch_size))

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
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
        ORDER BY
            CASE
                WHEN token_metrics.metrics_updated_at IS NULL
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

    return [dict(row) for row in rows]


def get_metrics_progress():
    """
    Return metrics-population progress for enabled tokens.
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

    total_enabled = cursor.fetchone()["total_enabled"]

    cursor.execute(
        """
        SELECT COUNT(*) AS tokens_with_metrics
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
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


def get_liquid_tokens(
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
    limit=500,
):
    """
    Return enabled tokens meeting minimum market requirements.

    This includes market metrics in the returned dictionaries.
    """

    initialize_token_metrics_table()

    minimum_liquidity_usd = max(
        0.0,
        float(minimum_liquidity_usd),
    )
    minimum_volume_24h_usd = max(
        0.0,
        float(minimum_volume_24h_usd),
    )
    limit = max(1, int(limit))

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
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
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.liquidity_usd >= ?
          AND token_metrics.volume_24h_usd >= ?
        ORDER BY
            token_metrics.liquidity_usd DESC,
            token_metrics.volume_24h_usd DESC,
            token_universe.symbol ASC
        LIMIT ?
        """,
        (
            minimum_liquidity_usd,
            minimum_volume_24h_usd,
            limit,
        ),
    )

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]


def get_scanner_tokens(
    batch_size=20,
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
):
    """
    Return high-quality tokens in the format expected by the scanner.

    Requirements:
    - Token must be enabled.
    - Token must have fewer than three failed scans.
    - Token must have at least one active pair.
    - Token must meet liquidity and volume thresholds.
    - Highest-liquidity tokens are selected first.
    """

    initialize_token_metrics_table()

    batch_size = max(1, int(batch_size))
    minimum_liquidity_usd = max(
        0.0,
        float(minimum_liquidity_usd),
    )
    minimum_volume_24h_usd = max(
        0.0,
        float(minimum_volume_24h_usd),
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
            token_metrics.price_usd,
            token_metrics.liquidity_usd,
            token_metrics.volume_24h_usd,
            token_metrics.pair_count,
            token_metrics.best_pair_address,
            token_metrics.best_dex,
            token_metrics.metrics_updated_at
        FROM token_universe
        INNER JOIN token_metrics
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.liquidity_usd >= ?
          AND token_metrics.volume_24h_usd >= ?
        ORDER BY
            token_metrics.liquidity_usd DESC,
            token_metrics.volume_24h_usd DESC,
            token_universe.symbol ASC
        LIMIT ?
        """,
        (
            minimum_liquidity_usd,
            minimum_volume_24h_usd,
            batch_size,
        ),
    )

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]


def count_scanner_tokens(
    minimum_liquidity_usd=100_000,
    minimum_volume_24h_usd=25_000,
):
    """
    Count tokens currently eligible for scanner use.
    """

    initialize_token_metrics_table()

    minimum_liquidity_usd = max(
        0.0,
        float(minimum_liquidity_usd),
    )
    minimum_volume_24h_usd = max(
        0.0,
        float(minimum_volume_24h_usd),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS token_count
        FROM token_universe
        INNER JOIN token_metrics
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.liquidity_usd >= ?
          AND token_metrics.volume_24h_usd >= ?
        """,
        (
            minimum_liquidity_usd,
            minimum_volume_24h_usd,
        ),
    )

    token_count = cursor.fetchone()["token_count"]

    connection.close()

    return token_count