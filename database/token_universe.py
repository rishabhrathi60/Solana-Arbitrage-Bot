import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE = Path(__file__).resolve().parent / "trades.db"


def initialize_token_table():
    """
    Create the token-universe table if it does not exist.
    """

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS token_universe (
            mint TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            decimals INTEGER,
            enabled INTEGER DEFAULT 1,
            last_scan TEXT,
            successful_scans INTEGER DEFAULT 0,
            failed_scans INTEGER DEFAULT 0
        )
        """
    )

    conn.commit()
    conn.close()


def save_tokens(tokens):
    """
    Save newly downloaded tokens without deleting scan history.
    """

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    for token in tokens:
        cursor.execute(
            """
            INSERT INTO token_universe (
                mint,
                symbol,
                name,
                decimals
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(mint) DO UPDATE SET
                symbol = excluded.symbol,
                name = excluded.name,
                decimals = excluded.decimals
            """,
            (
                token["mint"],
                token["symbol"],
                token["name"],
                token["decimals"],
            ),
        )

    conn.commit()
    conn.close()


def get_enabled_tokens():
    """
    Return all enabled tokens.
    """

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM token_universe
        WHERE enabled = 1
        ORDER BY symbol
        """
    )

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_token_batch(batch_size=20):
    """
    Return a rotating batch of enabled tokens.

    Tokens scanned least recently are returned first.
    Tokens with three or more failed scans are excluded.
    """

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(
            "batch_size must be a positive integer."
        )

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM token_universe
        WHERE enabled = 1
          AND failed_scans < 3
        ORDER BY
            CASE
                WHEN last_scan IS NULL THEN 0
                ELSE 1
            END,
            last_scan ASC,
            successful_scans DESC,
            symbol ASC
        LIMIT ?
        """,
        (batch_size,),
    )

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def mark_token_scanned(mint, successful):
    """
    Update scan history for one token.

    A failed scan increases failed_scans. The token is
    automatically disabled after its third failed scan.
    """

    if not mint:
        raise ValueError(
            "A token mint is required."
        )

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    scanned_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    if successful:
        cursor.execute(
            """
            UPDATE token_universe
            SET
                last_scan = ?,
                successful_scans = successful_scans + 1
            WHERE mint = ?
            """,
            (
                scanned_at,
                mint,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE token_universe
            SET
                last_scan = ?,
                failed_scans = failed_scans + 1,
                enabled = CASE
                    WHEN failed_scans + 1 >= 3 THEN 0
                    ELSE enabled
                END
            WHERE mint = ?
            """,
            (
                scanned_at,
                mint,
            ),
        )

    if cursor.rowcount == 0:
        conn.close()
        raise ValueError(
            f"Token mint was not found: {mint}"
        )

    conn.commit()
    conn.close()


def get_disabled_tokens(limit=20):
    """
    Return tokens automatically disabled after failures.
    """

    if not isinstance(limit, int) or limit <= 0:
        raise ValueError(
            "limit must be a positive integer."
        )

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            mint,
            symbol,
            name,
            failed_scans,
            successful_scans,
            last_scan,
            enabled
        FROM token_universe
        WHERE enabled = 0
        ORDER BY
            failed_scans DESC,
            last_scan DESC
        LIMIT ?
        """,
        (limit,),
    )

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]