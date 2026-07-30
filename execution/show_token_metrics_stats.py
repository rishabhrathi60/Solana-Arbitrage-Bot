import sqlite3

from database.token_metrics import (
    DATABASE,
    initialize_token_metrics_table,
)


LIQUIDITY_THRESHOLDS = [
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
]

VOLUME_THRESHOLDS = [
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
]


def get_connection():
    """
    Create a SQLite connection that returns dictionary-like rows.
    """

    connection = sqlite3.connect(DATABASE)
    connection.row_factory = sqlite3.Row

    return connection


def get_overall_statistics(connection):
    """
    Return overall token-metrics coverage and market totals.
    """

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
        SELECT
            COUNT(*) AS tokens_with_metrics,
            SUM(
                CASE
                    WHEN token_metrics.pair_count > 0
                    THEN 1
                    ELSE 0
                END
            ) AS tokens_with_pairs,
            SUM(
                CASE
                    WHEN token_metrics.liquidity_usd > 0
                    THEN 1
                    ELSE 0
                END
            ) AS tokens_with_liquidity,
            SUM(
                CASE
                    WHEN token_metrics.volume_24h_usd > 0
                    THEN 1
                    ELSE 0
                END
            ) AS tokens_with_volume,
            COALESCE(
                SUM(token_metrics.liquidity_usd),
                0
            ) AS total_liquidity,
            COALESCE(
                SUM(token_metrics.volume_24h_usd),
                0
            ) AS total_volume
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
        """
    )

    metrics = dict(cursor.fetchone())

    tokens_with_metrics = (
        metrics["tokens_with_metrics"] or 0
    )

    coverage_percentage = (
        tokens_with_metrics / total_enabled * 100
        if total_enabled
        else 0.0
    )

    metrics["total_enabled"] = total_enabled
    metrics["coverage_percentage"] = (
        coverage_percentage
    )
    metrics["tokens_remaining"] = max(
        total_enabled - tokens_with_metrics,
        0,
    )

    return metrics


def count_tokens_above_liquidity(
    connection,
    minimum_liquidity,
):
    """
    Count tokens meeting one liquidity requirement.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS token_count
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.liquidity_usd >= ?
        """,
        (minimum_liquidity,),
    )

    return cursor.fetchone()["token_count"]


def count_tokens_above_volume(
    connection,
    minimum_volume,
):
    """
    Count tokens meeting one 24-hour volume requirement.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS token_count
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.volume_24h_usd >= ?
        """,
        (minimum_volume,),
    )

    return cursor.fetchone()["token_count"]


def count_tokens_above_both(
    connection,
    minimum_liquidity,
    minimum_volume,
):
    """
    Count tokens meeting liquidity and volume requirements.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS token_count
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
          AND token_metrics.liquidity_usd >= ?
          AND token_metrics.volume_24h_usd >= ?
        """,
        (
            minimum_liquidity,
            minimum_volume,
        ),
    )

    return cursor.fetchone()["token_count"]


def get_top_tokens(connection, limit=10):
    """
    Return the highest-liquidity tokens currently stored.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            token_universe.symbol,
            token_universe.name,
            token_metrics.price_usd,
            token_metrics.liquidity_usd,
            token_metrics.volume_24h_usd,
            token_metrics.pair_count,
            token_metrics.best_dex
        FROM token_metrics
        INNER JOIN token_universe
            ON token_universe.mint = token_metrics.mint
        WHERE token_universe.enabled = 1
          AND COALESCE(token_universe.failed_scans, 0) < 3
          AND token_metrics.pair_count > 0
        ORDER BY
            token_metrics.liquidity_usd DESC,
            token_metrics.volume_24h_usd DESC
        LIMIT ?
        """,
        (limit,),
    )

    return [dict(row) for row in cursor.fetchall()]


def print_overall_statistics(statistics):
    """
    Display overall metrics coverage.
    """

    print("\nTOKEN METRICS COVERAGE")
    print("-" * 55)

    print(
        f"Enabled tokens:        "
        f"{statistics['total_enabled']:,}"
    )
    print(
        f"Tokens with metrics:   "
        f"{statistics['tokens_with_metrics']:,}"
    )
    print(
        f"Tokens remaining:      "
        f"{statistics['tokens_remaining']:,}"
    )
    print(
        f"Coverage:              "
        f"{statistics['coverage_percentage']:.2f}%"
    )
    print(
        f"Tokens with pairs:     "
        f"{statistics['tokens_with_pairs'] or 0:,}"
    )
    print(
        f"Tokens with liquidity: "
        f"{statistics['tokens_with_liquidity'] or 0:,}"
    )
    print(
        f"Tokens with volume:    "
        f"{statistics['tokens_with_volume'] or 0:,}"
    )
    print(
        f"Stored liquidity:      "
        f"${statistics['total_liquidity']:,.2f}"
    )
    print(
        f"Stored 24h volume:     "
        f"${statistics['total_volume']:,.2f}"
    )


def print_liquidity_statistics(connection):
    """
    Display token counts for several liquidity thresholds.
    """

    print("\nLIQUIDITY THRESHOLDS")
    print("-" * 55)

    for threshold in LIQUIDITY_THRESHOLDS:
        token_count = count_tokens_above_liquidity(
            connection,
            threshold,
        )

        print(
            f"Liquidity >= ${threshold:>10,.0f}: "
            f"{token_count:>5,} tokens"
        )


def print_volume_statistics(connection):
    """
    Display token counts for several volume thresholds.
    """

    print("\n24-HOUR VOLUME THRESHOLDS")
    print("-" * 55)

    for threshold in VOLUME_THRESHOLDS:
        token_count = count_tokens_above_volume(
            connection,
            threshold,
        )

        print(
            f"Volume >= ${threshold:>13,.0f}: "
            f"{token_count:>5,} tokens"
        )


def print_combined_statistics(connection):
    """
    Display practical scanner-filter combinations.
    """

    combinations = [
        (25_000, 5_000),
        (50_000, 10_000),
        (100_000, 25_000),
        (250_000, 50_000),
        (500_000, 100_000),
    ]

    print("\nCOMBINED SCANNER FILTERS")
    print("-" * 55)

    for liquidity, volume in combinations:
        token_count = count_tokens_above_both(
            connection,
            liquidity,
            volume,
        )

        print(
            f"Liquidity >= ${liquidity:>9,.0f} | "
            f"Volume >= ${volume:>9,.0f}: "
            f"{token_count:>5,}"
        )


def print_top_tokens(tokens):
    """
    Display the best currently stored tokens.
    """

    print("\nTOP TOKENS BY LIQUIDITY")
    print("-" * 90)

    if not tokens:
        print("No tokens with active pairs were found.")
        return

    print(
        f"{'Symbol':<12}"
        f"{'Liquidity':>18}"
        f"{'24h Volume':>18}"
        f"{'Pairs':>8}"
        f"{'DEX':>16}"
    )

    print("-" * 90)

    for token in tokens:
        symbol = token.get("symbol") or "UNKNOWN"
        liquidity = token.get("liquidity_usd") or 0
        volume = token.get("volume_24h_usd") or 0
        pair_count = token.get("pair_count") or 0
        dex = token.get("best_dex") or "Unknown"

        print(
            f"{symbol[:11]:<12}"
            f"${liquidity:>17,.2f}"
            f"${volume:>17,.2f}"
            f"{pair_count:>8}"
            f"{dex[:15]:>16}"
        )


def show_token_metrics_report():
    """
    Generate the complete market-quality report.
    """

    initialize_token_metrics_table()

    connection = get_connection()

    try:
        statistics = get_overall_statistics(
            connection
        )

        print("\nSOLANA TOKEN MARKET REPORT")
        print("=" * 55)

        print_overall_statistics(statistics)
        print_liquidity_statistics(connection)
        print_volume_statistics(connection)
        print_combined_statistics(connection)

        top_tokens = get_top_tokens(
            connection,
            limit=10,
        )

        print_top_tokens(top_tokens)

        if statistics["coverage_percentage"] < 25:
            print(
                "\nNOTE: Coverage is still low. "
                "Threshold counts are preliminary."
            )
            print(
                "Continue running the metrics updater "
                "before selecting final scanner filters."
            )

    finally:
        connection.close()


if __name__ == "__main__":
    show_token_metrics_report()