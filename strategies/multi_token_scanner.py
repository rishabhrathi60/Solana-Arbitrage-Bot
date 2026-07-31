from strategies.scanner_engine import (
    BUY_SELL_WAIT_SECONDS,
    MAXIMUM_QUOTE_ATTEMPTS,
    MAX_QUOTE_REQUESTS_PER_MINUTE,
    PARALLEL_SCANNER_ENABLED,
    PARALLEL_TEST_TOKEN_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    SCANNER_MAX_WORKERS,
    create_quote_error_result,
    get_quote,
    scan_one_token,
    scan_tokens,
)
from strategies.selection_engine import (
    AI_EXPLOITATION_RATIO,
    FALLBACK_BATCH_SIZE,
    MINIMUM_LIQUIDITY_USD,
    MINIMUM_VOLUME_24H_USD,
    USE_AI_RANKING,
    USE_MARKET_FILTER,
    calculate_adaptive_batch_size,
    load_scanner_tokens,
    remove_usdc_and_duplicates,
)


def print_selection_summary(selection):
    """
    Display scanner-selection settings.
    """

    tokens = selection["tokens"]

    print("Market filter enabled.")

    print(
        "AI opportunity ranking: "
        f"{'enabled' if USE_AI_RANKING else 'disabled'}"
    )

    if USE_AI_RANKING:
        print(
            "AI selection mix: "
            f"{AI_EXPLOITATION_RATIO * 100:.0f}% "
            "exploitation / "
            f"{(1 - AI_EXPLOITATION_RATIO) * 100:.0f}% "
            "exploration"
        )

    print(
        "Ranking source: "
        f"{selection['ranking_source']}"
    )

    print(
        "Minimum liquidity: "
        f"${MINIMUM_LIQUIDITY_USD:,.0f}"
    )

    print(
        "Minimum 24h volume: "
        f"${MINIMUM_VOLUME_24H_USD:,.0f}"
    )

    print(
        "Eligible token pool: "
        f"{selection['eligible_count']:,}"
    )

    print(
        "Adaptive target batch: "
        f"{selection['target_batch_size']:,}"
    )

    print(
        "Useful tokens loaded: "
        f"{len(tokens):,}"
    )


def scan_all_tokens():
    """
    Select and scan one adaptive token batch.
    """

    selection = load_scanner_tokens()

    tokens = selection["tokens"]

    if not tokens:
        if USE_MARKET_FILTER:
            print(
                "No tokens currently meet "
                "the market filter requirements."
            )

            print(
                "Run the token-metrics updater "
                "or temporarily lower the "
                "thresholds."
            )
        else:
            print(
                "Token universe is empty. "
                "Run the token-universe updater."
            )

        return []

    if USE_MARKET_FILTER:
        print_selection_summary(
            selection
        )
    else:
        print(
            "Market filter disabled. "
            "Using the original token universe."
        )

        print(
            "Fallback batch size: "
            f"{FALLBACK_BATCH_SIZE}"
        )

    print(
        "Parallel scanner: "
        f"{'enabled' if PARALLEL_SCANNER_ENABLED else 'disabled'}"
    )

    print(
        "Scanner workers: "
        f"{SCANNER_MAX_WORKERS}"
    )

    print(
        "Quote request ceiling: "
        f"{MAX_QUOTE_REQUESTS_PER_MINUTE}/minute"
    )

    return scan_tokens(tokens)


__all__ = [
    "AI_EXPLOITATION_RATIO",
    "BUY_SELL_WAIT_SECONDS",
    "FALLBACK_BATCH_SIZE",
    "MAXIMUM_QUOTE_ATTEMPTS",
    "MAX_QUOTE_REQUESTS_PER_MINUTE",
    "MINIMUM_LIQUIDITY_USD",
    "MINIMUM_VOLUME_24H_USD",
    "PARALLEL_SCANNER_ENABLED",
    "PARALLEL_TEST_TOKEN_LIMIT",
    "REQUEST_TIMEOUT_SECONDS",
    "SCANNER_MAX_WORKERS",
    "USE_AI_RANKING",
    "USE_MARKET_FILTER",
    "calculate_adaptive_batch_size",
    "create_quote_error_result",
    "get_quote",
    "load_scanner_tokens",
    "remove_usdc_and_duplicates",
    "scan_all_tokens",
    "scan_one_token",
    "scan_tokens",
]