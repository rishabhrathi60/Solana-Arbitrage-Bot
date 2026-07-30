import os
import time

import requests
from dotenv import load_dotenv

from config import MIN_PROFIT_USD, TRADE_AMOUNT_USD
from database.token_metrics import (
    count_scanner_tokens,
    get_scanner_tokens,
)
from database.token_universe import (
    get_token_batch,
    mark_token_scanned,
)
from strategies.arbitrage import (
    ESTIMATED_EXECUTION_COST_USD,
    JUPITER_QUOTE_URL,
    USDC_DECIMALS,
    USDC_MINT,
    get_route_name,
)


load_dotenv()

JUPITER_API_KEY = os.getenv("JUPITER_API_KEY")

if not JUPITER_API_KEY:
    raise RuntimeError(
        "JUPITER_API_KEY is missing from the .env file."
    )


SESSION = requests.Session()

SESSION.headers.update(
    {
        "x-api-key": JUPITER_API_KEY,
        "Accept": "application/json",
        "User-Agent": "Solana-Arbitrage-Bot/1.0",
    }
)


MAXIMUM_QUOTE_ATTEMPTS = 3
BUY_SELL_WAIT_SECONDS = 2
TOKEN_WAIT_SECONDS = 3

# Set this to False to use the original token universe.
USE_MARKET_FILTER = True

MINIMUM_LIQUIDITY_USD = 50_000
MINIMUM_VOLUME_24H_USD = 10_000

# Adaptive scanner rules.
SMALL_POOL_MAXIMUM = 30
MEDIUM_POOL_MAXIMUM = 250

MEDIUM_POOL_BATCH_SIZE = 50
LARGE_POOL_BATCH_SIZE = 100

# Used only when the market filter is disabled.
FALLBACK_BATCH_SIZE = 20


def get_quote(input_mint, output_mint, amount):
    """
    Request a swap quote from Jupiter.
    """

    if not input_mint:
        raise ValueError("The input mint is missing.")

    if not output_mint:
        raise ValueError("The output mint is missing.")

    if input_mint == output_mint:
        raise ValueError(
            "The input and output mint cannot be the same."
        )

    amount = int(amount)

    if amount <= 0:
        raise ValueError(
            "The Jupiter quote amount must be greater than zero."
        )

    settings = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": 50,
        "restrictIntermediateTokens": "true",
    }

    last_error = None

    for attempt in range(
        1,
        MAXIMUM_QUOTE_ATTEMPTS + 1,
    ):
        try:
            response = SESSION.get(
                JUPITER_QUOTE_URL,
                params=settings,
                timeout=20,
            )

            if response.status_code == 429:
                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    wait_seconds = float(retry_after)
                except (TypeError, ValueError):
                    wait_seconds = 5 * attempt

                print(
                    "Jupiter rate limit reached. "
                    f"Waiting {wait_seconds:.0f} seconds."
                )

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            quote = response.json()

            if not isinstance(quote, dict):
                raise ValueError(
                    "Jupiter returned an unexpected response."
                )

            if "outAmount" not in quote:
                raise ValueError(
                    "Jupiter returned no outAmount: "
                    f"{quote}"
                )

            return quote

        except requests.RequestException as error:
            last_error = error

            if attempt >= MAXIMUM_QUOTE_ATTEMPTS:
                break

            wait_seconds = 2 * attempt

            print(
                f"Jupiter request failed on attempt "
                f"{attempt}/{MAXIMUM_QUOTE_ATTEMPTS}. "
                f"Waiting {wait_seconds} seconds."
            )

            time.sleep(wait_seconds)

    raise requests.RequestException(
        "Jupiter quote request failed after "
        f"{MAXIMUM_QUOTE_ATTEMPTS} attempts: "
        f"{last_error}"
    )


def create_quote_error_result(
    symbol,
    error,
    token=None,
):
    """
    Create a standard result when a token cannot be quoted.
    """

    token = token or {}

    return {
        "token": symbol,
        "buy_route": "Unavailable",
        "sell_route": "Unavailable",
        "starting_amount": TRADE_AMOUNT_USD,
        "ending_amount": 0.0,
        "quoted_profit": 0.0,
        "estimated_cost": (
            ESTIMATED_EXECUTION_COST_USD
        ),
        "net_profit": 0.0,
        "decision": "⚠️ QUOTE ERROR",
        "eligible": False,
        "error": str(error),
        "market_score": float(
            token.get("market_score") or 0
        ),
        "liquidity_score": float(
            token.get("liquidity_score") or 0
        ),
        "volume_score": float(
            token.get("volume_score") or 0
        ),
        "pair_score": float(
            token.get("pair_score") or 0
        ),
    }


def scan_one_token(symbol, token_mint, token=None):
    """
    Test a USDC-to-token-to-USDC round trip.
    """

    token = token or {}

    if not symbol:
        raise ValueError("Token symbol is missing.")

    if not token_mint:
        raise ValueError(
            f"The mint address is missing for {symbol}."
        )

    if token_mint == USDC_MINT:
        raise ValueError(
            "USDC cannot be scanned against itself."
        )

    starting_units = int(
        TRADE_AMOUNT_USD * USDC_DECIMALS
    )

    buy_quote = get_quote(
        USDC_MINT,
        token_mint,
        starting_units,
    )

    token_received = int(
        buy_quote["outAmount"]
    )

    if token_received <= 0:
        raise ValueError(
            f"Jupiter returned zero tokens for {symbol}."
        )

    time.sleep(BUY_SELL_WAIT_SECONDS)

    sell_quote = get_quote(
        token_mint,
        USDC_MINT,
        token_received,
    )

    ending_units = int(
        sell_quote["outAmount"]
    )

    ending_amount = (
        ending_units / USDC_DECIMALS
    )

    quoted_profit = (
        ending_amount - TRADE_AMOUNT_USD
    )

    estimated_net_profit = (
        quoted_profit
        - ESTIMATED_EXECUTION_COST_USD
    )

    eligible = (
        estimated_net_profit >= MIN_PROFIT_USD
    )

    return {
        "token": symbol,
        "buy_route": get_route_name(
            buy_quote
        ),
        "sell_route": get_route_name(
            sell_quote
        ),
        "starting_amount": TRADE_AMOUNT_USD,
        "ending_amount": ending_amount,
        "quoted_profit": quoted_profit,
        "estimated_cost": (
            ESTIMATED_EXECUTION_COST_USD
        ),
        "net_profit": estimated_net_profit,
        "decision": (
            "🟢 TEST FURTHER"
            if eligible
            else "🔴 SKIP"
        ),
        "eligible": eligible,
        "error": "",
        "market_score": float(
            token.get("market_score") or 0
        ),
        "liquidity_score": float(
            token.get("liquidity_score") or 0
        ),
        "volume_score": float(
            token.get("volume_score") or 0
        ),
        "pair_score": float(
            token.get("pair_score") or 0
        ),
    }


def calculate_adaptive_batch_size(
    eligible_token_count,
):
    """
    Choose scanner batch size from the eligible-token count.

    Rules:
    - 1 to 30 eligible tokens: scan all.
    - 31 to 250 eligible tokens: scan 50.
    - More than 250 eligible tokens: scan 100.
    """

    eligible_token_count = max(
        0,
        int(eligible_token_count),
    )

    if eligible_token_count == 0:
        return 0

    if eligible_token_count <= SMALL_POOL_MAXIMUM:
        return eligible_token_count

    if eligible_token_count <= MEDIUM_POOL_MAXIMUM:
        return min(
            MEDIUM_POOL_BATCH_SIZE,
            eligible_token_count,
        )

    return min(
        LARGE_POOL_BATCH_SIZE,
        eligible_token_count,
    )


def remove_usdc_and_duplicates(tokens):
    """
    Remove USDC and duplicate token mints.
    """

    cleaned_tokens = []
    seen_mints = set()

    for token in tokens:
        token_mint = token.get("mint")

        if not token_mint:
            continue

        if token_mint == USDC_MINT:
            continue

        if token_mint in seen_mints:
            continue

        seen_mints.add(token_mint)
        cleaned_tokens.append(token)

    return cleaned_tokens


def load_adaptive_scanner_tokens():
    """
    Load an adaptive smart-ranked scanner batch.

    If USDC appears in a batch, another token is requested so
    the useful scanner batch does not become unnecessarily
    smaller.
    """

    eligible_count = count_scanner_tokens(
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    target_batch_size = calculate_adaptive_batch_size(
        eligible_count
    )

    if target_batch_size <= 0:
        return [], eligible_count, 0

    tokens = get_scanner_tokens(
        batch_size=target_batch_size,
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    tokens = remove_usdc_and_duplicates(tokens)

    # USDC may have occupied one position in the first batch.
    # Request only the missing number of useful tokens.
    refill_attempts = 0
    maximum_refill_attempts = 3

    while (
        len(tokens) < target_batch_size
        and refill_attempts < maximum_refill_attempts
        and eligible_count > len(tokens)
    ):
        refill_attempts += 1

        missing_count = (
            target_batch_size - len(tokens)
        )

        extra_tokens = get_scanner_tokens(
            batch_size=missing_count,
            minimum_liquidity_usd=(
                MINIMUM_LIQUIDITY_USD
            ),
            minimum_volume_24h_usd=(
                MINIMUM_VOLUME_24H_USD
            ),
        )

        combined_tokens = (
            tokens + extra_tokens
        )

        updated_tokens = remove_usdc_and_duplicates(
            combined_tokens
        )

        if len(updated_tokens) == len(tokens):
            break

        tokens = updated_tokens

    return (
        tokens[:target_batch_size],
        eligible_count,
        target_batch_size,
    )


def load_scanner_tokens():
    """
    Load smart-filtered tokens or the original token batch.
    """

    if USE_MARKET_FILTER:
        (
            tokens,
            eligible_count,
            target_batch_size,
        ) = load_adaptive_scanner_tokens()

        print("Market filter enabled.")
        print("Smart market scoring enabled.")
        print("Adaptive batch sizing enabled.")

        print(
            f"Minimum liquidity: "
            f"${MINIMUM_LIQUIDITY_USD:,.0f}"
        )
        print(
            f"Minimum 24h volume: "
            f"${MINIMUM_VOLUME_24H_USD:,.0f}"
        )
        print(
            f"Eligible token pool: "
            f"{eligible_count:,}"
        )
        print(
            f"Adaptive target batch: "
            f"{target_batch_size:,}"
        )
        print(
            f"Useful tokens loaded: "
            f"{len(tokens):,}"
        )

        return tokens

    print(
        "Market filter disabled. "
        "Using the original rotating token batch."
    )

    tokens = get_token_batch(
        batch_size=FALLBACK_BATCH_SIZE + 1
    )

    tokens = remove_usdc_and_duplicates(tokens)

    return tokens[:FALLBACK_BATCH_SIZE]


def print_token_market_details(token):
    """
    Display stored market metrics and smart scores.
    """

    market_score = float(
        token.get("market_score") or 0
    )
    liquidity_score = float(
        token.get("liquidity_score") or 0
    )
    volume_score = float(
        token.get("volume_score") or 0
    )
    pair_score = float(
        token.get("pair_score") or 0
    )

    liquidity = float(
        token.get("liquidity_usd") or 0
    )
    volume = float(
        token.get("volume_24h_usd") or 0
    )
    pair_count = int(
        token.get("pair_count") or 0
    )

    print(
        f"  Market score: "
        f"{market_score:.2f}/100"
    )
    print(
        "  Score details: "
        f"liquidity {liquidity_score:.2f}, "
        f"volume {volume_score:.2f}, "
        f"pairs {pair_score:.2f}"
    )
    print(
        f"  Liquidity: ${liquidity:,.2f}"
    )
    print(
        f"  24h volume: ${volume:,.2f}"
    )
    print(
        f"  Trading pairs: {pair_count}"
    )


def scan_all_tokens():
    """
    Load an adaptive token batch and scan it.
    """

    tokens = load_scanner_tokens()
    results = []

    if not tokens:
        if USE_MARKET_FILTER:
            print(
                "No tokens currently meet the market "
                "filter requirements."
            )
            print(
                "Run the token-metrics updater again "
                "or temporarily lower the thresholds."
            )
        else:
            print(
                "Token universe is empty. "
                "Run the token-universe updater first."
            )

        return results

    if USE_MARKET_FILTER:
        print(
            f"Loaded {len(tokens)} smart-ranked, "
            "filtered tokens from the database."
        )
    else:
        print(
            f"Loaded rotating batch of {len(tokens)} "
            "tokens from the database."
        )

    for position, token in enumerate(
        tokens,
        start=1,
    ):
        symbol = (
            token.get("symbol") or "UNKNOWN"
        )
        token_mint = token.get("mint")

        print(
            f"\nScanning {position}/"
            f"{len(tokens)}: {symbol}"
        )

        if USE_MARKET_FILTER:
            print_token_market_details(token)

        try:
            result = scan_one_token(
                symbol,
                token_mint,
                token=token,
            )

            print(
                f"{symbol}: net profit "
                f"${result['net_profit']:.6f} "
                f"{result['decision']}"
            )

        except (
            requests.RequestException,
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            print(
                f"Could not scan {symbol}: {error}"
            )

            result = create_quote_error_result(
                symbol,
                error,
                token=token,
            )

        except Exception as error:
            print(
                "Unexpected error while scanning "
                f"{symbol}: {error}"
            )

            result = create_quote_error_result(
                symbol,
                error,
                token=token,
            )

        results.append(result)

        if token_mint:
            try:
                mark_token_scanned(
                    token_mint,
                    successful=(
                        result["decision"]
                        != "⚠️ QUOTE ERROR"
                    ),
                )

            except Exception as error:
                print(
                    "Could not update scan history "
                    f"for {symbol}: {error}"
                )

        if position < len(tokens):
            time.sleep(TOKEN_WAIT_SECONDS)

    results.sort(
        key=lambda item: (
            item["decision"]
            != "⚠️ QUOTE ERROR",
            item["net_profit"],
            item.get("market_score", 0),
        ),
        reverse=True,
    )

    successful_count = sum(
        result["decision"]
        != "⚠️ QUOTE ERROR"
        for result in results
    )

    failed_count = (
        len(results) - successful_count
    )

    eligible_count = sum(
        bool(result["eligible"])
        for result in results
    )

    print("\nBatch scan completed.")
    print(
        f"Tokens scanned: {len(results)}"
    )
    print(
        f"Successful quotes: "
        f"{successful_count}"
    )
    print(
        f"Quote errors: {failed_count}"
    )
    print(
        f"Eligible opportunities: "
        f"{eligible_count}"
    )

    return results