import os
import time

import requests
from dotenv import load_dotenv

from config import MIN_PROFIT_USD, TRADE_AMOUNT_USD
from database.token_metrics import get_scanner_tokens
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
BATCH_SIZE = 20

# Set this to False to use the original rotating token universe.
USE_MARKET_FILTER = True

# Preliminary filters while token-metrics coverage is still low.
MINIMUM_LIQUIDITY_USD = 50_000
MINIMUM_VOLUME_24H_USD = 10_000


def get_quote(input_mint, output_mint, amount):
    """
    Request a swap quote from Jupiter.
    """

    if not input_mint:
        raise ValueError("The input mint is missing.")

    if not output_mint:
        raise ValueError("The output mint is missing.")

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


def create_quote_error_result(symbol, error):
    """
    Create a standard result when a token cannot be quoted.
    """

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
    }


def scan_one_token(symbol, token_mint):
    """
    Test a USDC-to-token-to-USDC round trip.
    """

    if not symbol:
        raise ValueError("Token symbol is missing.")

    if not token_mint:
        raise ValueError(
            f"The mint address is missing for {symbol}."
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
    }


def load_scanner_tokens():
    """
    Load either filtered market tokens or the original
    rotating token batch.
    """

    if USE_MARKET_FILTER:
        tokens = get_scanner_tokens(
            batch_size=BATCH_SIZE,
            minimum_liquidity_usd=(
                MINIMUM_LIQUIDITY_USD
            ),
            minimum_volume_24h_usd=(
                MINIMUM_VOLUME_24H_USD
            ),
        )

        print(
            "Market filter enabled."
        )
        print(
            f"Minimum liquidity: "
            f"${MINIMUM_LIQUIDITY_USD:,.0f}"
        )
        print(
            f"Minimum 24h volume: "
            f"${MINIMUM_VOLUME_24H_USD:,.0f}"
        )

        return tokens

    print(
        "Market filter disabled. "
        "Using the original rotating token batch."
    )

    return get_token_batch(
        batch_size=BATCH_SIZE
    )


def scan_all_tokens():
    """
    Load a token batch from SQLite and scan it.
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
            f"Loaded {len(tokens)} filtered tokens "
            "from the database."
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
            liquidity = (
                token.get("liquidity_usd") or 0
            )
            volume = (
                token.get("volume_24h_usd") or 0
            )
            pair_count = (
                token.get("pair_count") or 0
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

        try:
            result = scan_one_token(
                symbol,
                token_mint,
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
            )

        except Exception as error:
            print(
                "Unexpected error while scanning "
                f"{symbol}: {error}"
            )

            result = create_quote_error_result(
                symbol,
                error,
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