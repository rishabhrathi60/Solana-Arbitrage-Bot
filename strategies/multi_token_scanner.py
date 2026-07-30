import time

import requests

from config import MIN_PROFIT_USD, TRADE_AMOUNT_USD
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


SESSION = requests.Session()

MAXIMUM_QUOTE_ATTEMPTS = 3
BUY_SELL_WAIT_SECONDS = 2
TOKEN_WAIT_SECONDS = 3
BATCH_SIZE = 20


def get_quote(input_mint, output_mint, amount):
    """
    Request a swap quote from Jupiter.
    """

    settings = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount),
        "slippageBps": 50,
        "restrictIntermediateTokens": "true",
    }

    for attempt in range(MAXIMUM_QUOTE_ATTEMPTS):
        response = SESSION.get(
            JUPITER_QUOTE_URL,
            params=settings,
            timeout=20,
        )

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")

            try:
                wait_seconds = int(retry_after)
            except (TypeError, ValueError):
                wait_seconds = 5 * (attempt + 1)

            print(
                "Jupiter rate limit reached. "
                f"Waiting {wait_seconds} seconds."
            )

            time.sleep(wait_seconds)
            continue

        response.raise_for_status()

        quote = response.json()

        if "outAmount" not in quote:
            raise ValueError(
                f"Jupiter returned no outAmount: {quote}"
            )

        return quote

    raise requests.RequestException(
        "Jupiter rate limit continued after "
        f"{MAXIMUM_QUOTE_ATTEMPTS} attempts."
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
        "estimated_cost": ESTIMATED_EXECUTION_COST_USD,
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

    token_received = int(buy_quote["outAmount"])

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

    ending_units = int(sell_quote["outAmount"])
    ending_amount = ending_units / USDC_DECIMALS

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
        "buy_route": get_route_name(buy_quote),
        "sell_route": get_route_name(sell_quote),
        "starting_amount": TRADE_AMOUNT_USD,
        "ending_amount": ending_amount,
        "quoted_profit": quoted_profit,
        "estimated_cost": ESTIMATED_EXECUTION_COST_USD,
        "net_profit": estimated_net_profit,
        "decision": (
            "🟢 TEST FURTHER"
            if eligible
            else "🔴 SKIP"
        ),
        "eligible": eligible,
        "error": "",
    }


def scan_all_tokens():
    """
    Load a rotating batch of enabled tokens from SQLite
    and scan them.
    """

    tokens = get_token_batch(batch_size=BATCH_SIZE)
    results = []

    if not tokens:
        print(
            "Token universe is empty. "
            "Run the token-universe updater first."
        )
        return results

    print(
        f"Loaded rotating batch of {len(tokens)} tokens "
        "from the database."
    )

    for token in tokens:
        symbol = token.get("symbol") or "UNKNOWN"
        token_mint = token.get("mint")

        try:
            result = scan_one_token(
                symbol,
                token_mint,
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
                f"Unexpected error while scanning "
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
                    f"Could not update scan history "
                    f"for {symbol}: {error}"
                )

        time.sleep(TOKEN_WAIT_SECONDS)

    results.sort(
        key=lambda item: (
            item["decision"] != "⚠️ QUOTE ERROR",
            item["net_profit"],
        ),
        reverse=True,
    )

    return results