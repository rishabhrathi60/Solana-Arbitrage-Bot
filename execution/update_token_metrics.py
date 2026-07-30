import time

import requests

from database.token_metrics import (
    get_metrics_progress,
    get_token_metrics_batch,
    save_token_metrics,
)


DEXSCREENER_TOKEN_PAIRS_URL = (
    "https://api.dexscreener.com/token-pairs/v1/"
    "solana/{token_mint}"
)

REQUEST_TIMEOUT_SECONDS = 20
REQUEST_WAIT_SECONDS = 1
METRICS_BATCH_SIZE = 20

SESSION = requests.Session()

SESSION.headers.update(
    {
        "User-Agent": (
            "Solana-Arbitrage-Bot/1.0"
        ),
        "Accept": "application/json",
    }
)


def safe_float(value, default=0.0):
    """
    Convert a value to float without crashing.
    """

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_token_pairs(token_mint):
    """
    Download all known Solana trading pairs for one token.
    """

    if not token_mint:
        raise ValueError("A token mint is required.")

    url = DEXSCREENER_TOKEN_PAIRS_URL.format(
        token_mint=token_mint
    )

    response = SESSION.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    pairs = response.json()

    if not isinstance(pairs, list):
        raise ValueError(
            "DEX Screener returned an unexpected response."
        )

    return pairs


def pair_contains_token(pair, token_mint):
    """
    Confirm that a returned pair contains the requested token.
    """

    base_token = pair.get("baseToken") or {}
    quote_token = pair.get("quoteToken") or {}

    base_address = base_token.get("address")
    quote_address = quote_token.get("address")

    return token_mint in {
        base_address,
        quote_address,
    }


def calculate_token_metrics(token_mint, pairs):
    """
    Calculate token-level metrics from all available Solana pairs.
    """

    solana_pairs = [
        pair
        for pair in pairs
        if pair.get("chainId") == "solana"
        and pair_contains_token(pair, token_mint)
    ]

    if not solana_pairs:
        return {
            "mint": token_mint,
            "price_usd": 0.0,
            "liquidity_usd": 0.0,
            "volume_24h_usd": 0.0,
            "pair_count": 0,
            "best_pair_address": None,
            "best_dex": None,
        }

    total_liquidity = sum(
        safe_float(
            (pair.get("liquidity") or {}).get("usd")
        )
        for pair in solana_pairs
    )

    total_volume_24h = sum(
        safe_float(
            (pair.get("volume") or {}).get("h24")
        )
        for pair in solana_pairs
    )

    best_pair = max(
        solana_pairs,
        key=lambda pair: safe_float(
            (pair.get("liquidity") or {}).get("usd")
        ),
    )

    price_usd = safe_float(
        best_pair.get("priceUsd")
    )

    return {
        "mint": token_mint,
        "price_usd": price_usd,
        "liquidity_usd": total_liquidity,
        "volume_24h_usd": total_volume_24h,
        "pair_count": len(solana_pairs),
        "best_pair_address": best_pair.get(
            "pairAddress"
        ),
        "best_dex": best_pair.get("dexId"),
    }


def print_progress():
    """
    Print overall token metrics population progress.
    """

    progress = get_metrics_progress()

    total_enabled = progress["total_enabled"]
    tokens_with_metrics = progress[
        "tokens_with_metrics"
    ]
    tokens_remaining = progress[
        "tokens_remaining"
    ]

    if total_enabled > 0:
        completion_percentage = (
            tokens_with_metrics
            / total_enabled
            * 100
        )
    else:
        completion_percentage = 0.0

    print("\nToken metrics progress:")
    print(
        f"Tokens with metrics: "
        f"{tokens_with_metrics:,}"
    )
    print(
        f"Tokens remaining: "
        f"{tokens_remaining:,}"
    )
    print(
        f"Total enabled tokens: "
        f"{total_enabled:,}"
    )
    print(
        f"Initial coverage: "
        f"{completion_percentage:.2f}%"
    )


def update_token_metrics(
    batch_size=METRICS_BATCH_SIZE,
):
    """
    Download and save metrics for the next rotating token batch.
    """

    tokens = get_token_metrics_batch(
        batch_size=batch_size
    )

    if not tokens:
        print("No eligible tokens were found.")
        return

    print(
        f"Updating metrics for the next "
        f"{len(tokens)} tokens."
    )

    successful_updates = 0
    failed_updates = 0

    for position, token in enumerate(
        tokens,
        start=1,
    ):
        symbol = token.get("symbol") or "UNKNOWN"
        token_mint = token.get("mint")
        previous_update = token.get(
            "metrics_updated_at"
        )

        if previous_update:
            update_status = (
                f"last updated {previous_update}"
            )
        else:
            update_status = "never updated"

        print(
            f"\nProcessing {position}/"
            f"{len(tokens)}: {symbol}"
        )
        print(f"  Status: {update_status}")

        try:
            pairs = get_token_pairs(token_mint)

            metrics = calculate_token_metrics(
                token_mint,
                pairs,
            )

            save_token_metrics(
                mint=metrics["mint"],
                price_usd=metrics["price_usd"],
                liquidity_usd=(
                    metrics["liquidity_usd"]
                ),
                volume_24h_usd=(
                    metrics["volume_24h_usd"]
                ),
                pair_count=metrics["pair_count"],
                best_pair_address=(
                    metrics["best_pair_address"]
                ),
                best_dex=metrics["best_dex"],
            )

            print(
                f"  Price: "
                f"${metrics['price_usd']:,.8f}"
            )
            print(
                f"  Liquidity: "
                f"${metrics['liquidity_usd']:,.2f}"
            )
            print(
                f"  24h volume: "
                f"${metrics['volume_24h_usd']:,.2f}"
            )
            print(
                f"  Pairs: "
                f"{metrics['pair_count']}"
            )

            successful_updates += 1

        except requests.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response is not None
                else "unknown"
            )

            print(
                f"  HTTP error {status_code}: "
                f"{error}"
            )

            failed_updates += 1

        except requests.RequestException as error:
            print(
                f"  Network error: {error}"
            )

            failed_updates += 1

        except (
            TypeError,
            ValueError,
            KeyError,
        ) as error:
            print(
                f"  Data error: {error}"
            )

            failed_updates += 1

        time.sleep(REQUEST_WAIT_SECONDS)

    print("\nMetrics update completed.")
    print(
        f"Successful updates: "
        f"{successful_updates}"
    )
    print(
        f"Failed updates: "
        f"{failed_updates}"
    )

    print_progress()


if __name__ == "__main__":
    update_token_metrics()