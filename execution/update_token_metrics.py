import threading
import time
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)

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

# Start conservatively. We can tune these after testing.
METRICS_BATCH_SIZE = 250
MAX_WORKERS = 5

# DEX Screener documents up to 300 requests/minute for this
# category. We intentionally stay below it.
MAX_REQUESTS_PER_MINUTE = 240
MINIMUM_REQUEST_INTERVAL = (
    60.0 / MAX_REQUESTS_PER_MINUTE
)

MAXIMUM_REQUEST_ATTEMPTS = 3
BASE_RETRY_WAIT_SECONDS = 2


_rate_limit_lock = threading.Lock()
_last_request_started_at = 0.0

_thread_local = threading.local()


def get_session():
    """
    Create one requests session per worker thread.

    Sharing one Session across several threads is avoided.
    """

    if not hasattr(thread_local := _thread_local, "session"):
        thread_local.session = requests.Session()

        thread_local.session.headers.update(
            {
                "User-Agent": (
                    "Solana-Arbitrage-Bot/1.0"
                ),
                "Accept": "application/json",
            }
        )

    return thread_local.session


def wait_for_request_slot():
    """
    Apply one process-wide request-start rate limit.

    Threads may download concurrently, but new requests are
    started at a controlled interval.
    """

    global _last_request_started_at

    with _rate_limit_lock:
        current_time = time.monotonic()

        elapsed = (
            current_time - _last_request_started_at
        )

        wait_seconds = max(
            0.0,
            MINIMUM_REQUEST_INTERVAL - elapsed,
        )

        if wait_seconds > 0:
            time.sleep(wait_seconds)

        _last_request_started_at = time.monotonic()


def safe_float(value, default=0.0):
    """
    Convert a value to float without crashing.
    """

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def pair_contains_token(pair, token_mint):
    """
    Confirm that a returned pair contains the requested token.
    """

    base_token = pair.get("baseToken") or {}
    quote_token = pair.get("quoteToken") or {}

    return token_mint in {
        base_token.get("address"),
        quote_token.get("address"),
    }


def get_token_pairs(token_mint):
    """
    Download all known Solana pairs for one token.

    Retries temporary network errors, rate limits and server
    failures.
    """

    if not token_mint:
        raise ValueError("A token mint is required.")

    url = DEXSCREENER_TOKEN_PAIRS_URL.format(
        token_mint=token_mint
    )

    session = get_session()
    last_error = None

    for attempt in range(
        1,
        MAXIMUM_REQUEST_ATTEMPTS + 1,
    ):
        wait_for_request_slot()

        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    wait_seconds = float(retry_after)
                except (TypeError, ValueError):
                    wait_seconds = (
                        BASE_RETRY_WAIT_SECONDS
                        * attempt
                    )

                last_error = requests.HTTPError(
                    "DEX Screener rate limit reached.",
                    response=response,
                )

                if attempt < MAXIMUM_REQUEST_ATTEMPTS:
                    time.sleep(wait_seconds)
                    continue

            if 500 <= response.status_code < 600:
                last_error = requests.HTTPError(
                    "DEX Screener server error.",
                    response=response,
                )

                if attempt < MAXIMUM_REQUEST_ATTEMPTS:
                    time.sleep(
                        BASE_RETRY_WAIT_SECONDS
                        * attempt
                    )
                    continue

            response.raise_for_status()

            pairs = response.json()

            if not isinstance(pairs, list):
                raise ValueError(
                    "DEX Screener returned an "
                    "unexpected response."
                )

            return pairs

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:
            last_error = error

            if attempt < MAXIMUM_REQUEST_ATTEMPTS:
                time.sleep(
                    BASE_RETRY_WAIT_SECONDS
                    * attempt
                )
                continue

        except requests.RequestException as error:
            last_error = error
            break

    raise requests.RequestException(
        "DEX Screener request failed after "
        f"{MAXIMUM_REQUEST_ATTEMPTS} attempts: "
        f"{last_error}"
    )


def calculate_token_metrics(token_mint, pairs):
    """
    Calculate token-level metrics from all available pairs.
    """

    solana_pairs = [
        pair
        for pair in pairs
        if pair.get("chainId") == "solana"
        and pair_contains_token(
            pair,
            token_mint,
        )
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
            (pair.get("liquidity") or {}).get(
                "usd"
            )
        )
        for pair in solana_pairs
    )

    total_volume_24h = sum(
        safe_float(
            (pair.get("volume") or {}).get(
                "h24"
            )
        )
        for pair in solana_pairs
    )

    best_pair = max(
        solana_pairs,
        key=lambda pair: safe_float(
            (pair.get("liquidity") or {}).get(
                "usd"
            )
        ),
    )

    return {
        "mint": token_mint,
        "price_usd": safe_float(
            best_pair.get("priceUsd")
        ),
        "liquidity_usd": total_liquidity,
        "volume_24h_usd": total_volume_24h,
        "pair_count": len(solana_pairs),
        "best_pair_address": best_pair.get(
            "pairAddress"
        ),
        "best_dex": best_pair.get("dexId"),
    }


def fetch_token_metrics(token):
    """
    Worker task: download and calculate one token's metrics.

    Database writes are intentionally performed later in the
    main thread to avoid unnecessary SQLite write contention.
    """

    symbol = token.get("symbol") or "UNKNOWN"
    token_mint = token.get("mint")

    if not token_mint:
        return {
            "success": False,
            "symbol": symbol,
            "mint": None,
            "error": "Token mint is missing.",
        }

    try:
        pairs = get_token_pairs(token_mint)

        metrics = calculate_token_metrics(
            token_mint,
            pairs,
        )

        return {
            "success": True,
            "symbol": symbol,
            "mint": token_mint,
            "metrics": metrics,
        }

    except (
        requests.RequestException,
        TypeError,
        ValueError,
        KeyError,
    ) as error:
        return {
            "success": False,
            "symbol": symbol,
            "mint": token_mint,
            "error": str(error),
        }

    except Exception as error:
        return {
            "success": False,
            "symbol": symbol,
            "mint": token_mint,
            "error": (
                "Unexpected worker error: "
                f"{error}"
            ),
        }


def print_progress():
    """
    Print overall metrics-population progress.
    """

    progress = get_metrics_progress()

    total_enabled = progress["total_enabled"]
    tokens_with_metrics = progress[
        "tokens_with_metrics"
    ]
    tokens_remaining = progress[
        "tokens_remaining"
    ]

    completion_percentage = (
        tokens_with_metrics
        / total_enabled
        * 100
        if total_enabled
        else 0.0
    )

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
    max_workers=MAX_WORKERS,
):
    """
    Download metrics concurrently and save them to SQLite.

    Network requests run in worker threads. SQLite writes remain
    in the main thread.
    """

    batch_size = max(1, int(batch_size))
    max_workers = max(1, int(max_workers))

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
    print(f"Worker threads: {max_workers}")
    print(
        "Request ceiling: "
        f"{MAX_REQUESTS_PER_MINUTE}/minute"
    )

    started_at = time.monotonic()

    successful_updates = 0
    failed_updates = 0
    completed_count = 0

    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="metrics-worker",
    ) as executor:
        future_to_token = {
            executor.submit(
                fetch_token_metrics,
                token,
            ): token
            for token in tokens
        }

        for future in as_completed(
            future_to_token
        ):
            completed_count += 1
            token = future_to_token[future]

            symbol = (
                token.get("symbol") or "UNKNOWN"
            )

            try:
                result = future.result()
            except Exception as error:
                result = {
                    "success": False,
                    "symbol": symbol,
                    "error": (
                        "Worker future failed: "
                        f"{error}"
                    ),
                }

            print(
                f"\nCompleted {completed_count}/"
                f"{len(tokens)}: "
                f"{result['symbol']}"
            )

            if not result["success"]:
                print(
                    f"  Failed: {result['error']}"
                )
                failed_updates += 1
                continue

            metrics = result["metrics"]

            try:
                save_token_metrics(
                    mint=metrics["mint"],
                    price_usd=(
                        metrics["price_usd"]
                    ),
                    liquidity_usd=(
                        metrics["liquidity_usd"]
                    ),
                    volume_24h_usd=(
                        metrics["volume_24h_usd"]
                    ),
                    pair_count=(
                        metrics["pair_count"]
                    ),
                    best_pair_address=(
                        metrics[
                            "best_pair_address"
                        ]
                    ),
                    best_dex=metrics["best_dex"],
                )

            except Exception as error:
                print(
                    "  Database save failed: "
                    f"{error}"
                )
                failed_updates += 1
                continue

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

    elapsed_seconds = (
        time.monotonic() - started_at
    )

    processed_count = (
        successful_updates + failed_updates
    )

    tokens_per_minute = (
        processed_count
        / elapsed_seconds
        * 60
        if elapsed_seconds > 0
        else 0.0
    )

    print("\nParallel metrics update completed.")
    print(
        f"Successful updates: "
        f"{successful_updates}"
    )
    print(
        f"Failed updates: "
        f"{failed_updates}"
    )
    print(
        f"Elapsed time: "
        f"{elapsed_seconds:.1f} seconds"
    )
    print(
        f"Processing speed: "
        f"{tokens_per_minute:.1f} tokens/minute"
    )

    print_progress()


if __name__ == "__main__":
    update_token_metrics()