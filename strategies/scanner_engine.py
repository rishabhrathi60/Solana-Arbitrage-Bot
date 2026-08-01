import os
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)

import requests
from dotenv import load_dotenv

from config import (
    MIN_PROFIT_USD,
    TRADE_AMOUNT_USD,
)
from database.token_universe import (
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

JUPITER_API_KEY = os.getenv(
    "JUPITER_API_KEY"
)

if not JUPITER_API_KEY:
    raise RuntimeError(
        "JUPITER_API_KEY is missing "
        "from the .env file."
    )


MAXIMUM_QUOTE_ATTEMPTS = 3
REQUEST_TIMEOUT_SECONDS = 20
BUY_SELL_WAIT_SECONDS = 2

PARALLEL_SCANNER_ENABLED = True
SCANNER_MAX_WORKERS = 3

MAX_QUOTE_REQUESTS_PER_MINUTE = 60

MINIMUM_QUOTE_REQUEST_INTERVAL = (
    60.0 / MAX_QUOTE_REQUESTS_PER_MINUTE
)

PARALLEL_TEST_TOKEN_LIMIT = None


_thread_local = threading.local()
_rate_limit_lock = threading.Lock()
_last_quote_request_at = 0.0


def safe_float(value):
    """
    Convert a value to float safely.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def utc_now():
    """
    Return the current timezone-aware UTC time.
    """

    return datetime.now(timezone.utc)


def utc_text(value=None):
    """
    Return an ISO-8601 UTC timestamp with millisecond precision.
    """

    timestamp = value or utc_now()

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    return timestamp.astimezone(
        timezone.utc
    ).isoformat(
        timespec="milliseconds"
    )


def parse_quote_source_time(quote):
    """
    Read a provider timestamp when Jupiter supplies one.

    Jupiter quote responses do not always include a source timestamp, so this
    safely returns None when no recognized field exists.
    """

    if not isinstance(quote, dict):
        return None

    raw_value = (
        quote.get("timestamp")
        or quote.get("quoteTimestamp")
        or quote.get("timeTakenAt")
        or quote.get("createdAt")
        or quote.get("contextTimestamp")
    )

    if raw_value is None:
        return None

    try:
        if isinstance(raw_value, (int, float)):
            numeric = float(raw_value)

            if numeric > 10_000_000_000:
                numeric /= 1_000.0

            parsed = datetime.fromtimestamp(
                numeric,
                tz=timezone.utc,
            )

        else:
            text = str(raw_value).strip()

            if not text:
                return None

            if text.endswith("Z"):
                text = text[:-1] + "+00:00"

            parsed = datetime.fromisoformat(text)

            if parsed.tzinfo is None:
                parsed = parsed.replace(
                    tzinfo=timezone.utc
                )

            parsed = parsed.astimezone(
                timezone.utc
            )

        return parsed

    except (
        OSError,
        OverflowError,
        TypeError,
        ValueError,
    ):
        return None


def get_thread_session():
    """
    Return one HTTP session for the current worker.
    """

    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        session.headers.update(
            {
                "x-api-key": JUPITER_API_KEY,
                "Accept": "application/json",
                "User-Agent": (
                    "Solana-Arbitrage-Bot/2.0"
                ),
            }
        )

        _thread_local.session = session

    return _thread_local.session


def wait_for_quote_request_slot():
    """
    Apply a process-wide Jupiter request-start limit.
    """

    global _last_quote_request_at

    with _rate_limit_lock:
        current_time = time.monotonic()

        elapsed = (
            current_time
            - _last_quote_request_at
        )

        wait_seconds = max(
            0.0,
            (
                MINIMUM_QUOTE_REQUEST_INTERVAL
                - elapsed
            ),
        )

        if wait_seconds > 0:
            time.sleep(wait_seconds)

        _last_quote_request_at = (
            time.monotonic()
        )


def get_quote(
    input_mint,
    output_mint,
    amount,
):
    """
    Request a Jupiter quote with retries and timing instrumentation.

    The returned quote dictionary includes private instrumentation fields:
      _quote_started_at
      _quote_received_at
      _quote_source_time
      _quote_latency_ms
      _quote_age_ms
      _quote_attempts

    These fields do not affect quote calculations or routing logic.
    """

    if not input_mint:
        raise ValueError(
            "The input mint is missing."
        )

    if not output_mint:
        raise ValueError(
            "The output mint is missing."
        )

    if input_mint == output_mint:
        raise ValueError(
            "The input and output mint "
            "cannot be the same."
        )

    amount = int(amount)

    if amount <= 0:
        raise ValueError(
            "The Jupiter quote amount must "
            "be greater than zero."
        )

    settings = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": 50,
        "restrictIntermediateTokens": "true",
    }

    session = get_thread_session()
    last_error = None

    for attempt in range(
        1,
        MAXIMUM_QUOTE_ATTEMPTS + 1,
    ):
        wait_for_quote_request_slot()

        request_started_at = utc_now()
        request_started_monotonic = (
            time.perf_counter()
        )

        try:
            response = session.get(
                JUPITER_QUOTE_URL,
                params=settings,
                timeout=(
                    REQUEST_TIMEOUT_SECONDS
                ),
            )

            request_received_at = utc_now()
            request_latency_ms = max(
                0.0,
                (
                    time.perf_counter()
                    - request_started_monotonic
                )
                * 1_000.0,
            )

            if response.status_code == 429:
                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                try:
                    wait_seconds = float(
                        retry_after
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    wait_seconds = 5 * attempt

                last_error = (
                    requests.HTTPError(
                        "Jupiter rate limit "
                        "reached.",
                        response=response,
                    )
                )

                if (
                    attempt
                    < MAXIMUM_QUOTE_ATTEMPTS
                ):
                    print(
                        "Jupiter rate limit "
                        "reached. Waiting "
                        f"{wait_seconds:.0f} "
                        "seconds."
                    )

                    time.sleep(wait_seconds)
                    continue

            if (
                500
                <= response.status_code
                < 600
            ):
                wait_seconds = 2 * attempt

                last_error = (
                    requests.HTTPError(
                        "Jupiter returned a "
                        "temporary server error.",
                        response=response,
                    )
                )

                if (
                    attempt
                    < MAXIMUM_QUOTE_ATTEMPTS
                ):
                    print(
                        "Jupiter temporary "
                        "server error. Waiting "
                        f"{wait_seconds} seconds."
                    )

                    time.sleep(wait_seconds)
                    continue

            response.raise_for_status()

            quote = response.json()

            if not isinstance(quote, dict):
                raise ValueError(
                    "Jupiter returned an "
                    "unexpected response."
                )

            if "outAmount" not in quote:
                raise ValueError(
                    "Jupiter returned no "
                    f"outAmount: {quote}"
                )

            source_time = parse_quote_source_time(
                quote
            )

            quote_age_ms = (
                max(
                    0.0,
                    (
                        request_received_at
                        - source_time
                    ).total_seconds()
                    * 1_000.0,
                )
                if source_time is not None
                else 0.0
            )

            instrumented_quote = dict(quote)
            instrumented_quote.update(
                {
                    "_quote_started_at": utc_text(
                        request_started_at
                    ),
                    "_quote_received_at": utc_text(
                        request_received_at
                    ),
                    "_quote_source_time": (
                        utc_text(source_time)
                        if source_time is not None
                        else None
                    ),
                    "_quote_latency_ms": (
                        request_latency_ms
                    ),
                    "_quote_age_ms": quote_age_ms,
                    "_quote_attempts": attempt,
                }
            )

            return instrumented_quote

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:
            last_error = error

            if (
                attempt
                < MAXIMUM_QUOTE_ATTEMPTS
            ):
                wait_seconds = 2 * attempt

                print(
                    "Jupiter connection "
                    "failed on attempt "
                    f"{attempt}/"
                    f"{MAXIMUM_QUOTE_ATTEMPTS}. "
                    "Waiting "
                    f"{wait_seconds} seconds."
                )

                time.sleep(wait_seconds)
                continue

        except requests.RequestException as error:
            last_error = error

            if (
                attempt
                < MAXIMUM_QUOTE_ATTEMPTS
            ):
                wait_seconds = 2 * attempt

                print(
                    "Jupiter request failed "
                    "on attempt "
                    f"{attempt}/"
                    f"{MAXIMUM_QUOTE_ATTEMPTS}. "
                    "Waiting "
                    f"{wait_seconds} seconds."
                )

                time.sleep(wait_seconds)
                continue

            break

    raise requests.RequestException(
        "Jupiter quote request failed after "
        f"{MAXIMUM_QUOTE_ATTEMPTS} attempts: "
        f"{last_error}"
    )


def build_model_fields(token):
    """
    Return ranking and prediction fields for results.
    """

    return {
        "market_score": safe_float(
            token.get("market_score")
        ),
        "liquidity_score": safe_float(
            token.get("liquidity_score")
        ),
        "volume_score": safe_float(
            token.get("volume_score")
        ),
        "pair_score": safe_float(
            token.get("pair_score")
        ),
        "intelligence_score": safe_float(
            token.get(
                "intelligence_score"
            )
        ),
        "ai_opportunity_score": safe_float(
            token.get(
                "ai_opportunity_score"
            )
        ),
        "combined_confidence": safe_float(
            token.get(
                "combined_confidence"
            )
        ),
        "opportunity_probability": safe_float(
            token.get(
                "opportunity_probability"
            )
        ),
        "expected_profit_usd": safe_float(
            token.get(
                "expected_profit_usd"
            )
        ),
        "trend_score": safe_float(
            token.get("trend_score")
        ),
        "stability_score": safe_float(
            token.get("stability_score")
        ),
        "downside_risk_score": safe_float(
            token.get(
                "downside_risk_score"
            )
        ),
        "scanner_selection_type": (
            token.get(
                "scanner_selection_type"
            )
            or "unknown"
        ),
    }


def create_quote_error_result(
    symbol,
    error,
    token=None,
):
    """
    Create a standard quote-error result.
    """

    token = token or {}

    return {
        "token": symbol,
        "mint": token.get("mint"),
        "buy_route": "Unavailable",
        "sell_route": "Unavailable",
        "starting_amount": (
            TRADE_AMOUNT_USD
        ),
        "ending_amount": 0.0,
        "quoted_profit": 0.0,
        "estimated_cost": (
            ESTIMATED_EXECUTION_COST_USD
        ),
        "net_profit": 0.0,
        "decision": "⚠️ QUOTE ERROR",
        "eligible": False,
        "quote_successful": False,
        "quote_started_at": None,
        "quote_received_at": utc_text(),
        "quote_source_time": None,
        "quote_latency_ms": 0.0,
        "quote_age_ms": 0.0,
        "quote_attempts": 0,
        "buy_quote_latency_ms": 0.0,
        "sell_quote_latency_ms": 0.0,
        "buy_quote_age_ms": 0.0,
        "sell_quote_age_ms": 0.0,
        "error": str(error),
        **build_model_fields(token),
    }


def scan_one_token(
    symbol,
    token_mint,
    token=None,
):
    """
    Test a USDC-token-USDC round trip.
    """

    token = token or {}

    if not symbol:
        raise ValueError(
            "Token symbol is missing."
        )

    if not token_mint:
        raise ValueError(
            "The mint address is missing "
            f"for {symbol}."
        )

    if token_mint == USDC_MINT:
        raise ValueError(
            "USDC cannot be scanned "
            "against itself."
        )

    starting_units = int(
        TRADE_AMOUNT_USD
        * USDC_DECIMALS
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
            "Jupiter returned zero tokens "
            f"for {symbol}."
        )

    time.sleep(
        BUY_SELL_WAIT_SECONDS
    )

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
        ending_amount
        - TRADE_AMOUNT_USD
    )

    estimated_net_profit = (
        quoted_profit
        - ESTIMATED_EXECUTION_COST_USD
    )

    eligible = (
        estimated_net_profit
        >= MIN_PROFIT_USD
    )

    return {
        "token": symbol,
        "mint": token_mint,
        "buy_route": get_route_name(
            buy_quote
        ),
        "sell_route": get_route_name(
            sell_quote
        ),
        "starting_amount": (
            TRADE_AMOUNT_USD
        ),
        "ending_amount": ending_amount,
        "quoted_profit": quoted_profit,
        "estimated_cost": (
            ESTIMATED_EXECUTION_COST_USD
        ),
        "net_profit": (
            estimated_net_profit
        ),
        "decision": (
            "🟢 TEST FURTHER"
            if eligible
            else "🔴 SKIP"
        ),
        "eligible": eligible,
        "quote_successful": True,
        "quote_started_at": (
            buy_quote.get(
                "_quote_started_at"
            )
        ),
        "quote_received_at": (
            sell_quote.get(
                "_quote_received_at"
            )
        ),
        "quote_source_time": (
            sell_quote.get(
                "_quote_source_time"
            )
            or buy_quote.get(
                "_quote_source_time"
            )
        ),
        "quote_latency_ms": (
            safe_float(
                buy_quote.get(
                    "_quote_latency_ms"
                )
            )
            + safe_float(
                sell_quote.get(
                    "_quote_latency_ms"
                )
            )
        ),
        "quote_age_ms": max(
            safe_float(
                buy_quote.get(
                    "_quote_age_ms"
                )
            ),
            safe_float(
                sell_quote.get(
                    "_quote_age_ms"
                )
            ),
        ),
        "quote_attempts": (
            int(
                safe_float(
                    buy_quote.get(
                        "_quote_attempts"
                    )
                )
            )
            + int(
                safe_float(
                    sell_quote.get(
                        "_quote_attempts"
                    )
                )
            )
        ),
        "buy_quote_latency_ms": safe_float(
            buy_quote.get(
                "_quote_latency_ms"
            )
        ),
        "sell_quote_latency_ms": safe_float(
            sell_quote.get(
                "_quote_latency_ms"
            )
        ),
        "buy_quote_age_ms": safe_float(
            buy_quote.get(
                "_quote_age_ms"
            )
        ),
        "sell_quote_age_ms": safe_float(
            sell_quote.get(
                "_quote_age_ms"
            )
        ),
        "buy_quote_attempts": int(
            safe_float(
                buy_quote.get(
                    "_quote_attempts"
                )
            )
        ),
        "sell_quote_attempts": int(
            safe_float(
                sell_quote.get(
                    "_quote_attempts"
                )
            )
        ),
        "error": "",
        **build_model_fields(token),
    }


def print_token_details(token):
    """
    Print why a token was selected.
    """

    print(
        "  Selection: "
        f"{token.get('scanner_selection_type')}"
    )

    print(
        "  AI opportunity: "
        f"{safe_float(token.get('ai_opportunity_score')):.2f}/100"
    )

    print(
        "  Opportunity probability: "
        f"{safe_float(token.get('opportunity_probability')):.2f}%"
    )

    print(
        "  Expected profit: "
        f"${safe_float(token.get('expected_profit_usd')):.6f}"
    )

    print(
        "  Combined confidence: "
        f"{safe_float(token.get('combined_confidence')):.2f}/100"
    )

    print(
        "  Downside risk: "
        f"{safe_float(token.get('downside_risk_score')):.2f}/100"
    )

    print(
        "  Trend / stability: "
        f"{safe_float(token.get('trend_score')):.2f} / "
        f"{safe_float(token.get('stability_score')):.2f}"
    )

    print(
        "  Intelligence / market: "
        f"{safe_float(token.get('intelligence_score')):.2f} / "
        f"{safe_float(token.get('market_score')):.2f}"
    )

    print(
        "  Liquidity / volume: "
        f"${safe_float(token.get('liquidity_usd')):,.2f} / "
        f"${safe_float(token.get('volume_24h_usd')):,.2f}"
    )


def scan_token_worker(token):
    """
    Scan one token inside a worker thread.
    """

    symbol = (
        token.get("symbol")
        or "UNKNOWN"
    )

    token_mint = token.get("mint")

    try:
        result = scan_one_token(
            symbol=symbol,
            token_mint=token_mint,
            token=token,
        )

    except (
        requests.RequestException,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        result = create_quote_error_result(
            symbol=symbol,
            error=error,
            token=token,
        )

    except Exception as error:
        result = create_quote_error_result(
            symbol=symbol,
            error=(
                "Unexpected worker error: "
                f"{error}"
            ),
            token=token,
        )

    return {
        "token_data": token,
        "symbol": symbol,
        "mint": token_mint,
        "result": result,
    }


def run_sequential_scanner(tokens):
    """
    Scan tokens sequentially.
    """

    worker_results = []

    for position, token in enumerate(
        tokens,
        start=1,
    ):
        symbol = (
            token.get("symbol")
            or "UNKNOWN"
        )

        print(
            f"\nScanning {position}/"
            f"{len(tokens)}: {symbol}"
        )

        print_token_details(token)

        worker_output = (
            scan_token_worker(token)
        )

        result = worker_output["result"]

        print(
            f"{symbol}: net profit "
            f"${result['net_profit']:.6f} "
            f"{result['decision']}"
        )

        worker_results.append(
            worker_output
        )

    return worker_results


def run_parallel_scanner(tokens):
    """
    Scan tokens with a small worker pool.
    """

    worker_results = []

    with ThreadPoolExecutor(
        max_workers=SCANNER_MAX_WORKERS,
        thread_name_prefix=(
            "scanner-worker"
        ),
    ) as executor:
        future_to_token = {
            executor.submit(
                scan_token_worker,
                token,
            ): token
            for token in tokens
        }

        completed_count = 0

        for future in as_completed(
            future_to_token
        ):
            completed_count += 1

            token = future_to_token[
                future
            ]

            symbol = (
                token.get("symbol")
                or "UNKNOWN"
            )

            try:
                worker_output = (
                    future.result()
                )

            except Exception as error:
                worker_output = {
                    "token_data": token,
                    "symbol": symbol,
                    "mint": token.get("mint"),
                    "result": (
                        create_quote_error_result(
                            symbol=symbol,
                            error=(
                                "Worker future "
                                f"failed: {error}"
                            ),
                            token=token,
                        )
                    ),
                }

            worker_results.append(
                worker_output
            )

            print(
                f"\nCompleted "
                f"{completed_count}/"
                f"{len(tokens)}: "
                f"{symbol}"
            )

            print_token_details(token)

            result = (
                worker_output["result"]
            )

            print(
                f"{symbol}: net profit "
                f"${result['net_profit']:.6f} "
                f"{result['decision']}"
            )

    return worker_results


def save_scan_history(worker_results):
    """
    Save token scan success or failure.
    """

    for worker_output in worker_results:
        mint = worker_output["mint"]

        if not mint:
            continue

        result = worker_output["result"]

        try:
            mark_token_scanned(
                mint,
                successful=bool(
                    result.get(
                        "quote_successful"
                    )
                ),
            )

        except Exception as error:
            print(
                "Could not update scan history "
                f"for "
                f"{worker_output['symbol']}: "
                f"{error}"
            )


def scan_tokens(tokens):
    """
    Scan a token batch and return sorted results.
    """

    if PARALLEL_TEST_TOKEN_LIMIT is not None:
        limit = max(
            1,
            int(PARALLEL_TEST_TOKEN_LIMIT),
        )

        tokens = tokens[:limit]

        print(
            "Parallel test limit enabled: "
            f"{len(tokens)} tokens."
        )

    started_at = time.monotonic()

    if PARALLEL_SCANNER_ENABLED:
        worker_results = (
            run_parallel_scanner(tokens)
        )
    else:
        worker_results = (
            run_sequential_scanner(tokens)
        )

    save_scan_history(worker_results)

    results = [
        output["result"]
        for output in worker_results
    ]

    results.sort(
        key=lambda result: (
            bool(
                result.get(
                    "quote_successful"
                )
            ),
            safe_float(
                result.get("net_profit")
            ),
            safe_float(
                result.get(
                    "ai_opportunity_score"
                )
            ),
            safe_float(
                result.get(
                    "combined_confidence"
                )
            ),
        ),
        reverse=True,
    )

    elapsed_seconds = (
        time.monotonic()
        - started_at
    )

    successful_count = sum(
        bool(
            result.get(
                "quote_successful"
            )
        )
        for result in results
    )

    eligible_count = sum(
        bool(result.get("eligible"))
        for result in results
    )

    tokens_per_minute = (
        len(results)
        / elapsed_seconds
        * 60
        if elapsed_seconds > 0
        else 0.0
    )

    print(
        "\nParallel batch scan completed."
    )

    print(
        f"Tokens scanned: "
        f"{len(results)}"
    )

    print(
        "Successful quotes: "
        f"{successful_count}"
    )

    print(
        "Quote errors: "
        f"{len(results) - successful_count}"
    )

    print(
        "Eligible opportunities: "
        f"{eligible_count}"
    )

    print(
        "Elapsed time: "
        f"{elapsed_seconds:.1f} seconds"
    )

    print(
        "Scanner speed: "
        f"{tokens_per_minute:.1f} "
        "tokens/minute"
    )

    return results